import re
import socket

from dolphie.DataTypes import ConnectionSource, Replica
from dolphie.Modules.Functions import format_number, format_picoseconds, format_time
from dolphie.Modules.ManualException import ManualException
from dolphie.Modules.MySQL import Database
from dolphie.Modules.Queries import MySQLQueries
from dolphie.Modules.TabManager import Tab
from rich.style import Style
from rich.table import Table
from textual.containers import ScrollableContainer
from textual.widgets import Static

# Example GTID: 3beacd96-6fe3-18ec-9d95-b4592zec4b45:1-26
_GTID_PATTERN = re.compile(r"\b(\w+(?:-\w+){4}):(.+)\b")


def _filter_gtid_sets(gtid_sets: str, exclude_uuids: set[str]) -> str:
    """Remove GTID lines containing any of the excluded UUIDs."""
    remaining = [
        line.strip().rstrip(",")
        for line in gtid_sets.splitlines()
        if line.strip() and not any(uuid in line for uuid in exclude_uuids)
    ]
    return ",\n".join(remaining)


def _color_gtid_sets(gtid_sets: str, primary_uuid: str) -> str:
    """Apply Rich markup to GTID sets, highlighting the primary UUID."""

    def _colorize(match):
        source_id = match.group(1)
        transaction_id = match.group(2)

        if source_id == primary_uuid:
            return f"[highlight]{source_id}[/highlight]:{transaction_id}"
        return f"[dark_gray]{source_id}:{transaction_id}[/dark_gray]"

    return _GTID_PATTERN.sub(_colorize, gtid_sets.replace(",", ""))


def _sync_grid(grid, items: dict[str, Table], item_type: str, tab_id: str, app, tracked: dict[str, Static]):
    """Synchronize grid child widgets with the current set of items.

    Uses the caller-owned ``tracked`` dict to avoid DOM queries. The dict maps
    item keys to their mounted Static widgets and is updated in place.
    """
    current_keys = set(items.keys())

    # Update existing or mount new
    for key, table in items.items():
        if key in tracked:
            tracked[key].update(table)
        else:
            try:
                static = Static(table, id=f"{item_type}_{key}_{tab_id}")
                grid.mount(
                    ScrollableContainer(static, id=f"{item_type}_container_{key}_{tab_id}")
                )
                tracked[key] = static
            except Exception:
                app.notify(
                    f"Failed to mount {item_type} [$highlight]{key}",
                    severity="error",
                )

    # Remove stale widgets
    for key in set(tracked.keys()) - current_keys:
        tracked[key].parent.remove()
        del tracked[key]


def create_panel(tab: Tab):
    dolphie = tab.dolphie

    global_variables = dolphie.global_variables
    group_replication_data = dolphie.group_replication_data
    connection_source_alt = dolphie.connection_source_alt
    panels = dolphie.panels

    # --- Replication panel ---
    if not dolphie.replication_status:
        tab.replication_container.display = False
    else:
        tab.replication_container.display = True

        if dolphie.replication_applier_status.get("data"):
            table_thread_applier_status = Table(box=None, header_style="#c5c7d2")
            table_thread_applier_status.add_column("Worker", justify="center")
            table_thread_applier_status.add_column("Usage", min_width=6)
            table_thread_applier_status.add_column("Apply Time")
            table_thread_applier_status.add_column("Last Applied Transaction")
            table_thread_applier_status.add_column("Retries")
            table_thread_applier_status.add_column("Error Time")
            table_thread_applier_status.add_column("Error Message", overflow="fold")

            for row in dolphie.replication_applier_status["data"]:
                worker_id = row.get("worker_id")
                thread_id = row.get("thread_id")

                # Handle the ROLLUP row, which contains the total for all threads
                if not thread_id:
                    all_workers_diff = dolphie.replication_applier_status["diff_all"]
                    continue

                # Calculate the difference in thread events for this worker
                worker_diff = dolphie.replication_applier_status[f"diff_{thread_id}"]

                # Format the last applied transaction
                last_applied_transaction = row.get("last_applied_transaction", "N/A")
                if last_applied_transaction and "-" in last_applied_transaction:
                    source_id_split = last_applied_transaction.split("-")[4].split(":")[0]
                    transaction_id = last_applied_transaction.split(":")[1]
                    last_applied_transaction = f"…[dark_gray]{source_id_split}[/dark_gray]:{transaction_id}"

                # Format the last error time
                last_error_time = row.get("applying_transaction_last_transient_error_timestamp", "N/A")
                last_error_time = "" if last_error_time == "0000-00-00 00:00:00.000000" else str(last_error_time)

                # Calculate the usage percentage for each worker for the current poll
                usage_percentage = round(100 * (worker_diff / all_workers_diff), 2) if all_workers_diff > 0 else 0.0
                retries_count = row.get("applying_transaction_retries_count", 0)
                retries_count = f"[dark_gray]{retries_count}" if retries_count == 0 else f"[red]{retries_count}"

                # Display the cumulative usage for the worker
                table_thread_applier_status.add_row(
                    f"[b highlight]{worker_id}[/b highlight]: {thread_id}",
                    f"{usage_percentage}%",
                    format_picoseconds(float(row["apply_time"])),
                    last_applied_transaction,
                    retries_count,
                    last_error_time,
                    row.get("applying_transaction_last_transient_error_message", "N/A"),
                )

            tab.replication_thread_applier.update(table_thread_applier_status)
            tab.replication_thread_applier_container.display = True
        else:
            tab.replication_thread_applier_container.display = False

        if connection_source_alt == ConnectionSource.mariadb:
            available_replication_variables = {
                "slave_parallel_mode": "parallel_mode",
                "slave_parallel_workers": "parallel_workers",
                "slave_parallel_threads": "parallel_threads",
                "log_slave_updates": "log_slave_updates",
            }
        elif dolphie.is_mysql_version_at_least("8.0.22"):
            available_replication_variables = {
                "replica_parallel_type": "parallel_type",
                "replica_parallel_workers": "parallel_workers",
                "replica_preserve_commit_order": "preserve_commit_order",
                "log_replica_updates": "log_replica_updates",
            }
        else:
            available_replication_variables = {
                "slave_parallel_type": "parallel_type",
                "slave_parallel_workers": "parallel_workers",
                "slave_preserve_commit_order": "preserve_commit_order",
                "log_slave_updates": "log_slave_updates",
            }

        replication_variables = "  ".join(
            f"[$label]{display_name}[/$label] {global_variables.get(var, 'N/A')}"
            for var, display_name in available_replication_variables.items()
        )

        tab.replication_variables.update(replication_variables)
        tab.replication_status.update(create_replication_table(tab))

    # --- Group Replication panel ---
    if not (dolphie.group_replication or dolphie.innodb_cluster):
        tab.group_replication_container.display = False
    else:
        tab.group_replication_container.display = True

        available_variables = {
            "group_replication_view_change_uuid": ("View UUID", global_variables),
            "group_replication_communication_stack": ("Communication Stack", global_variables),
            "group_replication_consistency": ("Global Consistency", global_variables),
            "group_replication_paxos_single_leader": ("Paxos Single Leader", global_variables),
            "write_concurrency": ("Write Concurrency", group_replication_data),
        }

        group_replication_variables = "  ".join(
            f"[$label]{label}[/$label] {source.get(var, 'N/A')}" for var, (label, source) in available_variables.items()
        )

        title_prefix = panels.get_key(panels.replication.name)
        cluster_title = "InnoDB Cluster" if dolphie.innodb_cluster else "Group Replication"
        cluster_name = group_replication_data.get("cluster_name")
        final_cluster_name = (
            cluster_name if cluster_name else global_variables.get("group_replication_group_name", "N/A")
        )
        tab.group_replication_title.update(
            f"[b]{title_prefix}{cluster_title} ([$highlight]{final_cluster_name}[/$highlight])"
        )
        tab.group_replication_data.update(group_replication_variables)

        # Check if this server is the GR primary
        dolphie.is_group_replication_primary = any(
            row.get("MEMBER_ID") == dolphie.server_uuid and row.get("MEMBER_ROLE") == "PRIMARY"
            for row in dolphie.group_replication_members
        )

        items = create_group_replication_member_table(tab)
        _sync_grid(tab.group_replication_grid, items, "member", tab.id, dolphie.app, tab.member_widgets)

    # --- ClusterSet panel ---
    innodb_cluster_clustersets = dolphie.innodb_cluster_clustersets
    if not innodb_cluster_clustersets:
        tab.clusterset_container.display = False
    else:
        tab.clusterset_container.display = True

        tab.clusterset_title.update(
            f"[b]{panels.get_key(panels.replication.name)}ClusterSets "
            f"([$highlight]{len(innodb_cluster_clustersets)}[/$highlight])"
        )

        host_cluster_name = group_replication_data.get("cluster_name")

        items = {}
        for row in innodb_cluster_clustersets:
            clusterset_name = row["ClusterSet"]
            clusters = row["Clusters"]

            formatted_clusters = clusters.replace(host_cluster_name, f"[b highlight]{host_cluster_name}[/b highlight]")

            table = Table(box=None, show_header=False)
            table.add_column()
            table.add_column()
            table.add_row("[b][light_blue]ClusterSet", f"[light_blue]{clusterset_name}")
            table.add_row("[label]Clusters", formatted_clusters)

            items[clusterset_name] = table

        _sync_grid(tab.clusterset_grid, items, "clusterset", tab.id, dolphie.app, tab.clusterset_widgets)


# This function isn't in create_panel() because it's called as part of the replica worker instead of the main worker
def create_replica_panel(tab: Tab):
    dolphie = tab.dolphie

    # Refresh optimization: cache frequently accessed objects
    replica_manager = dolphie.replica_manager
    panels = dolphie.panels

    if not replica_manager.replicas:
        tab.replicas_container.display = False
        return

    tab.replicas_container.display = True
    tab.replicas_loading_indicator.display = False

    # Update replicas title
    num_replicas = len(replica_manager.available_replicas)
    title_prefix = panels.get_key(panels.replication.name)
    tab.replicas_title.update(f"[b]{title_prefix}Replicas ([$highlight]{num_replicas}[/$highlight])")

    # Sync replica grid widgets with current replica data
    sorted_replicas = replica_manager.get_sorted_replicas()
    items = {r.row_key: r.table for r in sorted_replicas if r.table}

    _sync_grid(tab.replicas_grid, items, "replica", tab.id, dolphie.app, tab.replica_widgets)


def create_replication_table(tab: Tab, dashboard_table=False, replica: Replica = None) -> Table:
    dolphie = tab.dolphie

    # When replica is specified, that means we're creating a table for a replica and not replication
    if replica:
        data = replica.replication_status
        mysql_version = replica.mysql_version
        connection_source_alt = replica.connection_source_alt
    else:
        data = dolphie.replication_status
        mysql_version = dolphie.host_version
        connection_source_alt = dolphie.connection_source_alt

    # Determine replication terminology based on MySQL version
    # and connection source (MariaDB or MySQL)
    source_prefix = (
        "Source"
        if dolphie.is_mysql_version_at_least("8.0.22", mysql_version)
        and connection_source_alt != ConnectionSource.mariadb
        else "Master"
    )
    replica_prefix = "Replica" if source_prefix == "Source" else "Slave"
    uuid_key = f"{source_prefix}_UUID"

    primary_uuid = data.get(uuid_key)
    primary_host = dolphie.get_hostname(data.get(f"{source_prefix}_Host"))
    primary_user = data.get(f"{source_prefix}_User")
    primary_log_file = data.get(f"{source_prefix}_Log_File")
    primary_ssl_allowed = data.get(f"{source_prefix}_SSL_Allowed")
    relay_primary_log_file = data.get(f"Relay_{source_prefix}_Log_File")
    replica_sql_running_state = data.get(f"{replica_prefix}_SQL_Running_State")
    replica_io_state = data.get(f"{replica_prefix}_IO_State")
    read_primary_log_pos = data.get(f"Read_{source_prefix}_Log_Pos")
    exec_primary_log_pos = data.get(f"Exec_{source_prefix}_Log_Pos")

    is_io_running = data.get(f"{replica_prefix}_IO_Running") == "Yes"
    is_sql_running = data.get(f"{replica_prefix}_SQL_Running") == "Yes"
    io_thread_running = "[green]ON[/green]" if is_io_running else "[red]OFF[/red]"
    sql_thread_running = "[green]ON[/green]" if is_sql_running else "[red]OFF[/red]"

    # Determine GTID status
    mariadb_using_gtid = data.get("Using_Gtid")
    mariadb_gtid_enabled = mariadb_using_gtid not in (None, "No")
    mysql_gtid_enabled = bool(data.get("Executed_Gtid_Set"))

    if mariadb_gtid_enabled:
        gtid_status = mariadb_using_gtid
    elif mysql_gtid_enabled:
        auto_position = "ON" if data.get("Auto_Position") == 1 else "OFF"
        gtid_status = f"ON [label]Auto Position[/label] {auto_position}"
    else:
        gtid_status = "OFF"

    # Replica lag calculation
    replica_lag = data.get("Seconds_Behind", 0)
    formatted_replica_lag = None
    if replica_lag is not None:
        sql_delay = data.get("SQL_Delay")
        if sql_delay:
            # Check if it's already an int or a string representing an int
            if isinstance(sql_delay, int) or (isinstance(sql_delay, str) and sql_delay.isdigit()):
                replica_lag = max(0, replica_lag - int(sql_delay))

        lag_color = "green"
        if replica_lag >= 20:
            lag_color = "red"
        elif replica_lag >= 10:
            lag_color = "yellow"

        formatted_replica_lag = f"[{lag_color}]{format_time(replica_lag)}[/{lag_color}]"

    if dashboard_table:
        table = Table(
            show_header=False,
            box=None,
            expand=True,
            title="Replication",
            title_style=Style(color="#bbc8e8", bold=True),
            style="table_border",
        )
        table.add_column(no_wrap=True)
        table.add_column(max_width=30)
    else:
        table = Table(show_header=False, box=None)
        table.add_column()
        table.add_column(overflow="fold")

    if replica:
        table.add_row("[b][light_blue]Host", f"[light_blue]{replica.host}")
        table.add_row("[label]Version", f"{replica.host_distro} {replica.mysql_version}")
    else:
        table.add_row("[label]Primary", primary_host)

    if not dashboard_table:
        table.add_row("[label]User", primary_user)

    table.add_row(
        "[label]Thread",
        f"[label]IO[/label] {io_thread_running} [label]SQL[/label] {sql_thread_running}",
    )

    replication_delay = ""
    if data["SQL_Delay"]:
        if dashboard_table:
            replication_delay = "[dark_yellow](delayed)"
        else:
            replication_delay = f"[dark_yellow]Delay[/dark_yellow] {format_time(data['SQL_Delay'])}"

    if formatted_replica_lag is None or not is_sql_running:
        table.add_row("[label]Lag", "")
    else:
        table.add_row(
            "[label]Lag",
            f"{formatted_replica_lag} [label]Speed[/label] {data['Replica_Speed']} {replication_delay}",
        )

    if dashboard_table:
        table.add_row("[label]Binlog IO", str(primary_log_file))
        table.add_row("[label]Binlog SQL", str(relay_primary_log_file))
        table.add_row("[label]Relay Log ", str(data["Relay_Log_File"]))
        table.add_row("[label]GTID", gtid_status)
        table.add_row("[label]State", str(replica_sql_running_state))
    else:
        table.add_row(
            "[label]Binlog IO",
            f"{primary_log_file} ([dark_gray]{read_primary_log_pos}[/dark_gray])",
        )
        table.add_row(
            "[label]Binlog SQL",
            f"{relay_primary_log_file} ([dark_gray]{exec_primary_log_pos}[/dark_gray])",
        )
        table.add_row(
            "[label]Relay Log",
            f"{data['Relay_Log_File']} ([dark_gray]{data['Relay_Log_Pos']}[/dark_gray])",
        )

    if not dashboard_table:
        ssl_enabled = "ON" if primary_ssl_allowed == "Yes" else "OFF"
        table.add_row("[label]SSL", ssl_enabled)

        replication_status_filtering = [
            "Replicate_Do_DB",
            "Replicate_Ignore_Table",
            "Replicate_Ignore_DB",
            "Replicate_Do_Table",
            "Replicate_Wild_Do_Table",
            "Replicate_Wild_Ignore_Table",
            "Replicate_Rewrite_DB",
        ]

        for status_filter in replication_status_filtering:
            value = data.get(status_filter)

            status_filter_formatted = f"Filter: {status_filter.split('Replicate_')[1]}"
            if value:
                table.add_row(f"[label]{status_filter_formatted}", str(value))

        error_types = ["Last_IO_Error", "Last_SQL_Error"]
        errors = [(error_type, data[error_type]) for error_type in error_types if data[error_type]]

        if errors:
            for error_type, error_message in errors:
                table.add_row(
                    f"[label]{error_type.replace('_', ' ')}",
                    f"[red]{error_message}[/red]",
                )
        else:
            table.add_row("[label]IO State", str(replica_io_state))
            table.add_row("[label]SQL State", str(replica_sql_running_state))

        if mysql_gtid_enabled:
            executed_gtid_set = data["Executed_Gtid_Set"]
            retrieved_gtid_set = data["Retrieved_Gtid_Set"]

            table.add_row("[label]GTID", gtid_status)

            if replica:
                # Exclude the primary's own UUID and all its replication source UUIDs to avoid
                # false positives from stale gtid_executed snapshots. The primary actively receives
                # GTIDs from its sources, so by the time replicas are checked the snapshot is behind.
                exclude_uuids = {dolphie.server_uuid} | dolphie.replication_source_uuids

                replica_gtid_set = _filter_gtid_sets(executed_gtid_set, exclude_uuids)
                primary_gtid_set = _filter_gtid_sets(dolphie.global_variables.get("gtid_executed", ""), exclude_uuids)

                replica.connection.execute(
                    "SELECT GTID_SUBTRACT(%s, %s) AS errant_trxs",
                    (replica_gtid_set, primary_gtid_set),
                )
                gtid_data = replica.connection.fetchone()
                if gtid_data.get("errant_trxs"):
                    errant_trx = f"[red]{gtid_data['errant_trxs']}[/red]"
                else:
                    errant_trx = "[green]None[/green]"

                table.add_row("[label]Errant TRX", errant_trx)

                # If this replica has replicas, use its primary server UUID, else use its own
                primary_uuid = primary_uuid or dolphie.server_uuid

            retrieved_gtid_set = _color_gtid_sets(retrieved_gtid_set, primary_uuid)
            executed_gtid_set = _color_gtid_sets(executed_gtid_set, primary_uuid)

            table.add_row("[label]Retrieved GTID", retrieved_gtid_set)
            table.add_row("[label]Executed GTID", executed_gtid_set)
        elif mariadb_gtid_enabled:
            primary_id = data.get("Master_Server_Id")

            table.add_row("[label]GTID", gtid_status)

            # Check if GTID IO position exists
            gtid_io_pos = data.get("Gtid_IO_Pos")
            # gtid_io_pos = "1-1-3323,1-2-32,1-3-5543,1-4-554454"
            if gtid_io_pos:
                # If this is a replica, use its primary server ID, else use its own server ID
                if replica:
                    replica_primary_server_id = dolphie.replication_status.get("Master_Server_Id")
                    primary_id = replica_primary_server_id or dolphie.global_variables.get("server_id")

                gtids = gtid_io_pos.split(",")
                for idx, gtid in enumerate(gtids):
                    server_id = gtid.split("-")[1]

                    if str(server_id) == str(primary_id):
                        # Highlight GTID if it matches the primary ID
                        gtids[idx] = f"[highlight]{gtid}[/highlight]"
                    else:
                        # Otherwise, darken GTID
                        gtids[idx] = f"[dark_gray]{gtid}[/dark_gray]"

                table.add_row("[label]GTID IO Pos", "\n".join(gtids))

    return table


def create_group_replication_member_table(tab: Tab) -> dict[str, Table]:
    dolphie = tab.dolphie

    if not dolphie.group_replication_members:
        return {}

    unsorted: list[tuple[str, str, Table]] = []  # (member_id, host, table)
    for row in dolphie.group_replication_members:
        member_id = row.get("MEMBER_ID")
        member_host = row.get("MEMBER_HOST")
        member_port = row.get("MEMBER_PORT")
        host = f"{member_host}:{member_port}"

        member_role = row.get("MEMBER_ROLE", "N/A")
        if member_role == "PRIMARY":
            member_role = f"[b][highlight]{member_role}[/highlight]"

        member_state = row.get("MEMBER_STATE", "N/A")
        member_state = f"[green]{member_state}[/green]" if member_state == "ONLINE" else f"[red]{member_state}[/red]"

        table = Table(box=None, show_header=False)
        table.add_column()
        table.add_column()

        table.add_row("[b][light_blue]Member", f"[light_blue]{host}")
        table.add_row("[label]UUID", str(member_id))
        table.add_row("[label]Role", member_role)
        table.add_row("[label]State", member_state)
        table.add_row("[label]Version", row.get("MEMBER_VERSION", "N/A"))

        table.add_row(
            "[label]Conflict",
            f"[label]Queue[/label]: {format_number(row.get('COUNT_TRANSACTIONS_IN_QUEUE', 'N/A'))}"
            f" [label]Checked[/label]: {format_number(row.get('COUNT_TRANSACTIONS_CHECKED', 'N/A'))}"
            f" [label]Detected[/label]: {format_number(row.get('COUNT_TRANSACTIONS_DETECTED', 'N/A'))}",
        )
        table.add_row(
            "[label]Applied",
            f"{format_number(row.get('COUNT_TRANSACTIONS_REMOTE_APPLIED', 'N/A'))}"
            f" [label]Queue[/label]: {format_number(row.get('COUNT_TRANSACTIONS_REMOTE_IN_APPLIER', 'N/A'))} ",
        )
        table.add_row(
            "[label]Local",
            f"[label]Proposed[/label]: {format_number(row.get('COUNT_TRANSACTIONS_LOCAL_PROPOSED', 'N/A'))}"
            f" [label]Rollback[/label]: {format_number(row.get('COUNT_TRANSACTIONS_LOCAL_ROLLBACK', 'N/A'))}",
        )
        table.add_row(
            "[label]Rows",
            f"{format_number(row.get('COUNT_TRANSACTIONS_ROWS_VALIDATING', 'N/A'))}"
            " [dark_gray](used for certification)[/dark_gray]",
        )

        unsorted.append((member_id, host, table))

    # Return sorted by host, keyed by member_id
    return {mid: tbl for mid, _, tbl in sorted(unsorted, key=lambda x: x[1])}


def fetch_replication_data(tab: Tab, replica: Replica = None) -> dict:
    dolphie = tab.dolphie
    connection = replica.connection if replica else dolphie.main_db_connection
    mysql_version = replica.mysql_version if replica else None
    connection_source_alt = replica.connection_source_alt if replica else dolphie.connection_source_alt

    # Determine replication status query
    use_show_replica_status = (
        dolphie.is_mysql_version_at_least("8.0.22", use_version=mysql_version)
        and connection_source_alt != ConnectionSource.mariadb
    )
    replication_status_query = (
        MySQLQueries.show_replica_status if use_show_replica_status else MySQLQueries.show_slave_status
    )

    # Determine lag source and query
    replica_lag_source = "HB" if dolphie.heartbeat_table else None
    replica_lag_query = MySQLQueries.heartbeat_replica_lag if replica_lag_source else replication_status_query

    # Fetch replication status
    connection.execute(replication_status_query)
    all_rows = connection.fetchall()
    replication_status = all_rows[0] if all_rows else {}

    # Collect all source UUIDs for multi-source replication errant TRX detection
    if not replica and all_rows:
        uuid_field = "Source_UUID" if use_show_replica_status else "Master_UUID"
        dolphie.replication_source_uuids = {row.get(uuid_field) for row in all_rows if row.get(uuid_field)}

    # Fetch replica lag using alternative method if applicable
    if replica_lag_source:
        connection.execute(replica_lag_query)
        replica_lag_data = connection.fetchone()
    else:
        replica_lag_data = replication_status

    # Determine lag key and calculate replica lag
    lag_key = "Seconds_Behind_Source" if use_show_replica_status else "Seconds_Behind_Master"
    seconds_behind = replica_lag_data.get(lag_key)
    replica_lag = int(seconds_behind) if seconds_behind is not None else 0

    if replication_status:
        # Update replication status with lag and speed
        previous_lag = (
            replica.replication_status.get("Seconds_Behind", 0)
            if replica
            else dolphie.replication_status.get("Seconds_Behind", 0)
        )
        replication_status["Seconds_Behind"] = replica_lag
        replication_status["Replica_Speed"] = (
            round((previous_lag - replica_lag) / dolphie.polling_latency)
            if previous_lag and replica_lag < previous_lag
            else 0
        )

    return replication_status


def fetch_replicas(tab: Tab):
    dolphie = tab.dolphie

    # Refresh optimization: cache frequently accessed objects
    replica_manager = dolphie.replica_manager
    connection_source_alt = dolphie.connection_source_alt

    # Track which row_keys are active this cycle so we can remove stale ones after
    active_row_keys = set()

    for row in replica_manager.available_replicas:
        replica_error = None
        host = dolphie.get_hostname(row["host"].split(":")[0])

        # MariaDB can't correlate processlist threads to SHOW SLAVE HOSTS via UUID like MySQL can.
        # Instead, we match by host from SHOW SLAVE HOSTS and only TCP-probe as a last resort
        # when multiple replicas share the same host.
        if connection_source_alt == ConnectionSource.mariadb:
            row_port = row.get("port")
            if not row_port:
                assigned_port = None

                # Match by host from SHOW SLAVE HOSTS
                matching = [
                    pd
                    for pd in replica_manager.ports.values()
                    if not pd.get("in_use") and dolphie.get_hostname(pd.get("host")) == host
                ]

                if len(matching) == 1:
                    # Unique host match — use directly
                    matching[0]["in_use"] = True
                    assigned_port = matching[0]["port"]
                else:
                    # Multiple replicas on same host or no host match — TCP probe to disambiguate
                    candidates = matching or [pd for pd in replica_manager.ports.values() if not pd.get("in_use")]
                    for port_data in candidates:
                        port = port_data.get("port", 3306)
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sock.settimeout(2)

                        try:
                            sock.connect((host, port))
                            port_data["in_use"] = True
                            assigned_port = port
                            break
                        except (TimeoutError, OSError, ConnectionRefusedError):
                            continue
                        finally:
                            sock.close()

                if not assigned_port:
                    replica_error = "No available port found for MariaDB replica"
            else:
                assigned_port = row_port
        else:
            # We can correlate the replica in the processlist to a specific port from SHOW SLAVE HOSTS with MySQL
            assigned_port = replica_manager.ports.get(row.get("replica_uuid"), {}).get("port", 3306)

        # Update the port of available_replicas so it can be used for the row_key
        row["port"] = assigned_port

        # Create a unique row key for the replica since we now have the assigned port for it
        row_key = replica_manager.create_replica_row_key(row.get("host"), assigned_port)
        active_row_keys.add(row_key)

        host_and_port = f"{host}:{assigned_port}" if assigned_port else host

        replica = replica_manager.get_replica(row_key)
        if not replica:
            replica = replica_manager.add_replica(
                row_key=row_key,
                thread_id=row.get("id"),
                host=host_and_port,
                port=assigned_port,
            )

        # If we don't have a replica connection, we create one
        if not replica.connection and assigned_port:
            try:
                replica.connection = Database(
                    app=dolphie.app,
                    host=host,
                    user=dolphie.user,
                    password=dolphie.password,
                    port=assigned_port,
                    socket=None,
                    ssl=dolphie.ssl,
                    save_connection_id=False,
                )
                global_variables = replica.connection.fetch_status_and_variables("variables")

                replica.mysql_version = dolphie.parse_server_version(global_variables.get("version"))
                replica.host_distro, replica.connection_source_alt = dolphie.determine_distro_and_connection_source_alt(
                    global_variables
                )
            except ManualException as e:
                replica_error = e.reason

        # If we have a replica connection, we fetch its replication status
        if replica.connection:
            try:
                replica.replication_status = fetch_replication_data(tab, replica)
                if replica.replication_status:
                    replica.table = create_replication_table(tab, replica=replica)
            except ManualException as e:
                replica_error = e.reason

        if replica_error:
            table = Table(box=None, show_header=False)
            table.add_column()
            table.add_column(overflow="fold")

            table.add_row("[b][light_blue]Host", f"[light_blue]{host_and_port}")
            table.add_row("[label]User", row["user"])
            table.add_row("[label]Error", f"[red]{replica_error}")

            replica.table = table

    # Remove replicas that are no longer in available_replicas
    for row_key in set(replica_manager.replicas.keys()) - active_row_keys:
        replica_manager.remove_replica(row_key)
