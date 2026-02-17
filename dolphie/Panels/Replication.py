import re

from dolphie.DataTypes import ConnectionSource, Replica
from dolphie.Modules.Functions import (
    format_bytes,
    format_number,
    format_picoseconds,
    format_time,
)
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


# MariaDB GTID format: domain_id-server_id-sequence_number (e.g., 0-1-10)
def _color_mariadb_gtid_sets(gtid_sets: str, primary_server_id) -> str:
    """Apply Rich markup to MariaDB GTID sets, highlighting the primary server_id."""
    primary_server_id = str(primary_server_id)
    colored = []
    for gtid in gtid_sets.split(","):
        gtid = gtid.strip()
        if not gtid:
            continue
        parts = gtid.split("-")
        if len(parts) >= 3 and parts[1] == primary_server_id:
            colored.append(f"[highlight]{gtid}[/highlight]")
        else:
            colored.append(f"[dark_gray]{gtid}[/dark_gray]")
    return "\n".join(colored)


def _detect_mariadb_errant_trx(replica_gtid_current_pos: str, replica_server_id, primary_gtid_current_pos: str):
    """Detect errant transactions on a MariaDB replica.

    MariaDB GTIDs encode the originating server: domain_id-server_id-sequence.
    In normal replication a replica only executes transactions from its primary, so
    its gtid_current_pos should not contain GTIDs with its own server_id
    unless someone wrote directly to it.

    We scan the replica's gtid_current_pos for entries whose server_id
    matches the replica itself, then compare against the primary's
    gtid_current_pos. A GTID is errant when:
      - The primary has no entry for that (domain_id, server_id) pair, or
      - The replica's sequence exceeds the primary's sequence.

    Returns a comma-separated string of errant GTIDs, or None if clean.
    """
    replica_server_id = str(replica_server_id)

    def _parse_gtids(gtid_str: str) -> dict:
        result = {}
        for gtid in gtid_str.split(","):
            gtid = gtid.strip()
            if not gtid:
                continue
            parts = gtid.split("-")
            if len(parts) >= 3:
                result[(parts[0], parts[1])] = int(parts[2])
        return result

    replica_gtids = _parse_gtids(replica_gtid_current_pos)
    primary_gtids = _parse_gtids(primary_gtid_current_pos)

    errant = []
    for (domain_id, server_id), seq in replica_gtids.items():
        if server_id != replica_server_id:
            continue
        primary_seq = primary_gtids.get((domain_id, server_id))
        if primary_seq is None or seq > primary_seq:
            errant.append(f"{domain_id}-{server_id}-{seq}")

    return ",".join(errant) if errant else None


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
                grid.mount(ScrollableContainer(static, id=f"{item_type}_container_{key}_{tab_id}"))
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
            "group_replication_single_primary_mode": ("Single Primary", global_variables),
            "group_replication_consistency": ("Global Consistency", global_variables),
            "write_concurrency": ("Write Concurrency", group_replication_data),
        }

        group_replication_variables = "  ".join(
            f"[$label]{label}[/$label] {source.get(var, 'N/A')}" for var, (label, source) in available_variables.items()
        )

        title_prefix = panels.replication.content_key
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

    # --- Galera Cluster panel ---
    if not dolphie.galera_cluster:
        tab.galera_container.display = False
    else:
        tab.galera_container.display = True

        gcache_size = global_variables.get("wsrep_provider_gcache_size")
        gcache_formatted = format_bytes(int(gcache_size)) if gcache_size else "N/A"

        galera_variables = (
            f"[$label]Cluster Name[/$label] {global_variables.get('wsrep_cluster_name', 'N/A')}"
            f"  [$label]SST Method[/$label] {global_variables.get('wsrep_sst_method', 'N/A')}"
            f"  [$label]OSU Method[/$label] {global_variables.get('wsrep_osu_method', 'N/A')}"
            f"  [$label]GCache Size[/$label] {gcache_formatted}"
            f"  [$label]Apply Threads[/$label] {global_variables.get('wsrep_slave_threads', 'N/A')}"
            f"  [$label]Sync Wait[/$label] {global_variables.get('wsrep_sync_wait', 'N/A')}"
        )

        title_prefix = panels.replication.content_key
        tab.galera_title.update(
            f"[b]{title_prefix}Galera Cluster"
            f" ([$highlight]{dolphie.global_status.get('wsrep_cluster_size', 'N/A')} nodes[/$highlight])"
        )
        tab.galera_data.update(galera_variables)

        items = create_galera_node_table(tab)
        _sync_grid(tab.galera_grid, items, "galera_node", tab.id, dolphie.app, tab.galera_widgets)

    # --- ClusterSet panel ---
    innodb_cluster_clustersets = dolphie.innodb_cluster_clustersets
    if not innodb_cluster_clustersets:
        tab.clusterset_container.display = False
    else:
        tab.clusterset_container.display = True

        tab.clusterset_title.update(
            f"[b]{panels.replication.content_key}ClusterSets "
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
            table.add_row("[b][label]Clusters", formatted_clusters)

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
    title_prefix = panels.replication.content_key
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
        table.add_row("[b][label]Version", f"{replica.host_distro} {replica.mysql_version}")
    else:
        table.add_row("[b][label]Primary", primary_host)

    if not dashboard_table:
        table.add_row("[b][label]User", primary_user)

    table.add_row(
        "[b][label]Thread",
        f"[label]IO[/label] {io_thread_running} [label]SQL[/label] {sql_thread_running}",
    )

    replication_delay = ""
    if data["SQL_Delay"]:
        if dashboard_table:
            replication_delay = "[dark_yellow](delayed)"
        else:
            replication_delay = f"[dark_yellow]Delay[/dark_yellow] {format_time(data['SQL_Delay'])}"

    if formatted_replica_lag is None or not is_sql_running:
        table.add_row("[b][label]Lag", "")
    else:
        table.add_row(
            "[b][label]Lag",
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
            "[b][label]Binlog IO",
            f"{primary_log_file} ([dark_gray]{read_primary_log_pos}[/dark_gray])",
        )
        table.add_row(
            "[b][label]Binlog SQL",
            f"{relay_primary_log_file} ([dark_gray]{exec_primary_log_pos}[/dark_gray])",
        )
        table.add_row(
            "[b][label]Relay Log",
            f"{data['Relay_Log_File']} ([dark_gray]{data['Relay_Log_Pos']}[/dark_gray])",
        )

    if not dashboard_table:
        ssl_enabled = "ON" if primary_ssl_allowed == "Yes" else "OFF"
        table.add_row("[b][label]SSL", ssl_enabled)

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
                table.add_row(f"[b][label]{status_filter_formatted}", str(value))

        error_types = ["Last_IO_Error", "Last_SQL_Error"]
        errors = [(error_type, data[error_type]) for error_type in error_types if data[error_type]]

        if errors:
            for error_type, error_message in errors:
                table.add_row(
                    f"[b][label]{error_type.replace('_', ' ')}",
                    f"[red]{error_message}[/red]",
                )
        else:
            table.add_row("[b][label]IO State", str(replica_io_state))
            table.add_row("[b][label]SQL State", str(replica_sql_running_state))

        if mysql_gtid_enabled:
            executed_gtid_set = data["Executed_Gtid_Set"]
            retrieved_gtid_set = data["Retrieved_Gtid_Set"]

            table.add_row("[b][label]GTID", gtid_status)

            if replica:
                # Exclude the primary's own UUID and all its replication source UUIDs to avoid
                # false positives from stale gtid_executed snapshots. The primary actively receives
                # GTIDs from its sources, so by the time replicas are checked the snapshot is behind.
                # For Group Replication, also exclude the group UUID since it has the same race.
                exclude_uuids = {dolphie.server_uuid} | dolphie.replication_source_uuids
                if dolphie.group_replication:
                    gr_group_name = dolphie.global_variables.get("group_replication_group_name")
                    if gr_group_name:
                        exclude_uuids.add(gr_group_name)

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

                table.add_row("[b][label]Errant TRX", errant_trx)

                # If this replica has replicas, use its primary server UUID, else use its own
                primary_uuid = primary_uuid or dolphie.server_uuid

            retrieved_gtid_set = _color_gtid_sets(retrieved_gtid_set, primary_uuid)
            executed_gtid_set = _color_gtid_sets(executed_gtid_set, primary_uuid)

            table.add_row("[b][label]Retrieved GTID", retrieved_gtid_set)
            table.add_row("[b][label]Executed GTID", executed_gtid_set)
        elif mariadb_gtid_enabled:
            primary_id = data.get("Master_Server_Id")

            table.add_row("[b][label]GTID", gtid_status)

            if replica:
                # Determine the primary server ID for coloring
                replica_primary_server_id = dolphie.replication_status.get("Master_Server_Id")
                primary_id = replica_primary_server_id or dolphie.global_variables.get("server_id")

                replica.connection.execute(
                    "SELECT @@server_id AS server_id, @@gtid_slave_pos AS gtid_slave_pos, "
                    "@@gtid_current_pos AS gtid_current_pos"
                )
                gtid_data = replica.connection.fetchone()

                replica_server_id = gtid_data.get("server_id")
                replica_gtid_slave_pos = gtid_data.get("gtid_slave_pos", "")
                replica_gtid_current_pos = gtid_data.get("gtid_current_pos", "")

                # Detect errant transactions
                primary_gtid_current_pos = dolphie.global_variables.get("gtid_current_pos", "")
                if replica_gtid_current_pos and primary_gtid_current_pos:
                    errant = _detect_mariadb_errant_trx(
                        replica_gtid_current_pos, replica_server_id, primary_gtid_current_pos
                    )
                    errant_trx = f"[red]{errant}[/red]" if errant else "[green]None[/green]"
                else:
                    errant_trx = "[green]None[/green]"
                table.add_row("[b][label]Errant TRX", errant_trx)

                # Retrieved GTID from SHOW SLAVE STATUS
                gtid_io_pos = data.get("Gtid_IO_Pos")
                if gtid_io_pos:
                    table.add_row("[b][label]Retrieved GTID", _color_mariadb_gtid_sets(gtid_io_pos, primary_id))

                # Executed GTID from the replica's gtid_slave_pos
                if replica_gtid_slave_pos:
                    table.add_row(
                        "[b][label]Executed GTID", _color_mariadb_gtid_sets(replica_gtid_slave_pos, primary_id)
                    )
            else:
                # Self-view: this host is a replica
                gtid_io_pos = data.get("Gtid_IO_Pos")
                if gtid_io_pos:
                    table.add_row("[b][label]Retrieved GTID", _color_mariadb_gtid_sets(gtid_io_pos, primary_id))

                gtid_slave_pos = dolphie.global_variables.get("gtid_slave_pos")
                if gtid_slave_pos:
                    table.add_row("[b][label]Executed GTID", _color_mariadb_gtid_sets(gtid_slave_pos, primary_id))

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
        table.add_row("[b][label]UUID", str(member_id))
        table.add_row("[b][label]Role", member_role)
        table.add_row("[b][label]State", member_state)
        table.add_row("[b][label]Version", row.get("MEMBER_VERSION", "N/A"))

        table.add_row(
            "[b][label]Certifier",
            f"[label]Queue[/label] {format_number(row.get('COUNT_TRANSACTIONS_IN_QUEUE', 'N/A'))}"
            f" [label]Checked[/label] {format_number(row.get('COUNT_TRANSACTIONS_CHECKED', 'N/A'))}"
            f" [label]Detected[/label] {format_number(row.get('COUNT_CONFLICTS_DETECTED', 'N/A'))}",
        )
        table.add_row(
            "[b][label]Applier",
            f"{format_number(row.get('COUNT_TRANSACTIONS_REMOTE_APPLIED', 'N/A'))}"
            f" [label]Queue[/label] {format_number(row.get('COUNT_TRANSACTIONS_REMOTE_IN_APPLIER_QUEUE', 'N/A'))}",
        )
        table.add_row(
            "[b][label]Local",
            f"[label]Proposed[/label] {format_number(row.get('COUNT_TRANSACTIONS_LOCAL_PROPOSED', 'N/A'))}"
            f" [label]Rollback[/label] {format_number(row.get('COUNT_TRANSACTIONS_LOCAL_ROLLBACK', 'N/A'))}",
        )
        table.add_row(
            "[b][label]Cert Rows",
            format_number(row.get("COUNT_TRANSACTIONS_ROWS_VALIDATING", "N/A")),
        )

        unsorted.append((member_id, host, table))

    # Return sorted by host, keyed by member_id
    return {mid: tbl for mid, _, tbl in sorted(unsorted, key=lambda x: x[1])}


def create_galera_node_table(tab: Tab) -> dict[str, Table]:
    dolphie = tab.dolphie
    galera = dolphie.global_status

    if not dolphie.galera_cluster_members:
        return {}

    local_uuid = galera.get("wsrep_gcomm_uuid")

    unsorted: list[tuple[str, str, Table]] = []
    for row in dolphie.galera_cluster_members:
        node_uuid = row.get("node_uuid")
        node_name = row.get("node_name", "N/A")
        node_address = row.get("node_incoming_address", "N/A")
        # Strip the port if it's 0 (default when wsrep-node-incoming-address isn't set)
        if node_address.endswith(":0"):
            node_address = node_address[:-2]
        is_local = node_uuid == local_uuid

        table = Table(box=None, show_header=False)
        table.add_column()
        table.add_column()

        if is_local:
            table.add_row(
                "[b][light_blue]Member", f"[b][highlight]{node_name}[/highlight] [light_blue]({node_address})"
            )
        else:
            table.add_row("[b][light_blue]Member", f"[light_blue]{node_name} ({node_address})")

        table.add_row("[b][label]UUID", f"[dark_gray]{node_uuid}[/dark_gray]")

        if is_local:
            # Node state
            node_state = galera.get("wsrep_local_state_comment", "N/A")
            if node_state == "Synced":
                node_state_colored = f"[green]{node_state}[/green]"
            elif node_state in ("Donor/Desynced", "Donor"):
                node_state_colored = f"[yellow]{node_state}[/yellow]"
            else:
                node_state_colored = f"[red]{node_state}[/red]"

            table.add_row(
                "[b][label]State",
                f"{node_state_colored}"
                f"  [label]Connected[/label] {galera.get('wsrep_connected', 'N/A')}"
                f"  [label]Ready[/label] {galera.get('wsrep_ready', 'N/A')}",
            )

            # Flow control
            flow_control_paused = float(galera.get("wsrep_flow_control_paused", 0))
            recv_q = float(galera.get("wsrep_local_recv_queue_avg", 0))
            send_q = float(galera.get("wsrep_local_send_queue_avg", 0))

            table.add_row(
                "[b][label]Flow Control",
                f"[label]Paused[/label] {flow_control_paused:.4f}"
                f"  [label]Recv Q[/label] {recv_q:.4f}"
                f"  [label]Send Q[/label] {send_q:.4f}",
            )

            # Certification
            cert_failures = int(galera.get("wsrep_local_cert_failures", 0))
            bf_aborts = int(galera.get("wsrep_local_bf_aborts", 0))

            table.add_row(
                "[b][label]Certification",
                f"[label]Deps[/label] {format_number(galera.get('wsrep_cert_deps_distance', 0))}"
                f"  [label]Failures[/label] {format_number(cert_failures)}"
                f"  [label]Aborts[/label] {format_number(bf_aborts)}",
            )
            table.add_row(
                "[b][label]Writesets",
                f"[label]Replicated[/label] {format_number(galera.get('wsrep_replicated', 0))}"
                f" ({format_bytes(galera.get('wsrep_replicated_bytes', 0))})"
                f"  [label]Received[/label] {format_number(galera.get('wsrep_received', 0))}"
                f" ({format_bytes(galera.get('wsrep_received_bytes', 0))})",
            )

        unsorted.append((node_uuid, node_name, table))

    # Return sorted by node_name, keyed by node_uuid
    return {uid: tbl for uid, _, tbl in sorted(unsorted, key=lambda x: x[1])}


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
            if previous_lag and replica_lag < previous_lag and dolphie.polling_latency > 0
            else 0
        )

    return replication_status


def fetch_replicas(tab: Tab):
    dolphie = tab.dolphie

    # Track which row_keys are active this cycle so we can remove stale ones after
    active_row_keys = set()

    # Build lookup for existing replicas by thread_id for MariaDB port reuse
    replica_by_thread_id = (
        {r.thread_id: r for r in dolphie.replica_manager.replicas.values()}
        if dolphie.connection_source_alt == ConnectionSource.mariadb
        else {}
    )

    for row in dolphie.replica_manager.available_replicas:
        replica_error = None
        host = dolphie.get_hostname(row["host"].split(":")[0])

        if dolphie.connection_source_alt == ConnectionSource.mariadb:
            # MariaDB: no UUID for correlation - check if we already have this replica
            # by thread_id, otherwise rotate through available ports
            existing = replica_by_thread_id.get(row.get("id"))
            if existing:
                assigned_port = existing.port
                host = existing.host.split(":")[0]
            else:
                assigned_port = None
                for port_data in dolphie.replica_manager.ports.values():
                    if not port_data.get("in_use"):
                        assigned_port = port_data["port"]
                        port_data["in_use"] = True

                        # Use report_host from SHOW SLAVE HOSTS if specified, otherwise
                        # fall back to the processlist IP
                        report_host = port_data.get("host")
                        if report_host:
                            host = dolphie.get_hostname(report_host)

                        break
        else:
            # MySQL: correlate the replica in the processlist to a specific port from
            # SHOW REPLICAS via UUID — processlist IP is used for the host
            assigned_port = dolphie.replica_manager.ports.get(row.get("replica_uuid"), {}).get("port", 3306)

        # Update the port of available_replicas so it can be used for the row_key
        row["port"] = assigned_port
        port = assigned_port

        row_key = dolphie.replica_manager.create_replica_row_key(row.get("host"), port)
        active_row_keys.add(row_key)

        host_and_port = f"{host}:{port}" if port else host

        replica = dolphie.replica_manager.get_replica(row_key)
        if not replica:
            replica = dolphie.replica_manager.add_replica(
                row_key=row_key, thread_id=row.get("id"), host=host_and_port, port=port
            )

        # If we don't have a replica connection, we create one
        if not replica.connection and port:
            try:
                replica.connection = Database(
                    app=dolphie.app,
                    host=host,
                    user=dolphie.user,
                    password=dolphie.password,
                    port=port,
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
            table.add_row("[b][label]User", row["user"])
            table.add_row("[b][label]Error", f"[red]{replica_error}")

            replica.table = table

    # Remove replicas that are no longer in available_replicas
    for row_key in set(dolphie.replica_manager.replicas.keys()) - active_row_keys:
        dolphie.replica_manager.remove_replica(row_key)
