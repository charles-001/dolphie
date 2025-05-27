import re
import socket

from rich.style import Style
from rich.table import Table
from textual._node_list import DuplicateIds
from textual.containers import ScrollableContainer
from textual.widgets import Static

from dolphie.DataTypes import ConnectionSource, Replica
from dolphie.Modules.Functions import format_number, format_picoseconds, format_time
from dolphie.Modules.ManualException import ManualException
from dolphie.Modules.MySQL import Database
from dolphie.Modules.Queries import MySQLQueries
from dolphie.Modules.TabManager import Tab


def create_panel(tab: Tab):
    dolphie = tab.dolphie

    def create_clusterset_panel():
        if not dolphie.innodb_cluster_clustersets:
            tab.clusterset_container.display = False
            return

        tab.clusterset_container.display = True

        # Update the panel title
        tab.clusterset_title.update(
            f"[b]{dolphie.panels.get_key(dolphie.panels.replication.name)}ClusterSets "
            f"([$highlight]{len(dolphie.innodb_cluster_clustersets)}[/$highlight])"
        )

        existing_clusterset_names = {clusterset["ClusterSet"] for clusterset in dolphie.innodb_cluster_clustersets}
        existing_clusterset_components = {c.id.split("_")[1]: c for c in tab.dolphie.app.query(f".clusterset_{tab.id}")}
        host_cluster_name = dolphie.group_replication_data.get("cluster_name")

        for row in dolphie.innodb_cluster_clustersets:
            clusterset_name = row["ClusterSet"]
            clusters = row["Clusters"]

            # Highlight the host cluster name
            formatted_clusters = clusters.replace(host_cluster_name, f"[b highlight]{host_cluster_name}[/b highlight]")

            table = Table(box=None, show_header=False)
            table.add_column()
            table.add_column()
            table.add_row("[b][light_blue]ClusterSet", f"[light_blue]{clusterset_name}")
            table.add_row("[label]Clusters", formatted_clusters)

            if clusterset_name in existing_clusterset_components:
                existing_clusterset_components[clusterset_name].update(table)
            else:
                try:
                    tab.clusterset_grid.mount(
                        ScrollableContainer(
                            Static(
                                table,
                                id=f"clusterset_{clusterset_name}_{tab.id}",
                                classes=f"clusterset_{tab.id}",
                            ),
                            id=f"clusterset_container_{clusterset_name}_{tab.id}",
                            classes=f"clusterset_container_{tab.id} clusterset_container",
                        )
                    )
                except DuplicateIds:
                    tab.dolphie.app.notify(
                        f"Failed to mount clusterset [$highlight]{clusterset_name}", severity="error"
                    )

        # Remove ClusterSets that no longer exist
        for clusterset_name, container in existing_clusterset_components.items():
            if clusterset_name not in existing_clusterset_names:
                container.parent.remove()

    def create_group_replication_panel():
        if not (dolphie.group_replication or dolphie.innodb_cluster):
            tab.group_replication_container.display = False
            return

        tab.group_replication_container.display = True

        # Prepare group replication variables
        available_variables = {
            "group_replication_view_change_uuid": ("View UUID", dolphie.global_variables),
            "group_replication_communication_stack": ("Communication Stack", dolphie.global_variables),
            "group_replication_consistency": ("Global Consistency", dolphie.global_variables),
            "group_replication_paxos_single_leader": ("Paxos Single Leader", dolphie.global_variables),
            "write_concurrency": ("Write Concurrency", dolphie.group_replication_data),
        }

        group_replication_variables = "  ".join(
            f"[$label]{label}[/$label] {source.get(var, 'N/A')}" for var, (label, source) in available_variables.items()
        )

        # Update the panel title
        title_prefix = dolphie.panels.get_key(dolphie.panels.replication.name)
        cluster_title = "InnoDB Cluster" if dolphie.innodb_cluster else "Group Replication"
        cluster_name = dolphie.group_replication_data.get("cluster_name")
        final_cluster_name = (
            cluster_name if cluster_name else dolphie.global_variables.get("group_replication_group_name", "N/A")
        )
        tab.group_replication_title.update(
            f"[b]{title_prefix}{cluster_title} ([$highlight]{final_cluster_name}[/$highlight])"
        )
        tab.group_replication_data.update(group_replication_variables)

        # Generate and sort member tables
        member_tables = create_group_replication_member_table(tab)
        sorted_members = sorted(member_tables.items(), key=lambda x: x[1]["host"])
        existing_member_ids = {member_id for member_id, _ in sorted_members}

        # Query existing member components
        existing_member_components = {
            member.id.split("_")[1]: member for member in tab.dolphie.app.query(f".member_{tab.id}")
        }

        for member_id, member_info in sorted_members:
            member_table = member_info.get("table")
            if not member_table:
                continue

            if member_id in existing_member_components:
                existing_member_components[member_id].update(member_table)
            else:
                try:
                    tab.group_replication_grid.mount(
                        ScrollableContainer(
                            Static(
                                member_table,
                                id=f"member_{member_id}_{tab.id}",
                                classes=f"member_{tab.id}",
                            ),
                            id=f"member_container_{member_id}_{tab.id}",
                            classes=f"member_container_{tab.id} member_container",
                        )
                    )
                except DuplicateIds:
                    tab.dolphie.app.notify(
                        f"Failed to mount member [$highlight]{member_info['host']}", severity="error"
                    )

        # Remove members that no longer exist
        for member_id, container in existing_member_components.items():
            if member_id not in existing_member_ids:
                container.parent.remove()

    def create_replication_panel():
        if not dolphie.replication_status:
            tab.replication_container.display = False
            return None

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
                if last_error_time == "0000-00-00 00:00:00.000000":
                    last_error_time = ""

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

        if tab.dolphie.connection_source_alt == ConnectionSource.mariadb:
            available_replication_variables = {
                "slave_parallel_mode": "parallel_mode",
                "slave_parallel_workers": "parallel_workers",
                "slave_parallel_threads": "parallel_threads",
                "log_slave_updates": "log_slave_updates",
            }
        else:
            if dolphie.is_mysql_version_at_least("8.0.22"):
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

        replication_variables = ""
        for setting_variable, setting_display_name in available_replication_variables.items():
            value = dolphie.global_variables.get(setting_variable, "N/A")
            replication_variables += f"[$label]{setting_display_name}[/$label] {value}  "
        replication_variables = replication_variables.strip()

        tab.replication_variables.update(replication_variables)

        tab.replication_status.update(create_replication_table(tab))

    create_replication_panel()
    create_group_replication_panel()
    create_clusterset_panel()


# This function isn't in create_panel() because it's called as part of the replica worker instead of the main worker
def create_replica_panel(tab: Tab):
    dolphie = tab.dolphie

    if not dolphie.replica_manager.replicas:
        tab.replicas_container.display = False
        return

    tab.replicas_container.display = True
    tab.replicas_loading_indicator.display = False

    # Update replicas title
    num_replicas = len(dolphie.replica_manager.available_replicas)
    title_prefix = dolphie.panels.get_key(dolphie.panels.replication.name)
    tab.replicas_title.update(f"[b]{title_prefix}Replicas ([$highlight]{num_replicas}[/$highlight])")

    # Get sorted replica connections and initialize existing replica IDs
    sorted_replicas = dolphie.replica_manager.get_sorted_replicas()
    existing_replica_ids = {replica.row_key for replica in sorted_replicas if replica.table}

    # Query existing replica components
    existing_replica_components = {
        replica.id.split("_")[1]: replica for replica in tab.dolphie.app.query(f".replica_{tab.id}")
    }

    for replica in sorted_replicas:
        if not replica.table:
            continue

        if replica.row_key in existing_replica_components:
            existing_replica_components[replica.row_key].update(replica.table)
        else:
            try:
                tab.replicas_grid.mount(
                    ScrollableContainer(
                        Static(
                            replica.table,
                            id=f"replica_{replica.row_key}_{tab.id}",
                            classes=f"replica_{tab.id}",
                        ),
                        id=f"replica_container_{replica.row_key}_{tab.id}",
                        classes=f"replica_container_{tab.id} replica_container",
                    )
                )
            except DuplicateIds:
                tab.dolphie.app.notify(
                    f"Failed to mount replica [$highlight]{replica.host}:{replica.port}", severity="error"
                )

    # Remove replicas that no longer exist
    for replica_id, replica_container in existing_replica_components.items():
        if replica_id not in existing_replica_ids:
            replica_container.parent.remove()


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

    io_thread_running = "[green]ON[/green]" if data.get(f"{replica_prefix}_IO_Running") == "Yes" else "[red]OFF[/red]"
    sql_thread_running = "[green]ON[/green]" if data.get(f"{replica_prefix}_SQL_Running") == "Yes" else "[red]OFF[/red]"

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
                replica_lag -= int(sql_delay)

        lag_color = "green"
        if replica_lag >= 20:
            lag_color = "red"
        elif replica_lag >= 10:
            lag_color = "yellow"

        formatted_replica_lag = f"[{lag_color}]{format_time(replica_lag)}[/{lag_color}]"

    table = Table(show_header=False, box=None)
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

    if formatted_replica_lag is None or sql_thread_running == "[red]OFF[/red]":
        table.add_row("[label]Lag", "")
    else:
        table.add_row(
            "[label]Lag",
            "%s [label]Speed[/label] %s %s" % (formatted_replica_lag, data["Replica_Speed"], replication_delay),
        )

    if dashboard_table:
        table.add_row("[label]Binlog IO", "%s" % primary_log_file)
        table.add_row("[label]Binlog SQL", "%s" % relay_primary_log_file)
        table.add_row("[label]Relay Log ", "%s" % data["Relay_Log_File"])
        table.add_row("[label]GTID", "%s" % gtid_status)
        table.add_row("[label]State", "%s" % replica_sql_running_state)
    else:
        table.add_row(
            "[label]Binlog IO",
            "%s ([dark_gray]%s[/dark_gray])" % (primary_log_file, read_primary_log_pos),
        )
        table.add_row(
            "[label]Binlog SQL",
            "%s ([dark_gray]%s[/dark_gray])" % (relay_primary_log_file, exec_primary_log_pos),
        )
        table.add_row(
            "[label]Relay Log",
            "%s ([dark_gray]%s[/dark_gray])" % (data["Relay_Log_File"], data["Relay_Log_Pos"]),
        )

    if dashboard_table:
        table.add_row("[label]GTID", "%s" % gtid_status)
        table.add_row("[label]State", "%s" % replica_sql_running_state)
    else:
        ssl_enabled = "ON" if primary_ssl_allowed == "Yes" else "OFF"
        table.add_row("[label]SSL", "%s" % ssl_enabled)

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
                table.add_row("[label]%s" % error_type.replace("_", " "), "[red]%s[/red]" % error_message)
        else:
            table.add_row("[label]IO State", "%s" % replica_io_state)
            table.add_row("[label]SQL State", "%s" % replica_sql_running_state)

        if mysql_gtid_enabled:
            executed_gtid_set = data["Executed_Gtid_Set"]
            retrieved_gtid_set = data["Retrieved_Gtid_Set"]

            table.add_row("[label]GTID", "%s" % gtid_status)

            if replica:
                replica_primary_server_uuid = None
                if dolphie.replication_status:
                    replica_primary_server_uuid = dolphie.replication_status.get(uuid_key)

                def remove_primary_uuid_gtid_set(gtid_sets):
                    lines = gtid_sets.splitlines()

                    # We ignore the GTID set that relates to the replica's primary UUID/host's UUID since
                    # we would always have errant transactions if not
                    server_uuid_set = {dolphie.server_uuid}
                    if replica_primary_server_uuid:
                        server_uuid_set.add(replica_primary_server_uuid)

                    # Filter lines based on server_uuid_set
                    remaining_lines = [line for line in lines if not any(uuid in line for uuid in server_uuid_set)]

                    # Join the modified lines with newlines
                    result = "\n".join(remaining_lines)

                    # Remove trailing comma if it exists
                    if result.endswith(","):
                        result = result[:-1]

                    return result

                replica_gtid_set = remove_primary_uuid_gtid_set(executed_gtid_set)
                primary_gtid_set = remove_primary_uuid_gtid_set(dolphie.global_variables.get("gtid_executed"))

                replica.connection.execute(
                    f"SELECT GTID_SUBTRACT('{replica_gtid_set}', '{primary_gtid_set}') AS errant_trxs"
                )
                gtid_data = replica.connection.fetchone()
                if gtid_data.get("errant_trxs"):
                    errant_trx = f"[red]{gtid_data['errant_trxs']}[/red]"
                else:
                    errant_trx = "[green]None[/green]"

                table.add_row("[label]Errant TRX", "%s" % errant_trx)

                # If this replica has replicas, use its primary server UUID, else use its own server UUID
                if replica_primary_server_uuid:
                    primary_uuid = replica_primary_server_uuid
                else:
                    primary_uuid = dolphie.server_uuid

            def color_gtid_set(match):
                source_id = match.group(1)
                transaction_id = match.group(2)

                if source_id == primary_uuid:
                    return f"[highlight]{source_id}[/highlight]:{transaction_id}"
                else:
                    return f"[dark_gray]{source_id}:{transaction_id}[/dark_gray]"

            # Example GTID: 3beacd96-6fe3-18ec-9d95-b4592zec4b45:1-26
            pattern = re.compile(r"\b(\w+(?:-\w+){4}):(.+)\b")
            retrieved_gtid_set = pattern.sub(color_gtid_set, retrieved_gtid_set.replace(",", ""))
            executed_gtid_set = pattern.sub(color_gtid_set, executed_gtid_set.replace(",", ""))

            table.add_row("[label]Retrieved GTID", "%s" % retrieved_gtid_set)
            table.add_row("[label]Executed GTID", "%s" % executed_gtid_set)
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
                    if replica_primary_server_id:
                        primary_id = replica_primary_server_id
                    else:
                        primary_id = dolphie.global_variables.get("server_id")

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


def create_group_replication_member_table(tab: Tab):
    dolphie = tab.dolphie

    if not dolphie.group_replication_members:
        return

    group_replica_tables = {}
    for row in dolphie.group_replication_members:
        trx_queued = row.get("COUNT_TRANSACTIONS_IN_QUEUE", "N/A")
        trx_checked = row.get("COUNT_TRANSACTIONS_CHECKED", "N/A")
        trx_detected = row.get("COUNT_TRANSACTIONS_DETECTED", "N/A")
        trx_rows_validating = row.get("COUNT_TRANSACTIONS_ROWS_VALIDATING", "N/A")
        trx_applied_queue = row.get("COUNT_TRANSACTIONS_REMOTE_IN_APPLIER", "N/A")
        trx_applied = row.get("COUNT_TRANSACTIONS_REMOTE_APPLIED", "N/A")
        trx_local_proposed = row.get("COUNT_TRANSACTIONS_LOCAL_PROPOSED", "N/A")
        trx_local_rollback = row.get("COUNT_TRANSACTIONS_LOCAL_ROLLBACK", "N/A")

        member_role = row.get("MEMBER_ROLE", "N/A")
        if member_role == "PRIMARY":
            member_role = f"[b][highlight]{member_role}[/highlight]"

        member_state = row.get("MEMBER_STATE", "N/A")
        if member_state == "ONLINE":
            member_state = f"[green]{member_state}[/green]"
        else:
            member_state = f"[red]{member_state}[/red]"

        table = Table(box=None, show_header=False)
        table.add_column()
        table.add_column()

        table.add_row("[b][light_blue]Member", f"[light_blue]{row.get('MEMBER_HOST')}:{row.get('MEMBER_PORT')}")
        table.add_row("[label]UUID", "%s" % row.get("MEMBER_ID"))
        table.add_row("[label]Role", member_role)
        table.add_row("[label]State", member_state)
        table.add_row("[label]Version", row.get("MEMBER_VERSION", "N/A"))

        table.add_row(
            "[label]Conflict",
            f"[label]Queue[/label]: {format_number(trx_queued)} [label]Checked[/label]:"
            f" {format_number(trx_checked)} [label]Detected[/label]: {format_number(trx_detected)}",
        )
        table.add_row(
            "[label]Applied",
            f"{format_number(trx_applied)} [label]Queue[/label]: {format_number(trx_applied_queue)} ",
        )
        table.add_row(
            "[label]Local",
            f"[label]Proposed[/label]: {format_number(trx_local_proposed)} [label]Rollback[/label]:"
            f" {format_number(trx_local_rollback)}",
        )
        table.add_row(
            "[label]Rows", f"{format_number(trx_rows_validating)} [dark_gray](used for certification)[/dark_gray]"
        )

        group_replica_tables[row.get("MEMBER_ID")] = {
            "host": f"{row.get('MEMBER_HOST')}:{row.get('MEMBER_PORT')}",
            "table": table,
        }

        if row.get("MEMBER_ID") == dolphie.server_uuid and row.get("MEMBER_ROLE") == "PRIMARY":
            dolphie.is_group_replication_primary = True

    return group_replica_tables


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
    replication_status = connection.fetchone() or {}

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

    # If replicas don't match available_replicas, remove replica connections that no longer exist
    if len(dolphie.replica_manager.replicas) != len(dolphie.replica_manager.available_replicas):
        unique_row_keys = {
            dolphie.replica_manager.create_replica_row_key(row.get("host"), row.get("port"))
            for row in dolphie.replica_manager.available_replicas
        }
        to_remove = set(dolphie.replica_manager.replicas.keys()) - unique_row_keys
        for row_key in to_remove:
            dolphie.replica_manager.remove_replica(row_key)

    for row in dolphie.replica_manager.available_replicas:
        replica_error = None
        host = dolphie.get_hostname(row["host"].split(":")[0])

        # MariaDB has no way of mapping a replica in processlist (AFAIK) to a specific port from SHOW SLAVE HOSTS
        # So we have to loop through available ports and manage it ourselves.
        if dolphie.connection_source_alt == ConnectionSource.mariadb:
            row_port = row.get("port")
            if not row_port:
                assigned_port = None
                # Loop through available ports
                for port_data in dolphie.replica_manager.ports.values():
                    port = port_data.get("port", 3306)

                    # If the port is not in use, try to connect to with it
                    if not port_data.get("in_use"):
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sock.settimeout(2)

                        try:
                            # Try to connect to the host and port
                            sock.connect((host, port))
                            port_data["in_use"] = True  # If we can connect, mark the port as in use to not use it again
                            assigned_port = port

                            break
                        except (socket.timeout, socket.error, ConnectionRefusedError):
                            continue  # Continue to the next available port
                        finally:
                            sock.close()

                if not assigned_port:
                    replica_error = "No available port found for MariaDB replica"
            else:
                assigned_port = row_port
        else:
            # We can correlate the replica in the processlist to a specific port from SHOW SLAVE HOSTS with MySQL
            assigned_port = dolphie.replica_manager.ports.get(row.get("replica_uuid"), {}).get("port", 3306)

        # Update the port of available_replicas so it can be used for the row_key
        row["port"] = assigned_port

        # Create a unique row key for the replica since we now have the assigned port for it
        row_key = f"{dolphie.replica_manager.create_replica_row_key(row.get('host'), assigned_port)}"

        host_and_port = f"{host}:{assigned_port}" if assigned_port else host

        replica = dolphie.replica_manager.get_replica(row_key)
        if not replica:
            replica = dolphie.replica_manager.add_replica(
                row_key=row_key, thread_id=row.get("id"), host=host_and_port, port=assigned_port
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
