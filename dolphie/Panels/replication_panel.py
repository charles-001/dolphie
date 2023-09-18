import re
from datetime import timedelta

import pymysql
from dolphie import Dolphie
from dolphie.Modules.Functions import format_number
from dolphie.Modules.Queries import MySQLQueries
from rich import box
from rich.align import Align
from rich.console import Group
from rich.panel import Panel
from rich.style import Style
from rich.table import Table


def create_panel(dolphie: Dolphie) -> Panel:
    if (
        dolphie.display_replication_panel
        and not dolphie.replica_data
        and not dolphie.replication_status
        and not dolphie.galera_cluster
        and not dolphie.group_replication
        and not dolphie.innodb_cluster
        and not dolphie.innodb_cluster_read_replica
    ):
        return "[yellow]This panel has no data to display[/yellow]"

    def create_group_replication_panel():
        if not dolphie.group_replication and not dolphie.innodb_cluster:
            return None

        available_group_replication_variables = {
            "group_replication_view_change_uuid": [dolphie.global_variables, "View UUID"],
            "group_replication_communication_stack": [dolphie.global_variables, "Communication Stack"],
            "group_replication_consistency": [dolphie.global_variables, "Global Consistency"],
            "group_replication_paxos_single_leader": [dolphie.global_variables, "Paxos Single Leader"],
            "write_concurrency": [dolphie.group_replication_data, "Write Concurrency"],
            "protocol_version": [dolphie.group_replication_data, "Protocol Version"],
        }

        group_replication_variables = ""
        for variable, setting in available_group_replication_variables.items():
            value = setting[0].get(variable, "N/A")
            group_replication_variables += f"[b light_blue]{setting[1]}[/b light_blue] {value}  "

        group_replication_member_tables = create_group_replication_member_table(dolphie)

        table_group_members_grid = Table.grid()
        if group_replication_member_tables:
            for i in range(0, len(group_replication_member_tables), 3):
                table_group_members_grid.add_row(
                    *[table for _, table in sorted(list(group_replication_member_tables.items()))[i : i + 3]]
                )

        title = "Group Replication"
        if dolphie.innodb_cluster:
            title = "InnoDB Cluster"

        return Panel(
            Group(Align.center(group_replication_variables), Align.center(table_group_members_grid)),
            title=(
                f"[b white]{title}"
                f" ([highlight]{dolphie.global_variables.get('group_replication_group_name', 'N/A')}[/highlight])"
            ),
            box=box.HORIZONTALS,
            border_style="panel_border",
        )

    def create_replica_panel():
        if not dolphie.replica_tables and not dolphie.replica_data:
            return None

        if dolphie.replica_tables:
            table_grid = Table.grid()

            num_replicas = len(dolphie.replica_tables)
            for i in range(0, num_replicas, 2):
                table_grid.add_row(*[table for _, table in sorted(list(dolphie.replica_tables.items()))[i : i + 2]])

            content = table_grid
        elif dolphie.replica_data:
            num_replicas = len(dolphie.replica_data)
            content = "\nLoading...\n"

        title = "Replica" if num_replicas == 1 else "Replicas"
        return Panel(
            Align.center(content),
            title=f"[b white]{num_replicas} {title}",
            box=box.HORIZONTALS,
            border_style="panel_border",
        )

    def create_cluster_panel():
        if not dolphie.galera_cluster:
            return None

        return Panel(
            Align.center(
                "\n[b light_blue]State[/b light_blue] "
                f" {dolphie.global_status.get('wsrep_local_state_comment', 'N/A')}\n"
            ),
            title="[b white]Cluster",
            box=box.HORIZONTALS,
            border_style="panel_border",
        )

    def create_replication_panel():
        if not dolphie.replication_status:
            return None

        replication_variables = ""
        available_replication_variables = {
            "binlog_transaction_dependency_tracking": "dependency_tracking",
            "slave_parallel_type": "parallel_type",
            "slave_parallel_workers": "parallel_workers",
            "slave_preserve_commit_order": "preserve_commit_order",
        }

        for setting_variable, setting_display_name in available_replication_variables.items():
            value = dolphie.global_variables.get(setting_variable, "N/A")
            replication_variables += f"[b light_blue]{setting_display_name}[/b light_blue] {value}  "
        replication_variables = replication_variables.strip()

        table_thread_applier_status = Table()
        if dolphie.replication_applier_status:
            table_thread_applier_status = Table(title_style=Style(bold=True), style="panel_border", box=box.ROUNDED)
            table_thread_applier_status.add_column("Worker", justify="center")
            table_thread_applier_status.add_column("Usage", min_width=6)
            table_thread_applier_status.add_column("Apply Time")
            table_thread_applier_status.add_column("Last Applied Transaction")

            for row in dolphie.replication_applier_status:
                # We use ROLLUP in the query, so the first row is the total for thread_events
                if not row["worker_id"]:
                    total_thread_events = row["total_thread_events"]
                    continue

                last_applied_transaction = ""
                if row["last_applied_transaction"]:
                    source_id_split = row["last_applied_transaction"].split("-")[4].split(":")[0]
                    transaction_id = row["last_applied_transaction"].split(":")[1]
                    last_applied_transaction = f"…[dark_gray]{source_id_split}[/dark_gray]:{transaction_id}"

                table_thread_applier_status.add_row(
                    str(row["worker_id"]),
                    str(round(100 * (row["total_thread_events"] / total_thread_events), 2)) + "%",
                    row["apply_time"],
                    last_applied_transaction,
                )

        table_grid_replication = Table.grid()
        table_grid_replication.add_row(create_replication_table(dolphie), table_thread_applier_status)

        return Panel(
            Group(Align.center(replication_variables), Align.center(table_grid_replication)),
            title="[b white]Replication",
            box=box.HORIZONTALS,
            border_style="panel_border",
        )

    group_panels = [
        create_group_replication_panel(),
        create_cluster_panel(),
        create_replication_panel(),
        create_replica_panel(),
    ]

    return Group(*[panel for panel in group_panels if panel])


def create_replication_table(dolphie: Dolphie, data=None, dashboard_table=False, replica_thread_id=None) -> Table:
    # When replica_thread_id is specified, that means we're creating a table for a replica and not replication
    if replica_thread_id:
        if dolphie.replica_connections[replica_thread_id]["previous_sbm"] is not None:
            replica_previous_replica_sbm = dolphie.replica_connections[replica_thread_id]["previous_sbm"]

        db_cursor = dolphie.replica_connections[replica_thread_id]["cursor"]
        sbm_source, data["Seconds_Behind_Master"] = dolphie.fetch_replication_data(replica_cursor=db_cursor)
    else:
        data = dolphie.replication_status
        replica_previous_replica_sbm = dolphie.previous_replica_sbm

        sbm_source = dolphie.replica_lag_source
        data["Seconds_Behind_Master"] = dolphie.replica_lag

    speed = 0
    if data["Seconds_Behind_Master"] is not None:
        replica_sbm = data["Seconds_Behind_Master"]

        if replica_thread_id:
            dolphie.replica_connections[replica_thread_id]["previous_sbm"] = replica_sbm

        if replica_previous_replica_sbm and replica_sbm < replica_previous_replica_sbm:
            speed = round((replica_previous_replica_sbm - replica_sbm) / dolphie.polling_latency)

        if replica_sbm >= 20:
            lag = "[red]%s" % "{:0>8}[/red]".format(str(timedelta(seconds=replica_sbm)))
        elif replica_sbm >= 10:
            lag = "[yellow]%s[/yellow]" % "{:0>8}".format(str(timedelta(seconds=replica_sbm)))
        else:
            lag = "[green]%s[/green]" % "{:0>8}".format(str(timedelta(seconds=replica_sbm)))

    data["Master_Host"] = dolphie.get_hostname(data["Master_Host"])
    mysql_gtid_enabled = False
    mariadb_gtid_enabled = False
    gtid_status = "OFF"
    if "Executed_Gtid_Set" in data and data["Executed_Gtid_Set"]:
        mysql_gtid_enabled = True
        gtid_status = "ON"
    if "Using_Gtid" in data and data["Using_Gtid"] != "No":
        mariadb_gtid_enabled = True
        gtid_status = data["Using_Gtid"]

    table_title = ""
    if dashboard_table:
        table_title = "Replication"

    table = Table(
        show_header=False,
        box=box.ROUNDED,
        title=table_title,
        title_style=Style(bold=True),
        style="table_border",
    )

    table.add_column()
    if dashboard_table:
        table.add_column(max_width=25, no_wrap=True)
    else:
        table.add_column(min_width=60, overflow="fold")

    if replica_thread_id:
        table.add_row("[label]Host", "%s" % dolphie.replica_connections[replica_thread_id]["host"])
    else:
        table.add_row("[label]Primary", "%s" % data["Master_Host"])

    if not dashboard_table:
        table.add_row("[label]User", "%s" % data["Master_User"])

    if data["Slave_IO_Running"].lower() == "yes":
        io_thread_running = "[green]Yes[/green]"
    else:
        io_thread_running = "[red]NO[/red]"

    if data["Slave_SQL_Running"].lower() == "yes":
        sql_thread_running = "[green]Yes[/green]"
    else:
        sql_thread_running = "[red]NO[/red]"

    table.add_row(
        "[label]Thread",
        "[label]IO %s [label]SQL %s" % (io_thread_running, sql_thread_running),
    )

    lag_source = "Lag"
    if sbm_source:
        lag_source = f"Lag ({sbm_source})"

    if data["Seconds_Behind_Master"] is None or data["Slave_SQL_Running"].lower() == "no":
        table.add_row(f"[label]{lag_source}", "")
    else:
        table.add_row(
            "[label]%s" % lag_source,
            "%s [label]Speed[/label] %s" % (lag, speed),
        )

    if dashboard_table:
        table.add_row("[label]Binlog IO", "%s" % (data["Master_Log_File"]))
        table.add_row("[label]Binlog SQL", "%s" % (data["Relay_Master_Log_File"]))
        table.add_row("[label]Relay Log ", "%s" % (data["Relay_Log_File"]))
        table.add_row("[label]GTID", "%s" % gtid_status)
        table.add_row("[label]State", "%s" % data["Slave_SQL_Running_State"])
    else:
        table.add_row(
            "[label]Binlog IO",
            "%s ([dark_gray]%s[/dark_gray])" % (data["Master_Log_File"], data["Read_Master_Log_Pos"]),
        )
        table.add_row(
            "[label]Binlog SQL",
            "%s ([dark_gray]%s[/dark_gray])"
            % (
                data["Relay_Master_Log_File"],
                data["Exec_Master_Log_Pos"],
            ),
        )
        table.add_row(
            "[label]Relay Log",
            "%s ([dark_gray]%s[/dark_gray])" % (data["Relay_Log_File"], data["Relay_Log_Pos"]),
        )

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
            value = dolphie.replication_status.get(status_filter)
            status_filter_formatted = f"Filter: {status_filter.split('Replicate_')[1]}"
            if value:
                table.add_row(f"[label]{status_filter_formatted}", str(value))

        error_types = ["Last_IO_Error", "Last_SQL_Error"]
        errors = [(error_type, data[error_type]) for error_type in error_types if data[error_type]]

        if errors:
            for error_type, error_message in errors:
                table.add_row("[label]%s" % error_type.replace("_", " "), "[red]%s[/red]" % error_message)
        else:
            table.add_row("[label]IO State", "%s" % data["Slave_IO_State"])
            table.add_row("[label]SQL State", "%s" % data["Slave_SQL_Running_State"])

        if mysql_gtid_enabled:
            primary_uuid = data["Master_UUID"]
            executed_gtid_set = data["Executed_Gtid_Set"]
            retrieved_gtid_set = data["Retrieved_Gtid_Set"]

            table.add_row("[label]Auto Position", "%s" % data["Auto_Position"])

            # We find errant transactions for replicas here
            if replica_thread_id:

                def remove_primary_uuid_gtid_set(gtid_sets):
                    lines = gtid_sets.splitlines()

                    # We ignore the GTID set that relates to the replica's primary UUID/host's UUID since
                    # we would always have errant transactions if not
                    server_uuid_set = {dolphie.server_uuid}
                    if dolphie.replication_primary_server_uuid:
                        server_uuid_set.add(dolphie.replication_primary_server_uuid)

                    # Filter lines based on server_uuid_set
                    remaining_lines = [line for line in lines if not any(uuid in line for uuid in server_uuid_set)]

                    # Join the modified lines with newlines
                    result = "\n".join(remaining_lines)

                    # Remove trailing comma if it exists
                    if result.endswith(","):
                        result = result[:-1]

                    return result

                replica_gtid_set = remove_primary_uuid_gtid_set(executed_gtid_set)
                primary_gtid_set = remove_primary_uuid_gtid_set(dolphie.global_variables["gtid_executed"])

                db_cursor.execute(f"SELECT GTID_SUBTRACT('{replica_gtid_set}', '{primary_gtid_set}') AS errant_trxs")
                gtid_data = db_cursor.fetchone()
                if gtid_data.get("errant_trxs"):
                    errant_trx = f"[red]{gtid_data['errant_trxs']}[/red]"
                else:
                    errant_trx = "[green]None[/green]"

                table.add_row("[label]Errant TRX", "%s" % errant_trx)

                # Since this is for the replica view, we use the host's UUID since its the primary
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
            table.add_row("[label]GTID IO Pos", "%s" % data["Gtid_IO_Pos"])

    return table


def create_group_replication_member_table(dolphie: Dolphie):
    if not dolphie.group_replication_members:
        return None

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
            table_border = "highlight"
            member_role = f"[highlight]{member_role}[/highlight]"
        else:
            table_border = "table_border"

        member_state = row.get("MEMBER_STATE", "N/A")
        if member_state == "ONLINE":
            member_state = f"[green]{member_state}[/green]"
        else:
            member_state = f"[red]{member_state}[/red]"

        table = Table(box=box.ROUNDED, show_header=False, style=table_border)
        table.add_column()
        table.add_column()

        table.add_row("[label]Member", f"{row.get('MEMBER_HOST')}:{row.get('MEMBER_PORT')}")
        table.add_row("[label]UUID", "%s" % row.get("MEMBER_ID"))
        table.add_row("[label]Role", member_role)
        table.add_row("[label]State", member_state)
        table.add_row("[label]Version", row.get("MEMBER_VERSION", "N/A"))

        table.add_row(
            "[label]Conflict",
            (
                f"[label]Queue[/label]: {format_number(trx_queued)} [label]Checked[/label]:"
                f" {format_number(trx_checked)} [label]Detected[/label]: {format_number(trx_detected)}"
            ),
        )
        table.add_row(
            "[label]Applied",
            f"{format_number(trx_applied)} [label]Queue[/label]: {format_number(trx_applied_queue)} ",
        )
        table.add_row(
            "[label]Local",
            (
                f"[label]Proposed[/label]: {format_number(trx_local_proposed)} [label]Rollback[/label]:"
                f" {format_number(trx_local_rollback)}"
            ),
        )
        table.add_row(
            "[label]Rows", f"{format_number(trx_rows_validating)} [dark_gray](used for certification)[/dark_gray]"
        )

        group_replica_tables[row.get("MEMBER_ID")] = table

    return group_replica_tables


def fetch_replica_table_data(dolphie: Dolphie):
    replica_tables = {}

    # Only run this query if we don't have replica ports or if the number of replicas has changed
    if len(dolphie.replica_connections) != len(dolphie.replica_data):
        # Remove replica connections that no longer exist
        unique_ids = set(row["id"] for row in dolphie.replica_data)
        for thread in list(dolphie.replica_connections.keys()):
            if thread not in unique_ids:
                del dolphie.replica_connections[thread]

        dolphie.replica_ports = {}
        dolphie.main_db_connection.execute(MySQLQueries.get_replicas)
        replica_data = dolphie.main_db_connection.fetchall()
        for row in replica_data:
            dolphie.replica_ports[row["Slave_UUID"]] = row["Port"]

    for row in dolphie.replica_data:
        thread_id = row["id"]

        host = dolphie.get_hostname(row["host"].split(":")[0])
        port = dolphie.replica_ports.get(row["replica_uuid"], 3306)

        # This lets us connect to replicas on the same host as the primary if we're connecting remotely
        if host == "localhost" or host == "127.0.0.1":
            host = dolphie.host

        if thread_id not in dolphie.replica_connections:
            host_and_port = "%s:%s" % (host, port)
            try:
                dolphie.replica_connections[thread_id] = {
                    "host": host_and_port,
                    "connection": pymysql.connect(
                        host=host,
                        user=dolphie.user,
                        passwd=dolphie.password,
                        port=port,
                        ssl=dolphie.ssl,
                        autocommit=True,
                        connect_timeout=3,
                    ),
                    "cursor": None,
                    "previous_sbm": 0,
                }
            except pymysql.Error as e:
                table = Table(box=box.ROUNDED, show_header=False, style="table_border")
                table.add_column()
                table.add_column()

                table.add_row("[label]Host", host_and_port)
                table.add_row("[label]User", row["user"])
                table.add_row("[label]Error", "[red]%s[red]" % e.args[1])

                replica_tables[host_and_port] = table

        replica_connection = dolphie.replica_connections.get(thread_id)
        if replica_connection:
            host_and_port = replica_connection["host"]
            try:
                replica_connection["cursor"] = replica_connection["connection"].cursor(pymysql.cursors.DictCursor)
                replica_connection["cursor"].execute(MySQLQueries.replication_status)
                replica_data = replica_connection["cursor"].fetchone()

                if replica_data:
                    replica_tables[host_and_port] = create_replication_table(
                        dolphie, data=replica_data, replica_thread_id=thread_id
                    )
            except pymysql.Error as e:
                table = Table(box=box.ROUNDED, show_header=False, style="table_border")
                table.add_column()
                table.add_column()

                table.add_row("[label]Host", host_and_port)
                table.add_row("[label]User", row["user"])
                table.add_row("[label]Error", "[red]%s[red]" % e.args[1])

                replica_tables[host_and_port] = table

    return replica_tables
