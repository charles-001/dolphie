import re
from datetime import timedelta

from dolphie.DataTypes import Replica
from dolphie.Modules.Functions import format_number
from dolphie.Modules.ManualException import ManualException
from dolphie.Modules.MySQL import Database
from dolphie.Modules.Queries import MySQLQueries
from dolphie.Modules.TabManager import Tab
from rich import box
from rich.align import Align
from rich.panel import Panel
from rich.style import Style
from rich.table import Table
from textual.containers import Container, ScrollableContainer
from textual.widgets import Static


def create_panel(tab: Tab):
    dolphie = tab.dolphie

    if (
        dolphie.display_replication_panel
        and not dolphie.available_replicas
        and not dolphie.replication_status
        and not dolphie.galera_cluster
        and not dolphie.group_replication
        and not dolphie.innodb_cluster
        and not dolphie.innodb_cluster_read_replica
    ):
        return "[yellow]Replication/Replicas panel has no data to display[/yellow]"

    def create_group_replication_panel():
        if not dolphie.group_replication and not dolphie.innodb_cluster:
            tab.group_replication_container.display = False
            return None

        tab.group_replication_container.display = True

        available_group_replication_variables = {
            "group_replication_view_change_uuid": [dolphie.global_variables, "View UUID"],
            "group_replication_communication_stack": [dolphie.global_variables, "Communication Stack"],
            "group_replication_consistency": [dolphie.global_variables, "Global Consistency"],
            "group_replication_paxos_single_leader": [dolphie.global_variables, "Paxos Single Leader"],
            "write_concurrency": [dolphie.group_replication_data, "Write Concurrency"],
        }

        group_replication_variables = ""
        for variable, setting in available_group_replication_variables.items():
            value = setting[0].get(variable, "N/A")
            group_replication_variables += f"[b light_blue]{setting[1]}[/b light_blue] {value}  "

        title = "Group Replication"
        if dolphie.innodb_cluster:
            title = "InnoDB Cluster"
        title = (
            f"[b]{title} "
            f"([highlight]{dolphie.global_variables.get('group_replication_group_name', 'N/A')}[/highlight])\n"
        )
        tab.group_replication_title.update(title)
        tab.group_replication_data.update(group_replication_variables)

        group_replication_member_tables = create_group_replication_member_table(tab)
        sorted_replication_members = sorted(group_replication_member_tables.items(), key=lambda x: x[1]["host"])
        if group_replication_member_tables:
            # Quick & easy way to remove members that no longer exist
            if len(dolphie.group_replication_members) != len(dolphie.app.query(f".group_member_{tab.id}")):
                for member in dolphie.app.query(f".group_replication_container_{tab.id}"):
                    member.remove()

            # Iterate over members in pairs of 3
            for i in range(0, len(sorted_replication_members), 3):
                member_pair = list(sorted_replication_members)[i : i + 3]

                # Create a container for each pair of members
                container_id = f"group_replication_container_{i}_{tab.id}"
                container = tab.dolphie.app.query(f"#{container_id}")
                if container:
                    container = container[0]
                else:
                    is_last_row = i + 3 >= len(sorted_replication_members)

                    container_classes = f"group_replication_container group_replication_container_{tab.id}"
                    if not is_last_row:
                        container_classes += " pad_bottom_1"

                    num_containers = len(dolphie.app.query(f".group_replication_container_{tab.id}"))
                    container = Container(id=container_id, classes=container_classes)
                    tab.group_replication_container.mount(container, after=2 + num_containers)

                for member_id, _ in member_pair:
                    member_table = group_replication_member_tables.get(member_id).get("table")
                    if not member_table:
                        continue

                    existing_member = tab.dolphie.app.query(f"#group_member_{member_id}_{tab.id}")
                    if existing_member:
                        existing_member[0].update(member_table)
                    else:
                        container.mount(
                            Container(
                                ScrollableContainer(
                                    Static(
                                        member_table,
                                        id=f"group_member_{member_id}_{tab.id}",
                                        classes=f"group_member_{tab.id}",
                                    ),
                                ),
                            ),
                        )

    def create_replica_panel():
        if not dolphie.replica_manager.replicas and dolphie.available_replicas:
            tab.replicas_container.display = True
            tab.replicas_title.update(
                f"[b]Loading [highlight]{len(dolphie.available_replicas)}[/highlight] replicas...\n"
            )
            tab.replicas_loading_indicator.display = True
        elif dolphie.replica_manager.replicas:
            tab.replicas_container.display = True
            tab.replicas_loading_indicator.display = False

            tab.replicas_title.update(f"[b]Replicas ([highlight]{len(dolphie.available_replicas)}[/highlight])\n")
            sorted_replica_connections = dolphie.replica_manager.get_sorted_replicas()

            # Quick & easy way to remove replicas that no longer exist
            if len(sorted_replica_connections) != len(dolphie.app.query(f".replica_{tab.id}")):
                for member in dolphie.app.query(f".replica_container_{tab.id}"):
                    member.remove()

            # Iterate over replicas in pairs of 2
            for i in range(0, len(sorted_replica_connections), 2):
                replica_pair = list(sorted_replica_connections)[i : i + 2]

                # Create a container for each pair of replicas
                container_id = f"replica_container_{i}_{tab.id}"
                container = tab.dolphie.app.query(f"#{container_id}")
                if container:
                    container = container[0]
                else:
                    is_last_row = i + 2 >= len(sorted_replica_connections)

                    container_classes = f"replica_container replica_container_{tab.id}"
                    if not is_last_row:
                        container_classes += " pad_bottom_1"

                    num_containers = len(dolphie.app.query(f".replica_container_{tab.id}"))
                    container = Container(id=container_id, classes=container_classes)
                    tab.replicas_container.mount(container, after=2 + num_containers)

                for replica in replica_pair:
                    replica_id = replica.thread_id
                    replica_table = replica.table

                    if not replica_table:
                        continue

                    existing_replica = tab.dolphie.app.query(f"#replica_{replica_id}_{tab.id}")
                    if existing_replica:
                        existing_replica[0].update(replica_table)
                    else:
                        container.mount(
                            Container(
                                ScrollableContainer(
                                    Static(
                                        replica_table, id=f"replica_{replica_id}_{tab.id}", classes=f"replica_{tab.id}"
                                    ),
                                ),
                            ),
                        )
        else:
            tab.replicas_container.display = False

    def create_cluster_panel():
        if not dolphie.galera_cluster:
            tab.cluster_data.display = False
            return None

        tab.cluster_data.display = True
        tab.cluster_data.update(
            Panel(
                Align.center(
                    "\n[b light_blue]State[/b light_blue] "
                    f" {dolphie.global_status.get('wsrep_local_state_comment', 'N/A')}\n"
                ),
                title="[b white]Cluster",
                box=box.HORIZONTALS,
                border_style="panel_border",
            )
        )

    def create_replication_panel():
        if not dolphie.replication_status:
            tab.replication_container.display = False
            return None

        tab.replication_container.display = True

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

        if dolphie.replication_applier_status:
            table_thread_applier_status = Table(box=None, header_style="#c5c7d2")
            table_thread_applier_status.add_column("Worker", justify="center")
            table_thread_applier_status.add_column("Usage", min_width=6)
            table_thread_applier_status.add_column("Apply Time")
            table_thread_applier_status.add_column("Last Applied Transaction")

            for row in dolphie.replication_applier_status:
                # We use ROLLUP in the query, so the first row is the total for thread_events
                if not row["worker_id"]:
                    total_thread_events = row["total_thread_events"]
                    continue

                last_applied_transaction = row["last_applied_transaction"]
                if row["last_applied_transaction"] and "-" in row["last_applied_transaction"]:
                    source_id_split = row["last_applied_transaction"].split("-")[4].split(":")[0]
                    transaction_id = row["last_applied_transaction"].split(":")[1]
                    last_applied_transaction = f"…[dark_gray]{source_id_split}[/dark_gray]:{transaction_id}"

                table_thread_applier_status.add_row(
                    str(row["worker_id"]),
                    str(round(100 * (row["total_thread_events"] / total_thread_events), 2)) + "%",
                    row["apply_time"],
                    last_applied_transaction,
                )

            tab.replication_thread_applier.update(table_thread_applier_status)
            tab.replication_thread_applier_container.display = True
        else:
            tab.replication_thread_applier_container.display = False

        tab.replication_variables.update(replication_variables)
        tab.replication_status.update(create_replication_table(tab))

    create_replication_panel()
    create_replica_panel()
    create_cluster_panel()
    create_group_replication_panel()


def create_replication_table(tab: Tab, data=None, dashboard_table=False, replica: Replica = None) -> Table:
    dolphie = tab.dolphie

    # When replica_thread_id is specified, that means we're creating a table for a replica and not replication
    if replica:
        if replica.previous_sbm is not None:
            replica_previous_replica_sbm = replica.previous_sbm

        sbm_source, data["Seconds_Behind_Master"] = dolphie.fetch_replication_data(replica=replica)
    else:
        data = dolphie.replication_status
        replica_previous_replica_sbm = dolphie.previous_replica_sbm

        sbm_source = dolphie.replica_lag_source
        data["Seconds_Behind_Master"] = dolphie.replica_lag

    speed = 0
    if data["Seconds_Behind_Master"] is not None:
        replica_sbm = data["Seconds_Behind_Master"]

        if replica:
            replica.previous_sbm = replica_sbm

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
        table.add_column()
    else:
        table.add_column()
        table.add_column(overflow="fold")

    if replica:
        table.add_row("[b][light_blue]Host", "[light_blue]%s" % replica.host)
        table.add_row("[label]Version", "%s" % replica.mysql_version)
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
            if replica:
                value = data.get(status_filter)
            else:
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
            if replica:

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

                replica.connection.execute(
                    f"SELECT GTID_SUBTRACT('{replica_gtid_set}', '{primary_gtid_set}') AS errant_trxs"
                )
                gtid_data = replica.connection.fetchone()
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


def create_group_replication_member_table(tab: Tab):
    dolphie = tab.dolphie

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

    return group_replica_tables


def fetch_replicas(tab: Tab):
    dolphie = tab.dolphie

    # Only run this query if we don't have replica ports or if the number of replicas has changed
    if len(dolphie.replica_manager.replicas) != len(dolphie.available_replicas):
        # Remove replica connections that no longer exist
        unique_ids = set(row["id"] for row in dolphie.available_replicas)
        for thread_id in list(dolphie.replica_manager.replicas.keys()):
            if thread_id not in unique_ids:
                dolphie.replica_manager.remove(thread_id)

        dolphie.replica_manager.ports = {}
        dolphie.main_db_connection.execute(MySQLQueries.get_replicas)
        replica_data = dolphie.main_db_connection.fetchall()
        for row in replica_data:
            dolphie.replica_manager.ports[row["Slave_UUID"]] = row["Port"]

    for row in dolphie.available_replicas:
        replica_error = None

        thread_id = row["id"]

        host = dolphie.get_hostname(row["host"].split(":")[0])
        port = dolphie.replica_manager.ports.get(row["replica_uuid"], 3306)

        # This lets us connect to replicas on the same host as the primary if we're connecting remotely
        if host == "localhost" or host == "127.0.0.1":
            host = dolphie.host

        if thread_id not in dolphie.replica_manager.replicas:
            host_and_port = "%s:%s" % (host, port)
            try:
                replica = dolphie.replica_manager.add(thread_id=thread_id, host=host_and_port)
                replica.connection = Database(
                    app=dolphie.app,
                    tab_name=dolphie.tab_name,
                    host=host,
                    user=dolphie.user,
                    password=dolphie.password,
                    port=port,
                    socket=None,
                    ssl=dolphie.ssl,
                    save_connection_id=False,
                )

                # Save the MySQL version for the replica
                version_split = replica.connection.fetch_value_from_field("SELECT @@version").split(".")
                replica.mysql_version = "%s.%s.%s" % (
                    version_split[0],
                    version_split[1],
                    version_split[2].split("-")[0],
                )
            except ManualException as e:
                replica_error = e.reason

        try:
            replica = dolphie.replica_manager.get(thread_id)
            if replica.connection:
                replica.connection.execute(MySQLQueries.replication_status)
                replica_data = replica.connection.fetchone()

                if replica_data:
                    replica.table = create_replication_table(tab, data=replica_data, replica=replica)
        except ManualException as e:
            replica_error = e.reason

        if replica_error:
            table = Table(box=None, show_header=False)
            table.add_column()
            table.add_column(overflow="fold")

            table.add_row("[label]User", row["user"])
            table.add_row("[label]Error", "[red]%s[/red]" % replica_error)

            replica.table = table
