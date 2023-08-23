import re
from datetime import timedelta

import pymysql
from dolphie import Dolphie
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
        and not dolphie.host_is_cluster
    ):
        return "[#f1fb82]No data to display![/#f1fb82] This host is not a replica and has no replicas connected"

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
            title=f"[b #e9e9e9]{num_replicas} {title}",
            box=box.HORIZONTALS,
            border_style="#6171a6",
        )

    def create_cluster_panel():
        if not dolphie.host_is_cluster:
            return None

        return Panel(
            Align.center(
                f"\n[b #bbc8e8]State[/b #bbc8e8]  {dolphie.global_status.get('wsrep_local_state_comment', 'N/A')}\n"
            ),
            title="[b #e9e9e9]Cluster",
            box=box.HORIZONTALS,
            border_style="#6171a6",
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
            replication_variables += f"[b #bbc8e8]{setting_display_name}[/b #bbc8e8] {value}  "
        replication_variables = replication_variables.strip()

        table_thread_applier_status = Table()
        if dolphie.replication_applier_status:
            table_thread_applier_status = Table(title_style=Style(bold=True), style="#6171a6", box=box.ROUNDED)
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
                    last_applied_transaction = f"…[#969aad]{source_id_split}[/#969aad]:{transaction_id}"

                table_thread_applier_status.add_row(
                    str(row["worker_id"]),
                    str(round(100 * (row["total_thread_events"] / total_thread_events), 2)) + "%",
                    row["apply_time"],
                    last_applied_transaction,
                )

        table_grid_replication = Table.grid()
        table_grid_replication.add_row(create_table(dolphie), table_thread_applier_status)

        return Panel(
            Group(Align.center(replication_variables), Align.center(table_grid_replication)),
            title="[b #e9e9e9]Replication",
            box=box.HORIZONTALS,
            border_style="#6171a6",
        )

    group_panels = [
        create_cluster_panel(),
        create_replication_panel(),
        create_replica_panel(),
    ]

    return Group(*[panel for panel in group_panels if panel])


def create_table(dolphie: Dolphie, data=None, dashboard_table=False, replica_thread_id=None) -> Table:
    table_title_style = Style(bold=True)
    table_line_color = "#6171a6"
    table_box = box.ROUNDED

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

    if data["Slave_IO_Running"].lower() == "no":
        data["Slave_IO_Running"] = "[#fc7979]NO[/#fc7979]"
    else:
        data["Slave_IO_Running"] = "[#54efae]Yes[/#54efae]"

    if data["Slave_SQL_Running"].lower() == "no":
        data["Slave_SQL_Running"] = "[#fc7979]NO[/#fc7979]"
    else:
        data["Slave_SQL_Running"] = "[#54efae]Yes[/#54efae]"

    speed = 0
    if data["Seconds_Behind_Master"] is not None:
        replica_sbm = data["Seconds_Behind_Master"]

        if replica_thread_id:
            dolphie.replica_connections[replica_thread_id]["previous_sbm"] = replica_sbm

        if replica_previous_replica_sbm and replica_sbm < replica_previous_replica_sbm:
            speed = round((replica_previous_replica_sbm - replica_sbm) / dolphie.worker_job_time)

        if replica_sbm >= 20:
            lag = "[#fc7979]%s" % "{:0>8}[/#fc7979]".format(str(timedelta(seconds=replica_sbm)))
        elif replica_sbm >= 10:
            lag = "[#f1fb82]%s[/#f1fb82]" % "{:0>8}".format(str(timedelta(seconds=replica_sbm)))
        else:
            lag = "[#54efae]%s[/#54efae]" % "{:0>8}".format(str(timedelta(seconds=replica_sbm)))

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
        box=table_box,
        title=table_title,
        title_style=table_title_style,
        style=table_line_color,
    )

    table.add_column()
    if dashboard_table:
        table.add_column(max_width=25, no_wrap=True)
    else:
        table.add_column(min_width=60, overflow="fold")

    if replica_thread_id:
        table.add_row("[#c5c7d2]Host", "%s" % dolphie.replica_connections[replica_thread_id]["host"])
    else:
        table.add_row("[#c5c7d2]Primary", "%s" % data["Master_Host"])

    if not dashboard_table:
        table.add_row("[#c5c7d2]User", "%s" % data["Master_User"])

    table.add_row(
        "[#c5c7d2]Thread",
        "[#c5c7d2]IO %s [#c5c7d2]SQL %s" % (data["Slave_IO_Running"], data["Slave_SQL_Running"]),
    )
    if data["Seconds_Behind_Master"] is None:
        table.add_row("[#c5c7d2]Lag", "")
    else:
        lag_source = "Lag"
        if sbm_source:
            lag_source = f"Lag ({sbm_source})"

        table.add_row(
            "[#c5c7d2]%s" % lag_source,
            "%s [#c5c7d2]Speed[/#c5c7d2] %s" % (lag, speed),
        )

    replication_status_filtering = [
        "Replicate_Do_DB",
        "Replicate_Ignore_Table",
        "Replicate_Do_Table",
        "Replicate_Wild_Do_Table",
        "Replicate_Wild_Ignore_Table",
    ]

    if not replica_thread_id:
        for status_filter in replication_status_filtering:
            value = dolphie.replication_status.get(status_filter)
            if value:
                table.add_row(f"[#c5c7d2]{filter}", str(value))

    if dashboard_table:
        table.add_row("[#c5c7d2]Binlog IO", "%s" % (data["Master_Log_File"]))
        table.add_row("[#c5c7d2]Binlog SQL", "%s" % (data["Relay_Master_Log_File"]))
        table.add_row("[#c5c7d2]Relay Log ", "%s" % (data["Relay_Log_File"]))
        table.add_row("[#c5c7d2]GTID", "%s" % gtid_status)
        table.add_row("[#c5c7d2]State", "%s" % data["Slave_SQL_Running_State"])
    else:
        table.add_row(
            "[#c5c7d2]Binlog IO",
            "%s ([#969aad]%s[/#969aad])" % (data["Master_Log_File"], data["Read_Master_Log_Pos"]),
        )
        table.add_row(
            "[#c5c7d2]Binlog SQL",
            "%s ([#969aad]%s[/#969aad])"
            % (
                data["Relay_Master_Log_File"],
                data["Exec_Master_Log_Pos"],
            ),
        )
        table.add_row(
            "[#c5c7d2]Relay Log",
            "%s ([#969aad]%s[/#969aad])" % (data["Relay_Log_File"], data["Relay_Log_Pos"]),
        )

        if mysql_gtid_enabled:
            primary_uuid = data["Master_UUID"]
            executed_gtid_set = data["Executed_Gtid_Set"]
            retrieved_gtid_set = data["Retrieved_Gtid_Set"]

            table.add_row("[#c5c7d2]Auto Position", "%s" % data["Auto_Position"])

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
                if gtid_data:
                    if gtid_data["errant_trxs"]:
                        errant_trx = f"[#fc7979]{gtid_data['errant_trxs']}[/#fc7979]"
                    else:
                        errant_trx = "[#54efae]None[/#54efae]"

                    table.add_row("[#c5c7d2]Errant TRX", "%s" % errant_trx)

                # Since this is for the replica view, we use the host's UUID since its the primary
                primary_uuid = dolphie.server_uuid

            def color_gtid_set(match):
                source_id = match.group(1)
                transaction_id = match.group(2)

                if source_id == primary_uuid:
                    return f"[#91abec]{source_id}[/#91abec]:{transaction_id}"
                else:
                    return f"[#969aad]{source_id}:{transaction_id}[/#969aad]"

            # Example GTID: 3beacd96-6fe3-18ec-9d95-b4592zec4b45:1-26
            pattern = re.compile(r"\b(\w+(?:-\w+){4}):(\d+(?:-\d*)?)\b")
            retrieved_gtid_set = pattern.sub(color_gtid_set, retrieved_gtid_set.replace(",", ""))
            executed_gtid_set = pattern.sub(color_gtid_set, executed_gtid_set.replace(",", ""))

            table.add_row("[#c5c7d2]Retrieved GTID", "%s" % retrieved_gtid_set)
            table.add_row("[#c5c7d2]Executed GTID", "%s" % executed_gtid_set)
        elif mariadb_gtid_enabled:
            table.add_row("[#c5c7d2]GTID IO Pos", "%s" % data["Gtid_IO_Pos"])

        error_types = ["Last_IO_Error", "Last_SQL_Error"]
        errors = [(error_type, data[error_type]) for error_type in error_types if data[error_type]]

        if errors:
            for error_type, error_message in errors:
                table.add_row("[#c5c7d2]%s" % error_type.replace("_", " "), "[#fc7979]%s[/#fc7979]" % error_message)
        else:
            table.add_row("[#c5c7d2]IO State", "%s" % data["Slave_IO_State"])
            table.add_row("[#c5c7d2]SQL State", "%s" % data["Slave_SQL_Running_State"])

    return table


def fetch_replica_table_data(dolphie: Dolphie):
    replica_tables = {}
    for row in dolphie.replica_data:
        thread_id = row["id"]

        # Resolve IPs to addresses and add to cache for fast lookup
        host = dolphie.get_hostname(row["host"].split(":")[0])

        try:
            if thread_id not in dolphie.replica_connections:
                dolphie.replica_connections[thread_id] = {
                    "host": host,
                    "connection": pymysql.connect(
                        host=host,
                        user=dolphie.user,
                        passwd=dolphie.password,
                        port=dolphie.port,
                        ssl=dolphie.ssl,
                        autocommit=True,
                    ),
                    "cursor": None,
                    "previous_sbm": 0,
                }

            replica_connection = dolphie.replica_connections[thread_id]
            replica_connection["cursor"] = replica_connection["connection"].cursor(pymysql.cursors.DictCursor)
            replica_connection["cursor"].execute(MySQLQueries.replication_status)
            replica_data = replica_connection["cursor"].fetchone()

            if replica_data:
                replica_tables[host] = create_table(dolphie, data=replica_data, replica_thread_id=thread_id)
        except pymysql.Error as e:
            table = Table(box=box.ROUNDED, show_header=False, style="#b0bad7")

            table.add_column()
            table.add_column()

            table.add_row("[#c5c7d2]Host", host)
            table.add_row("[#c5c7d2]User", row["user"])
            table.add_row("[#c5c7d2]Error", "[#fc7979]%s[#fc7979]" % e.args[1])

            replica_tables[host] = table

    return replica_tables
