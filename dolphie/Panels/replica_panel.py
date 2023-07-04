import re
from datetime import timedelta

import pymysql
from dolphie import Dolphie
from dolphie.Queries import Queries
from rich import box
from rich.align import Align
from rich.console import Group
from rich.style import Style
from rich.table import Table
from rich.text import Text


def create_panel(dolphie: Dolphie):
    table_grid = Table.grid()
    table_replication = Table()

    # Only run this if dashboard isn't turned on
    if dolphie.dashboard is not True:
        dolphie.replica_status = dolphie.fetch_data("replica_status")

    if dolphie.replica_status:
        table_replication = create_table(dolphie, dolphie.replica_status)

    find_replicas_query = 0
    if dolphie.use_performance_schema:
        find_replicas_query = Queries["ps_find_replicas"]
    else:
        find_replicas_query = Queries["pl_find_replicas"]

    replica_count = dolphie.db.cursor.execute(find_replicas_query)
    data = dolphie.db.cursor.fetchall()

    replica_count_text = Text.from_markup("\n[b steel_blue1]%s[/b steel_blue1] replicas" % (replica_count))

    table_split_counter = 1
    tables = []
    for row in data:
        thread_id = row["id"]

        # Resolve IPs to addresses and add to cache for fast lookup
        host = dolphie.get_hostname(row["host"].decode().split(":")[0])

        table = Table(box=box.ROUNDED, show_header=False, style="grey70")
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
                    "previous_sbm": 0,
                }

            replica_connection = dolphie.replica_connections[thread_id]
            replica_cursor = replica_connection["connection"].cursor(pymysql.cursors.DictCursor)
            replica_cursor.execute(Queries["replica_status"])
            replica_data = replica_cursor.fetchone()

            if replica_data:
                tables.append(create_table(dolphie, replica_data, list_replica_thread_id=thread_id))
        except pymysql.Error as e:
            row_style = Style(color="grey93")

            table.add_column()
            table.add_column(width=30)

            table.add_row("[grey78]Host", host, style=row_style)
            table.add_row("[grey78]User", row["user"].decode(), style=row_style)
            table.add_row("[bright_red]Error", e.args[1], style=row_style)

            tables.append(table)

        if table_split_counter == 3:
            table_grid.add_row(*tables)
            table_split_counter = 0
            tables = []

        table_split_counter += 1

    if table_split_counter:
        table_grid.add_row(*tables)

    if dolphie.replica_status:
        # GTID Sets can be very long, so we don't center align replication table or else table
        # will increase/decrease in size a lot
        if ("Executed_Gtid_Set" in dolphie.replica_status and dolphie.replica_status["Executed_Gtid_Set"]) or (
            "Using_Gtid" in dolphie.replica_status and dolphie.replica_status["Using_Gtid"] != "No"
        ):
            panel_data = Group(
                Align.left(table_replication),
                Align.center(replica_count_text),
                Align.center(table_grid),
            )
        else:
            panel_data = Group(
                Align.center(table_replication),
                Align.center(replica_count_text),
                Align.center(table_grid),
            )
    else:
        panel_data = Group(Align.center(replica_count_text), Align.center(table_grid))

    return panel_data


def create_table(dolphie: Dolphie, data, dashboard_table=False, list_replica_thread_id=None):
    # This is for the replica view
    if list_replica_thread_id:
        if dolphie.replica_connections[list_replica_thread_id]["previous_sbm"] is not None:
            replica_previous_replica_sbm = dolphie.replica_connections[list_replica_thread_id]["previous_sbm"]
    else:
        replica_previous_replica_sbm = dolphie.previous_replica_sbm

    if data["Slave_IO_Running"].lower() == "no":
        data["Slave_IO_Running"] = "[bright_red]NO[/bright_red]"
    else:
        data["Slave_IO_Running"] = "[bright_green]Yes[/bright_green]"

    if data["Slave_SQL_Running"].lower() == "no":
        data["Slave_SQL_Running"] = "[bright_red]NO[/bright_red]"
    else:
        data["Slave_SQL_Running"] = "[bright_green]Yes[/bright_green]"

    data["sbm_source"] = "Replica"
    # Use performance schema for seconds behind if host is MySQL 8
    if dolphie.full_version.startswith("8") and dolphie.performance_schema_enabled:
        dolphie.db.cursor.execute(Queries["ps_replica_lag"])
        replica_lag_data = dolphie.db.cursor.fetchone()

        data["sbm_source"] = "PS"
        if replica_lag_data["secs_behind"]:
            data["Seconds_Behind_Master"] = int(replica_lag_data["secs_behind"])
        else:
            data["Seconds_Behind_Master"] = 0
    # Use heartbeat table from pt-toolkit if specified
    elif dolphie.heartbeat_table:
        try:
            dolphie.db.cursor.execute(Queries["heartbeat_replica_lag"])
            replica_lag_data = dolphie.db.cursor.fetchone()

            if replica_lag_data["secs_behind"] is not None:
                data["sbm_source"] = "HB"
                data["Seconds_Behind_Master"] = int(replica_lag_data["secs_behind"])
        except pymysql.Error:
            pass

    data["speed"] = 0
    # Colorize seconds behind
    if data["Seconds_Behind_Master"] is not None:
        replica_sbm = data["Seconds_Behind_Master"]

        if list_replica_thread_id:
            dolphie.replica_connections[list_replica_thread_id]["previous_sbm"] = replica_sbm

        if replica_previous_replica_sbm and replica_sbm < replica_previous_replica_sbm:
            data["speed"] = round((replica_previous_replica_sbm - replica_sbm) / dolphie.refresh_interval)

        if replica_sbm != 0:
            if replica_sbm > 20:
                data["lag"] = "[bright_red]%s" % "{:0>8}".format(str(timedelta(seconds=replica_sbm)))
            elif replica_sbm > 10:
                data["lag"] = "[bright_yellow]%s" % "{:0>8}".format(str(timedelta(seconds=replica_sbm)))
            else:
                data["lag"] = "[bright_green]%s" % "{:0>8}".format(str(timedelta(seconds=replica_sbm)))
        elif replica_sbm == 0:
            data["lag"] = "[bright_green]00:00:00"

    data["Master_Host"] = dolphie.get_hostname(data["Master_Host"])
    data["mysql_gtid_enabled"] = False
    data["mariadb_gtid_enabled"] = False
    data["gtid"] = "OFF"
    if "Executed_Gtid_Set" in data and data["Executed_Gtid_Set"]:
        data["mysql_gtid_enabled"] = True
        data["gtid"] = "ON"
    if "Using_Gtid" in data and data["Using_Gtid"] != "No":
        data["mariadb_gtid_enabled"] = True
        data["gtid"] = data["Using_Gtid"]

    # Create table for Rich
    table_title_style = Style(color="grey93", bold=True)
    table_line_color = "grey78"
    row_style = Style(color="grey93")

    table_title = ""
    if dashboard_table is True or list_replica_thread_id is None:
        table_title = "Replication"

    table = Table(
        show_header=False,
        box=box.ROUNDED,
        title=table_title,
        title_style=table_title_style,
        style=table_line_color,
    )

    table.add_column()
    if dashboard_table is True:
        table.add_column(max_width=21)
    elif list_replica_thread_id is not None:
        if data["mysql_gtid_enabled"] or data["mariadb_gtid_enabled"]:
            table.add_column(max_width=60)
    else:
        table.add_column(overflow="fold")

    if list_replica_thread_id is not None:
        table.add_row(
            "[grey78]Host", "[grey93]%s" % dolphie.replica_connections[list_replica_thread_id]["host"], style=row_style
        )
    else:
        table.add_row("[grey78]Primary", "[grey93]%s" % data["Master_Host"], style=row_style)

    table.add_row("[grey78]User", "[grey93]%s" % data["Master_User"], style=row_style)
    table.add_row(
        "[grey78]Thread",
        "[grey78]IO [grey93]%s [grey78]SQL [grey93]%s" % (data["Slave_IO_Running"], data["Slave_SQL_Running"]),
        style=row_style,
    )
    if data["Seconds_Behind_Master"] is None:
        table.add_row("[grey78]Lag", "", style=row_style)
    else:
        table.add_row(
            "[grey78]%s Lag" % data["sbm_source"],
            "[grey93]%s [grey78]Speed [grey93]%s" % (data["lag"], data["speed"]),
            style=row_style,
        )
    if dashboard_table:
        table.add_row("[grey78]Binlog IO", "[grey93]%s" % (data["Master_Log_File"]), style=row_style)
        table.add_row("[grey78]Binlog SQL", "[grey93]%s" % (data["Relay_Master_Log_File"]), style=row_style)
        table.add_row("[grey78]Relay Log ", "[grey93]%s" % (data["Relay_Log_File"]), style=row_style)
        table.add_row("[grey78]GTID", "[grey93]%s" % data["gtid"], style=row_style)
    else:
        table.add_row(
            "[grey78]Binlog IO",
            "%s ([grey62]%s[/grey62])" % (data["Master_Log_File"], data["Read_Master_Log_Pos"]),
            style=row_style,
        )
        table.add_row(
            "[grey78]Binlog SQL",
            "%s ([grey62]%s[/grey62])"
            % (
                data["Relay_Master_Log_File"],
                data["Exec_Master_Log_Pos"],
            ),
            style=row_style,
        )
        table.add_row(
            "[grey78]Relay Log",
            "%s ([grey62]%s[/grey62])" % (data["Relay_Log_File"], data["Relay_Log_Pos"]),
            style=row_style,
        )

        table.add_row("[grey78]GTID", "[grey93]%s" % data["gtid"], style=row_style)
        if data["mysql_gtid_enabled"]:
            executed_gtid_set = data["Executed_Gtid_Set"]
            retrieved_gtid_set = data["Retrieved_Gtid_Set"]

            for m in re.findall(r"(\w+-\w+-\w+-\w+-)(\w+)", retrieved_gtid_set):
                source_id = m[0] + m[1]
                if source_id == dolphie.server_uuid:
                    retrieved_gtid_set = retrieved_gtid_set.replace(m[0], "[grey62]…[medium_spring_green]")
                else:
                    retrieved_gtid_set = retrieved_gtid_set.replace(m[0], "[grey62]…[steel_blue1]")

                retrieved_gtid_set = retrieved_gtid_set.replace(m[1], "%s[grey93]" % m[1])

            for m in re.findall(r"(\w+-\w+-\w+-\w+-)(\w+)", executed_gtid_set):
                source_id = m[0] + m[1]
                if source_id == dolphie.server_uuid:
                    executed_gtid_set = executed_gtid_set.replace(m[0], "[grey62]…[medium_spring_green]")
                else:
                    executed_gtid_set = executed_gtid_set.replace(m[0], "[grey62]…[steel_blue1]")

                executed_gtid_set = executed_gtid_set.replace(m[1], "%s[grey93]" % m[1])

            table.add_row("[grey78]Auto Position", "[grey93]%s" % data["Auto_Position"], style=row_style)
            table.add_row("[grey78]Retrieved GTID Set", "[grey93]%s" % retrieved_gtid_set, style=row_style)
            table.add_row("[grey78]Executed GTID Set", "[grey93]%s" % executed_gtid_set, style=row_style)
        elif data["mariadb_gtid_enabled"]:
            table.add_row("[grey78]GTID IO Pos ", "[grey93]%s" % data["Gtid_IO_Pos"], style=row_style)

        if data["Last_Error"]:
            table.add_row("[grey78]Error ", "[grey93]%s" % data["Last_Error"])
        elif data["Last_IO_Error"]:
            table.add_row("[grey78]Error ", "[grey93]%s" % data["Last_IO_Error"])
        elif data["Last_SQL_Error"]:
            table.add_row("[grey78]Error ", "[grey93]%s" % data["Last_SQL_Error"])

    return table
