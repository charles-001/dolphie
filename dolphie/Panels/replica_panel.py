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


def create_panel(dolphie: Dolphie):
    table_box = box.ROUNDED
    table_line_color = "#b0bad7"

    table_grid = Table.grid(pad_edge=True)
    table_replication = Table()

    # Only run this if dashboard isn't turned on
    dashboard = dolphie.app.query_one("#dashboard_panel")
    if not dashboard.display:
        dolphie.replica_status = dolphie.fetch_data("replica_status")

    if dolphie.replica_status:
        table_replication = create_table(dolphie, dolphie.replica_status)

    if dolphie.use_performance_schema:
        find_replicas_query = Queries["ps_find_replicas"]
    else:
        find_replicas_query = Queries["pl_find_replicas"]

    dolphie.db.cursor.execute(find_replicas_query)
    data = dolphie.db.fetchall()

    tables = {}
    for row in data:
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
            replica_connection["cursor"].execute(Queries["replica_status"])
            replica_data = replica_connection["cursor"].fetchone()

            if replica_data:
                tables[host] = create_table(dolphie, replica_data, list_replica_thread_id=thread_id)

        except pymysql.Error as e:
            table = Table(table_box, show_header=False, style=table_line_color)

            table.add_column()
            table.add_column(width=30)

            table.add_row("[#c5c7d2]Host", host)
            table.add_row("[#c5c7d2]User", row["user"])
            table.add_row("[#fc7979]Error", e.args[1])

            tables[host] = table

    # Stack tables in groups of 3
    tables = sorted(tables.items())
    num_tables = len(tables)
    for i in range(0, num_tables - (num_tables % 3), 3):
        table_grid.add_row(*[table for _, table in tables[i : i + 3]])

    if num_tables % 3 != 0:
        table_grid.add_row(*[table for _, table in tables[num_tables - (num_tables % 3) :]])

    if dolphie.replica_status:
        # GTID Sets can be very long, so we don't center align replication table or else table
        # will increase/decrease in size a lot
        if ("Executed_Gtid_Set" in dolphie.replica_status and dolphie.replica_status["Executed_Gtid_Set"]) or (
            "Using_Gtid" in dolphie.replica_status and dolphie.replica_status["Using_Gtid"] != "No"
        ):
            panel_data = Group(
                Align.left(table_replication),
                Align.center(table_grid),
            )
        else:
            panel_data = Group(
                Align.center(table_replication),
                Align.center(table_grid),
            )
    else:
        panel_data = Align.center(table_grid)

    return panel_data


def create_table(dolphie: Dolphie, data, dashboard_table=False, list_replica_thread_id=None):
    table_title_style = Style(bold=True)
    table_box = box.ROUNDED
    table_line_color = "#b0bad7"

    # This is for the replica view
    if list_replica_thread_id:
        if dolphie.replica_connections[list_replica_thread_id]["previous_sbm"] is not None:
            replica_previous_replica_sbm = dolphie.replica_connections[list_replica_thread_id]["previous_sbm"]

        db_cursor = dolphie.replica_connections[list_replica_thread_id]["cursor"]
    else:
        replica_previous_replica_sbm = dolphie.previous_replica_sbm
        db_cursor = dolphie.db.cursor

    if data["Slave_IO_Running"].lower() == "no":
        data["Slave_IO_Running"] = "[#fc7979]NO[/#fc7979]"
    else:
        data["Slave_IO_Running"] = "[#54efae]Yes[/#54efae]"

    if data["Slave_SQL_Running"].lower() == "no":
        data["Slave_SQL_Running"] = "[#fc7979]NO[/#fc7979]"
    else:
        data["Slave_SQL_Running"] = "[#54efae]Yes[/#54efae]"

    data["sbm_source"] = "Replica"
    # Use performance schema for seconds behind if host is MySQL 8
    if dolphie.mysql_version.startswith("8") and dolphie.performance_schema_enabled:
        db_cursor.execute(Queries["ps_replica_lag"])
        replica_lag_data = db_cursor.fetchone()

        data["sbm_source"] = "PS"
        if replica_lag_data["secs_behind"]:
            data["Seconds_Behind_Master"] = int(replica_lag_data["secs_behind"])
        else:
            data["Seconds_Behind_Master"] = 0
    # Use heartbeat table from pt-toolkit if specified
    elif dolphie.heartbeat_table:
        try:
            db_cursor.execute(Queries["heartbeat_replica_lag"])
            replica_lag_data = db_cursor.fetchone()

            if replica_lag_data["secs_behind"] is not None:
                data["sbm_source"] = "HB"
                data["Seconds_Behind_Master"] = int(replica_lag_data["secs_behind"])
        except pymysql.Error:
            pass

    data["Speed"] = 0
    # Colorize seconds behind
    if data["Seconds_Behind_Master"] is not None:
        replica_sbm = data["Seconds_Behind_Master"]

        if list_replica_thread_id:
            dolphie.replica_connections[list_replica_thread_id]["previous_sbm"] = replica_sbm

        if replica_previous_replica_sbm and replica_sbm < replica_previous_replica_sbm:
            data["Speed"] = round((replica_previous_replica_sbm - replica_sbm) / dolphie.refresh_interval)

        if replica_sbm != 0:
            if replica_sbm > 20:
                data["Lag"] = "[#fc7979]%s" % "{:0>8}".format(str(timedelta(seconds=replica_sbm)))
            elif replica_sbm > 10:
                data["Lag"] = "[#f1fb82w]%s" % "{:0>8}".format(str(timedelta(seconds=replica_sbm)))
            else:
                data["Lag"] = "[#54efae]%s" % "{:0>8}".format(str(timedelta(seconds=replica_sbm)))
        elif replica_sbm == 0:
            data["Lag"] = "[#54efae]00:00:00"

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

    table_title = ""
    if dashboard_table is True or list_replica_thread_id is None:
        table_title = "Replication"

    table = Table(
        show_header=False,
        box=table_box,
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
        table.add_row("[#c5c7d2]Host", "%s" % dolphie.replica_connections[list_replica_thread_id]["host"])
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
        table.add_row(
            "[#c5c7d2]%s Lag" % data["sbm_source"],
            "%s [#c5c7d2]Speed %s" % (data["Lag"], data["Speed"]),
        )
    if dashboard_table:
        table.add_row("[#c5c7d2]Binlog IO", "%s" % (data["Master_Log_File"]))
        table.add_row("[#c5c7d2]Binlog SQL", "%s" % (data["Relay_Master_Log_File"]))
        table.add_row("[#c5c7d2]Relay Log ", "%s" % (data["Relay_Log_File"]))
        table.add_row("[#c5c7d2]GTID", "%s" % data["gtid"])
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

        table.add_row("[#c5c7d2]GTID", "%s" % data["gtid"])

        if data["mysql_gtid_enabled"]:
            executed_gtid_set = data["Executed_Gtid_Set"]
            retrieved_gtid_set = data["Retrieved_Gtid_Set"]

            # Compile the regular expression patterns outside the loops for efficiency
            pattern = re.compile(r"\b(\w+-\w+-\w+-\w+-)(\w+:\d+-\d+)\b")

            def replace_gtid(match):
                gtid_prefix = match.group(1)
                last_part = match.group(2).split(":")[0]  # Get the last part before the colon
                gtid_suffix = ":" + match.group(2).split(":")[1]  # Get the part after the colon

                if gtid_prefix + last_part == dolphie.server_uuid:
                    return f"… [#54efae]{last_part}[/#54efae]{gtid_suffix}"
                else:
                    return f"… [#91abec]{last_part}[/#91abec]{gtid_suffix}"

            # Process retrieved_gtid_set
            retrieved_gtid_set = pattern.sub(replace_gtid, retrieved_gtid_set)

            # Process executed_gtid_set
            executed_gtid_set = pattern.sub(replace_gtid, executed_gtid_set)

            table.add_row("[#c5c7d2]Auto Position", "%s" % data["Auto_Position"])
            table.add_row("[#c5c7d2]Retrieved GTID Set", "%s" % retrieved_gtid_set)
            table.add_row("[#c5c7d2]Executed GTID Set", "%s" % executed_gtid_set)
        elif data["mariadb_gtid_enabled"]:
            table.add_row("[#c5c7d2]GTID IO Pos ", "%s" % data["Gtid_IO_Pos"])

        if data["Last_Error"]:
            table.add_row("[#c5c7d2]Error ", "%s" % data["Last_Error"])
        elif data["Last_IO_Error"]:
            table.add_row("[#c5c7d2]Error ", "%s" % data["Last_IO_Error"])
        elif data["Last_SQL_Error"]:
            table.add_row("[#c5c7d2]Error ", "%s" % data["Last_SQL_Error"])

    return table
