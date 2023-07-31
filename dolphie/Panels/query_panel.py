import re
from datetime import timedelta

from dolphie import Dolphie
from dolphie.Functions import format_number
from dolphie.Queries import Queries
from textual.widgets import DataTable


def create_panel(dolphie: Dolphie) -> DataTable:
    columns = [
        {"name": "Thread ID", "field": "id", "width": 11, "format_number": False},
        {"name": "Username", "field": "user", "width": 13, "format_number": False},
    ]

    if dolphie.show_additional_query_columns:
        columns.extend(
            [
                {"name": "Hostname/IP", "field": "host", "width": 16, "format_number": False},
                {"name": "Database", "field": "db", "width": 13, "format_number": False},
            ]
        )

    columns.extend(
        [
            {"name": "Command", "field": "command", "width": 8, "format_number": False},
            {"name": "State", "field": "state", "width": 16, "format_number": False},
            {"name": "TRX State", "field": "trx_state", "width": 9, "format_number": False},
            {"name": "Rows Lock", "field": "trx_rows_locked", "width": 9, "format_number": True},
            {"name": "Rows Mod", "field": "trx_rows_modified", "width": 8, "format_number": True},
        ]
    )

    if (
        dolphie.show_additional_query_columns
        and "innodb_thread_concurrency" in dolphie.variables
        and dolphie.variables["innodb_thread_concurrency"]
    ):
        columns.append({"name": "Tickets", "field": "trx_concurrency_tickets", "width": 8, "format_number": True})

    columns.extend(
        [
            {"name": "Time", "field": "formatted_time", "width": 9, "format_number": False},
            {"name": "Query", "field": "query", "width": None, "format_number": False},
        ]
    )

    processlist_datatable = dolphie.app.query_one("#processlist_panel")
    if len(processlist_datatable.columns) != len(columns):
        processlist_datatable.clear(columns=True)
    else:
        processlist_datatable.clear()

    if not processlist_datatable.columns:
        for column_data in columns:
            column_name = column_data["name"]
            column_key = column_data["field"]
            column_width = column_data["width"]
            processlist_datatable.add_column(column_name, key=column_key, width=column_width)

    for id, thread in dolphie.processlist_threads.items():
        if thread["command"] == "Killed":
            thread["command"] = "[#fc7979]%s" % thread["command"]
        else:
            thread["command"] = thread["command"]

        # Add rows for each thread or update existing ones
        row_values = []
        for column_data in columns:
            column_name = column_data["field"]
            column_format_number = column_data["format_number"]

            if column_name == "query":
                value = re.sub(r"\s+", " ", thread[column_name])
            else:
                value = format_number(thread[column_name]) if column_format_number else thread[column_name]

            row_values.append(value)

        processlist_datatable.add_row(*row_values)


def fetch_data(dolphie: Dolphie):
    if dolphie.use_performance_schema:
        processlist_query = Queries["ps_query"]
    else:
        processlist_query = Queries["pl_query"]

    if dolphie.sort_by_time_descending:
        if dolphie.use_performance_schema:
            processlist_query = processlist_query + " ORDER BY processlist_time DESC"
        else:
            processlist_query = processlist_query + " ORDER BY LENGTH(Time) DESC, Time DESC"
    else:
        if dolphie.use_performance_schema:
            processlist_query = processlist_query + " ORDER BY processlist_time"
        else:
            processlist_query = processlist_query + " ORDER BY LENGTH(Time), Time"

    ########################
    # WHERE clause filters #
    ########################
    where_clause = []

    # Only show running queries
    if not dolphie.show_idle_queries:
        if dolphie.use_performance_schema:
            where_clause.append(
                "(processlist_command != 'Sleep' AND processlist_command NOT LIKE 'Binlog Dump%') AND "
                "(processlist_info IS NOT NULL OR trx_query IS NOT NULL)"
            )
        else:
            where_clause.append(
                "(Command != 'Sleep' AND Command NOT LIKE 'Binlog Dump%') AND "
                "(Info IS NOT NULL OR trx_query IS NOT NULL)"
            )

    # Only show running transactions only
    if dolphie.show_trxs_only:
        where_clause.append("trx_state != ''")

    # Filter user
    if dolphie.user_filter:
        if dolphie.use_performance_schema:
            where_clause.append("processlist_user = '%s'" % dolphie.user_filter)
        else:
            where_clause.append("User = '%s'" % dolphie.user_filter)

    # Filter database
    if dolphie.db_filter:
        if dolphie.use_performance_schema:
            where_clause.append("processlist_db = '%s'" % dolphie.db_filter)
        else:
            where_clause.append("db = '%s'" % dolphie.db_filter)

    # Filter hostname/IP
    if dolphie.host_filter:
        # Have to use LIKE since there's a port at the end
        if dolphie.use_performance_schema:
            where_clause.append("processlist_host LIKE '%s%%'" % dolphie.host_filter)
        else:
            where_clause.append("Host LIKE '%s%%'" % dolphie.host_filter)

    # Filter time
    if dolphie.query_time_filter:
        if dolphie.use_performance_schema:
            where_clause.append("processlist_time >= '%s'" % dolphie.query_time_filter)
        else:
            where_clause.append("Time >= '%s'" % dolphie.query_time_filter)

    # Filter query
    if dolphie.query_filter:
        if dolphie.use_performance_schema:
            where_clause.append(
                "(processlist_info LIKE '%%%s%%' OR trx_query LIKE '%%%s%%')"
                % (dolphie.query_filter, dolphie.query_filter),
            )
        else:
            where_clause.append("Info LIKE '%%%s%%'" % dolphie.query_filter)

    # Add in our dynamic WHERE clause for filtering
    if where_clause:
        processlist_query = processlist_query.replace("$placeholder", "AND " + " AND ".join(where_clause))
    else:
        processlist_query = processlist_query.replace("$placeholder", "")

    processlist_threads = {}
    # Run the processlist query
    dolphie.main_db_connection.execute(processlist_query)
    threads = dolphie.main_db_connection.fetchall()

    for thread in threads:
        # Don't include Dolphie's thread
        if dolphie.connection_id == thread["id"]:
            continue

        command = thread["command"]
        if dolphie.use_performance_schema and dolphie.show_last_executed_query is False and command == "Sleep":
            query = ""
        else:
            # Use trx_query over Performance Schema query since it's more accurate
            if dolphie.use_performance_schema and thread["trx_query"]:
                query = thread["trx_query"]
            else:
                query = thread["query"]

        # Determine time color
        time = int(thread["time"])
        thread_color = ""
        if "SELECT /*!40001 SQL_NO_CACHE */ *" in query:
            thread_color = "magenta"
        elif query and command != "Sleep" and "Binlog Dump" not in command:
            if time >= 5:
                thread_color = "[#fc7979]"
            elif time >= 3:
                thread_color = "[#f1fb82]"
            elif time <= 2:
                thread_color = "[#54efae]"

        hours = time // 3600
        minutes = (time % 3600) // 60
        seconds = time % 60
        formatted_time = "{}{:02}:{:02}:{:02}".format(thread_color, hours, minutes, seconds)

        # If after the first loop there's nothing in cache, don't try to resolve anymore.
        # This is an optimization
        host = thread["host"].split(":")[0]
        if dolphie.first_loop is False:
            if dolphie.host_cache:
                host = dolphie.get_hostname(host)
        else:
            host = dolphie.get_hostname(host)

        processlist_threads[str(thread["id"])] = {
            "id": str(thread["id"]),
            "user": thread["user"],
            "host": host,
            "db": thread["db"],
            "formatted_time": formatted_time,
            "time": time,
            "hhmmss_time": "{}{:0>8}".format(thread_color, str(timedelta(seconds=time))),
            "command": command,
            "state": thread["state"],
            "trx_state": thread["trx_state"],
            "trx_operation_state": thread["trx_operation_state"],
            "trx_rows_locked": thread["trx_rows_locked"],
            "trx_rows_modified": thread["trx_rows_modified"],
            "trx_concurrency_tickets": thread["trx_concurrency_tickets"],
            "query": query,
        }

    return processlist_threads
