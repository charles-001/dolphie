import re
import string
from datetime import timedelta

from dolphie import Dolphie
from dolphie.Functions import detect_encoding, format_number
from dolphie.Queries import Queries
from rich import box
from rich.table import Table


def create_panel(dolphie: Dolphie):
    table = Table(header_style="bold white", box=box.SIMPLE_HEAVY, style="steel_blue1")

    columns = {}
    columns["Thread ID"] = {"field": "id", "width": 11, "format_number": False}
    columns["Username"] = {"field": "user", "width": 13, "format_number": False}

    if dolphie.show_additional_query_columns:
        columns["Hostname/IP"] = {"field": "host", "width": 16, "format_number": False}
        columns["Database"] = {"field": "db", "width": 13, "format_number": False}

    columns["Command"] = {"field": "command", "width": 8, "format_number": False}
    columns["State"] = {"field": "state", "width": 16, "format_number": False}
    columns["TRX State"] = {"field": "trx_state", "width": 9, "format_number": False}
    columns["Rows Lock"] = {"field": "trx_rows_locked", "width": 9, "format_number": True}
    columns["Rows Mod"] = {"field": "trx_rows_modified", "width": 8, "format_number": True}

    if (
        dolphie.show_additional_query_columns
        and "innodb_thread_concurrency" in dolphie.variables
        and dolphie.variables["innodb_thread_concurrency"]
    ):
        columns["Tickets"] = {"field": "trx_concurrency_tickets", "width": 8, "format_number": True}

    columns["Time"] = {"field": "formatted_time", "width": 9, "format_number": False}
    columns["Query"] = {"field": "shortened_query", "width": None, "format_number": False}

    total_width = 0
    for column, data in columns.items():
        if column == "Query":
            overflow = "crop"
        else:
            overflow = "ellipsis"

        if data["width"]:
            table.add_column(column, width=data["width"], no_wrap=True, overflow=overflow)
            total_width += data["width"]
        else:
            table.add_column(column, no_wrap=True, overflow=overflow)

    # This variable is to cut off query so it's the perfect width for the auto-sized column that matches terminal width
    query_characters = dolphie.console.size.width - total_width - ((len(columns) * 3) + 1)

    thread_counter = 0
    for id, thread in dolphie.processlist_threads.items():
        query = thread["query"]

        # Replace strings with [NONPRINTABLE] if they contain non-printable characters
        for m in re.findall(r"(\"(?:(?!(?<!\\)\").)*\"|'(?:(?!(?<!\\)').)*')", query):
            test_pattern = re.search(f"[^{re.escape(string.printable)}]", m)
            if test_pattern:
                query = query.replace(m, "[NONPRINTABLE]")

        # If no query, pad the query column with spaces so it's sized correctly
        if not query:
            query = query.ljust(query_characters)

        # Pad queries with spaces that are not the full size of query column
        elif len(query) < query_characters:
            query = query.ljust(dolphie.console.size.width)

        # Change values to what we want
        if thread["command"] == "Killed":
            thread["command"] = "[bright_red]%s" % thread["command"]
        else:
            thread["command"] = thread["command"]

        thread["shortened_query"] = query[0:query_characters]

        # Add rows for each thread
        row_values = []
        for column, data in columns.items():
            if data["format_number"]:
                row_values.append(format_number(thread[data["field"]]))
            else:
                row_values.append(thread[data["field"]])

        table.add_row(*row_values, style="grey93")

        thread_counter += 1

    # Add an invisible row to keep query column sized correctly
    if thread_counter == 0:
        empty_values = []
        for column, data in columns.items():
            if data["width"]:
                empty_values.append("")
            else:
                empty_values.append("".ljust(query_characters))

        table.add_row(*empty_values)

    return table


def get_data(dolphie: Dolphie):
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
    if dolphie.time_filter:
        if dolphie.use_performance_schema:
            where_clause.append("processlist_time >= '%s'" % dolphie.time_filter)
        else:
            where_clause.append("Time >= '%s'" % dolphie.time_filter)

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

    # Limit the SELECT query to only retrieve how many lines the terminal has
    processlist_query = processlist_query + " LIMIT %s" % (dolphie.console.size.height)

    processlist_threads = {}
    # Run the processlist query
    dolphie.db.execute(processlist_query)
    threads = dolphie.db.cursor.fetchall()

    for thread in threads:
        # Don't include Dolphie's thread
        if dolphie.connection_id == thread["id"]:
            continue

        command = thread["command"].decode()
        if dolphie.use_performance_schema and dolphie.show_last_executed_query is False and command == "Sleep":
            query = ""
        else:
            # Use trx_query over Performance Schema query since it's more accurate
            if dolphie.use_performance_schema and thread["trx_query"]:
                query = thread["trx_query"].decode(detect_encoding(thread["trx_query"]))
            else:
                query = thread["query"].decode(detect_encoding(thread["query"]))

        # Determine time color
        time = int(thread["time"])
        thread_color = "grey93"
        if "SELECT /*!40001 SQL_NO_CACHE */ *" in query:
            thread_color = "magenta"
        elif query and command != "Sleep" and "Binlog Dump" not in command:
            if time >= 4:
                thread_color = "bright_red"
            elif time >= 2:
                thread_color = "bright_yellow"
            elif time <= 1:
                thread_color = "bright_green"

        formatted_time = "[%s]%ss" % (thread_color, time)

        # If after the first loop there's nothing in cache, don't try to resolve anymore.
        # This is an optimization
        host = thread["host"].decode().split(":")[0]
        if dolphie.first_loop is False:
            if dolphie.host_cache:
                host = dolphie.get_hostname(host)
        else:
            host = dolphie.get_hostname(host)

        processlist_threads[str(thread["id"])] = {
            "id": str(thread["id"]),
            "user": thread["user"].decode(),
            "host": host,
            "db": thread["db"].decode(),
            "formatted_time": formatted_time,
            "time": time,
            "hhmmss_time": "[{}]{:0>8}".format(thread_color, str(timedelta(seconds=time))),
            "command": command,
            "state": thread["state"].decode(),
            "trx_state": thread["trx_state"].decode(),
            "trx_operation_state": thread["trx_operation_state"].decode(),
            "trx_rows_locked": thread["trx_rows_locked"].decode(),
            "trx_rows_modified": thread["trx_rows_modified"].decode(),
            "trx_concurrency_tickets": thread["trx_concurrency_tickets"].decode(),
            "query": re.sub(r"\s+", " ", query),
        }

    return processlist_threads
