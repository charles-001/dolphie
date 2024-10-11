from typing import Dict

from rich.syntax import Syntax
from textual.widgets import DataTable

from dolphie.DataTypes import ProcesslistThread
from dolphie.Modules.Functions import format_number, format_query
from dolphie.Modules.Queries import MySQLQueries
from dolphie.Modules.TabManager import Tab


def create_panel(tab: Tab) -> DataTable:
    dolphie = tab.dolphie

    if not dolphie.performance_schema_enabled and dolphie.use_performance_schema and not dolphie.replay_file:
        dolphie.app.notify(
            "Performance Schema is not enabled on this host, using Information Schema instead for Processlist"
        )
        dolphie.use_performance_schema = False

    columns = [
        {"name": "Thread ID", "field": "id", "width": None, "format_number": False},
    ]

    if dolphie.use_performance_schema:
        columns.extend([{"name": "Protocol", "field": "protocol", "width": 8, "format_number": False}])

    columns.extend(
        [
            {"name": "Username", "field": "user", "width": 20, "format_number": False},
        ]
    )

    if dolphie.show_additional_query_columns:
        columns.extend(
            [
                {"name": "Hostname/IP", "field": "host", "width": 25, "format_number": False},
                {"name": "Database", "field": "db", "width": 15, "format_number": False},
            ]
        )

    columns.extend(
        [
            {"name": "Command", "field": "command", "width": 8, "format_number": False},
            {"name": "State", "field": "state", "width": 20, "format_number": False},
            {"name": "TRX State", "field": "trx_state", "width": 9, "format_number": False},
            {"name": "R-Lock", "field": "trx_rows_locked", "width": 7, "format_number": True},
            {"name": "R-Mod", "field": "trx_rows_modified", "width": 7, "format_number": True},
        ]
    )

    if (
        dolphie.show_additional_query_columns and dolphie.global_variables.get("innodb_thread_concurrency")
    ) or dolphie.show_threads_with_concurrency_tickets:
        columns.append({"name": "Tickets", "field": "trx_concurrency_tickets", "width": 8, "format_number": False})

    if dolphie.show_trxs_only:
        columns.append(
            {"name": "TRX Age", "field": "trx_time", "width": 9, "format_number": False},
        )

    columns.extend(
        [
            {"name": "Age", "field": "formatted_time", "width": 9, "format_number": False},
            {"name": "Query", "field": "formatted_query", "width": None, "format_number": False},
            {"name": "time_seconds", "field": "time", "width": 0, "format_number": False},
        ]
    )

    # Refresh optimization
    query_length_max = 300
    processlist_datatable = tab.processlist_datatable

    # Clear table if columns change
    if len(processlist_datatable.columns) != len(columns):
        processlist_datatable.clear(columns=True)

    # Add columns to the datatable if it is empty
    if not processlist_datatable.columns:
        for column_data in columns:
            column_name = column_data["name"]
            column_width = column_data["width"]
            processlist_datatable.add_column(column_name, key=column_name, width=column_width)

    filter_threads = []
    # Iterate through processlist_threads
    for thread_id, thread in dolphie.processlist_threads.items():
        row_values = []

        thread: ProcesslistThread
        # We use filter here for replays since the original way requires changing WHERE clause
        if dolphie.replay_file:
            found = False
            if dolphie.show_trxs_only and thread.trx_state == "[dark_gray]N/A":
                found = True
            elif dolphie.user_filter and dolphie.user_filter != thread.user:
                found = True
            elif dolphie.db_filter and dolphie.db_filter != thread.db:
                found = True
            elif dolphie.host_filter and dolphie.host_filter not in thread.host:
                found = True
            elif dolphie.query_time_filter and int(dolphie.query_time_filter) >= thread.time:
                found = True
            elif dolphie.query_filter and dolphie.query_filter not in thread.formatted_query.code:
                found = True
            elif dolphie.show_threads_with_concurrency_tickets and thread.trx_concurrency_tickets == "[dark_gray]0":
                found = True

            if found:
                filter_threads.append(thread_id)
                continue

        for column_id, (column_data) in enumerate(columns):
            column_name = column_data["name"]
            column_field = column_data["field"]
            column_format_number = column_data["format_number"]
            column_value = getattr(thread, column_field)

            thread_value = format_number(column_value) if column_format_number else column_value
            if thread_id in processlist_datatable.rows:
                datatable_value = processlist_datatable.get_row(thread_id)[column_id]

                # Initialize temp values for possible Syntax object comparison below
                temp_thread_value = thread_value
                temp_datatable_value = datatable_value

                # If the column is the query, we need to compare the code of the Syntax object
                update_width = False
                if column_field == "formatted_query":
                    update_width = True
                    if isinstance(thread_value, Syntax):
                        temp_thread_value = thread_value.code

                        # Only show the first {query_length_max} characters of the query
                        thread_value = format_query(thread_value.code[:query_length_max])
                    if isinstance(datatable_value, Syntax):
                        temp_datatable_value = datatable_value.code

                # Update the datatable if values differ
                if (
                    temp_thread_value != temp_datatable_value
                    or column_field == "formatted_time"
                    or column_field == "time"
                ):
                    processlist_datatable.update_cell(thread_id, column_name, thread_value, update_width=update_width)
            else:
                # Only show the first {query_length_max} characters of the query
                if column_field == "formatted_query" and isinstance(thread_value, Syntax):
                    thread_value = format_query(thread_value.code[:query_length_max])

                # Create an array of values to append to the datatable
                row_values.append(thread_value)

        # Add a new row to the datatable
        if row_values:
            processlist_datatable.add_row(*row_values, key=thread_id)

    # Remove threads that were filtered out
    for thread_id in filter_threads:
        dolphie.processlist_threads.pop(thread_id)

    # Remove rows from processlist_datatable that no longer exist in processlist_threads
    if dolphie.processlist_threads:
        rows_to_remove = set(processlist_datatable.rows.keys()) - set(dolphie.processlist_threads.keys())
        for id in rows_to_remove:
            processlist_datatable.remove_row(id)
    else:
        if processlist_datatable.row_count:
            processlist_datatable.clear()

    processlist_datatable.sort("time_seconds", reverse=dolphie.sort_by_time_descending)

    tab.processlist_title.update(f"Processlist ([highlight]{processlist_datatable.row_count}[/highlight])")


def fetch_data(tab: Tab) -> Dict[str, ProcesslistThread]:
    dolphie = tab.dolphie

    if dolphie.use_performance_schema:
        processlist_query = MySQLQueries.ps_query
        if not dolphie.is_mysql_version_at_least("5.7"):
            # Remove the connection_type field for MySQL versions below 5.7 since it doesn't exist
            processlist_query = processlist_query.replace("connection_type", '""')
    else:
        processlist_query = MySQLQueries.pl_query

    ########################
    # WHERE clause filters #
    ########################
    where_clause = []

    # Filter out idle threads if specified
    if not dolphie.show_idle_threads:
        if dolphie.use_performance_schema:
            where_clause.append(
                "(processlist_command != 'Sleep' AND processlist_command NOT LIKE 'Binlog Dump%') AND (processlist_info"
                " IS NOT NULL OR trx_query IS NOT NULL) AND IFNULL(processlist_state, '') NOT LIKE 'Group Replication"
                " Module%'"
            )
        else:
            where_clause.append(
                "(Command != 'Sleep' AND Command NOT LIKE 'Binlog Dump%') AND (Info IS NOT NULL OR trx_query IS NOT"
                " NULL) AND IFNULL(State, '') NOT LIKE 'Group Replication Module%'"
            )

    # Only show running transactions only
    if dolphie.show_trxs_only:
        where_clause.append("trx_state != ''")

    if dolphie.show_threads_with_concurrency_tickets:
        where_clause.append("trx_concurrency_tickets > 0")

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
        processlist_query = processlist_query.replace("$1", "AND " + " AND ".join(where_clause))
    else:
        processlist_query = processlist_query.replace("$1", "")

    processlist_threads = {}
    # Run the processlist query
    dolphie.main_db_connection.execute(processlist_query)
    threads = dolphie.main_db_connection.fetchall()

    for thread in threads:
        # Don't include Dolphie's threads
        if (
            dolphie.main_db_connection.connection_id == thread["id"]
            or dolphie.secondary_db_connection.connection_id == thread["id"]
        ):
            continue

        # Use trx_query over Performance Schema query since it's more accurate
        if dolphie.use_performance_schema and thread["trx_query"]:
            thread["query"] = thread["trx_query"]
        thread["query"] = "" if thread["query"] is None else thread["query"]

        if thread["host"]:
            host = thread["host"].split(":")[0]
            thread["host"] = dolphie.get_hostname(host)

        # Remove trx_query from the thread data since it's not needed
        thread.pop("trx_query", None)

        processlist_threads[str(thread["id"])] = ProcesslistThread(thread)

    return processlist_threads
