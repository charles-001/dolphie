from typing import Dict

from dolphie.DataTypes import ProcesslistThread
from dolphie.Modules.Functions import format_number
from dolphie.Modules.Queries import MySQLQueries
from dolphie.Modules.TabManager import Tab
from textual.widgets import DataTable


def create_panel(tab: Tab) -> DataTable:
    dolphie = tab.dolphie

    if not dolphie.performance_schema_enabled and dolphie.use_performance_schema:
        dolphie.notify("Performance Schema is not enabled on this host, using Information Schema instead")
        dolphie.use_performance_schema = False

    columns = [
        {"name": "Thread ID", "field": "id", "width": 11, "format_number": False},
        {"name": "Username", "field": "user", "width": 13, "format_number": False},
    ]

    if dolphie.show_additional_query_columns:
        columns.extend([
            {"name": "Hostname/IP", "field": "host", "width": 16, "format_number": False},
            {"name": "Database", "field": "db", "width": 13, "format_number": False},
        ])

    columns.extend([
        {"name": "Command", "field": "command", "width": 8, "format_number": False},
        {"name": "State", "field": "state", "width": 16, "format_number": False},
        {"name": "TRX State", "field": "trx_state", "width": 9, "format_number": False},
        {"name": "R-Lock", "field": "trx_rows_locked", "width": 7, "format_number": True},
        {"name": "R-Mod", "field": "trx_rows_modified", "width": 7, "format_number": True},
    ])

    if dolphie.show_additional_query_columns and dolphie.global_variables.get("innodb_thread_concurrency"):
        columns.append({"name": "Tickets", "field": "trx_concurrency_tickets", "width": 8, "format_number": True})

    if dolphie.show_trxs_only:
        columns.append(
            {"name": "TRX Time", "field": "trx_time", "width": 9, "format_number": False},
        )

    columns.extend([
        {"name": "Time", "field": "formatted_time", "width": 9, "format_number": False},
        {"name": "Query", "field": "formatted_query", "width": None, "format_number": False},
        {"name": "time_seconds", "field": "time", "width": 0, "format_number": False},
    ])

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

    # Iterate through dolphie.processlist_threads
    for thread_id, thread in dolphie.processlist_threads.items():
        # Check if the thread_id exists in the datatable
        if thread_id in processlist_datatable.rows:
            datatable_row = processlist_datatable.get_row(thread_id)

            # Update the datatable if values differ
            for column_id, column_data in enumerate(columns):
                column_name = column_data["name"]
                column_field = column_data["field"]
                column_format_number = column_data["format_number"]
                column_value = getattr(thread, column_field)

                update_width = False
                if column_field == "formatted_query":
                    update_width = True

                value = format_number(column_value) if column_format_number else column_value
                if value != datatable_row[column_id] or column_field == "formatted_time":
                    processlist_datatable.update_cell(thread_id, column_name, value, update_width=update_width)
        else:
            # Add a new row to the datatable if thread_id does not exist
            row_values = []
            for column_data in columns:
                column_field = column_data["field"]
                column_format_number = column_data["format_number"]
                column_value = getattr(thread, column_field)

                value = format_number(column_value) if column_format_number else column_value

                row_values.append(value)

            processlist_datatable.add_row(*row_values, key=thread_id)

    # Remove rows from processlist_datatable that no longer exist in dolphie.processlist_threads
    rows_to_remove = set(processlist_datatable.rows.keys()) - set(dolphie.processlist_threads.keys())
    for id in rows_to_remove:
        processlist_datatable.remove_row(id)

    processlist_datatable.sort("time_seconds", reverse=dolphie.sort_by_time_descending)

    tab.processlist_title.update(f"Processlist ([highlight]{len(dolphie.processlist_threads)}[/highlight])")


def fetch_data(tab: Tab) -> Dict[str, ProcesslistThread]:
    dolphie = tab.dolphie

    if dolphie.use_performance_schema:
        processlist_query = MySQLQueries.ps_query
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
        else:
            thread["query"] = thread["query"]

        host = thread["host"].split(":")[0]
        thread["host"] = dolphie.get_hostname(host)

        processlist_threads[str(thread["id"])] = ProcesslistThread(thread)

    return processlist_threads
