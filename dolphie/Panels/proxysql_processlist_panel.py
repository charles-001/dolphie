from typing import Dict

from rich.syntax import Syntax
from textual.widgets import DataTable

from dolphie.DataTypes import ProcesslistThread, ProxySQLProcesslistThread
from dolphie.Modules.Functions import format_query
from dolphie.Modules.Queries import ProxySQLQueries
from dolphie.Modules.TabManager import Tab


def create_panel(tab: Tab) -> DataTable:
    dolphie = tab.dolphie

    columns = [
        {"name": "Thread ID", "field": "id", "width": 11},
        {"name": "Hostgroup", "field": "hostgroup", "width": 9},
        {"name": "Username", "field": "user", "width": 20},
    ]

    if dolphie.show_additional_query_columns:
        columns.extend(
            [
                {"name": "Frontend Host", "field": "frontend_host", "width": 25},
            ]
        )

    columns.extend(
        [
            {"name": "Backend Host", "field": "host", "width": 25},
            {"name": "Database", "field": "db", "width": 17},
            {"name": "Command", "field": "command", "width": 8},
            {"name": "Age", "field": "formatted_time", "width": 9},
            {"name": "Query", "field": "formatted_query", "width": None},
            {"name": "time_seconds", "field": "time", "width": 0},
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

        thread: ProxySQLProcesslistThread
        # We use filter here for replays since the original way requires changing WHERE clause
        if dolphie.replay_file:
            found = False
            if dolphie.user_filter and dolphie.user_filter != thread.user:
                found = True
            elif dolphie.db_filter and dolphie.db_filter != thread.db:
                found = True
            elif dolphie.host_filter and dolphie.host_filter not in thread.host:
                found = True
            elif dolphie.query_time_filter and int(dolphie.query_time_filter) >= thread.time:
                found = True
            elif dolphie.query_filter and dolphie.query_filter not in thread.formatted_query.code:
                found = True
            elif dolphie.hostgroup_filter and int(dolphie.hostgroup_filter) != thread.hostgroup:
                found = True

            if found:
                filter_threads.append(thread_id)
                continue

        for column_id, (column_data) in enumerate(columns):
            column_name = column_data["name"]
            column_field = column_data["field"]
            column_value = getattr(thread, column_field)

            thread_value = column_value

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
                        temp_thread_value = thread_value.code[:query_length_max]

                        # Only show the first {query_length_max} characters of the query
                        thread_value = format_query(thread_value.code[:query_length_max])
                    if isinstance(datatable_value, Syntax):
                        temp_datatable_value = datatable_value.code[:query_length_max]

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

    ########################
    # WHERE clause filters #
    ########################
    where_clause = []

    # Filter out idle threads if specified
    if not dolphie.show_idle_threads:
        where_clause.append("command NOT IN ('Sleep', 'Connecting client')")

    # Filter user
    if dolphie.user_filter:
        where_clause.append("user = '%s'" % dolphie.user_filter)

    # Filter database
    if dolphie.db_filter:
        where_clause.append("db = '%s'" % dolphie.db_filter)

    # Filter hostname/IP
    if dolphie.host_filter:
        where_clause.append("srv_host = '%s'" % dolphie.host_filter)

    # Filter time
    if dolphie.query_time_filter:
        # Convert to seconds
        time = dolphie.query_time_filter * 1000
        where_clause.append("time_ms >= '%s'" % time)

    # Filter query
    if dolphie.query_filter:
        where_clause.append("info LIKE '%%%s%%'" % dolphie.query_filter)

    # Filter hostgroup
    if dolphie.hostgroup_filter:
        where_clause.append("hostgroup = '%s'" % dolphie.hostgroup_filter)

    # Add in our dynamic WHERE clause for filtering
    if where_clause:
        processlist_query = ProxySQLQueries.processlist.replace("$1", " AND ".join(where_clause))
    else:
        processlist_query = ProxySQLQueries.processlist.replace("$1", "1=1")

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

        thread["frontend_host"] = dolphie.get_hostname(thread["frontend_host"])
        thread["backend_host"] = dolphie.get_hostname(thread["backend_host"])
        thread["query"] = "" if thread["query"] is None else thread["query"]

        processlist_threads[str(thread["id"])] = ProxySQLProcesslistThread(thread)

    return processlist_threads
