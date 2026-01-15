
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

    column_names = []
    column_fields = []

    # Add columns to the datatable if it is empty
    if not processlist_datatable.columns:
        for column_data in columns:
            processlist_datatable.add_column(
                column_data["name"], key=column_data["name"], width=column_data["width"]
            )

    for column_data in columns:
        column_names.append(column_data["name"])
        column_fields.append(column_data["field"])

    threads_to_render: dict[str, ProxySQLProcesslistThread] = {}
    if dolphie.replay_file:
        for thread_id, thread in dolphie.processlist_threads.items():
            thread: ProxySQLProcesslistThread

            # Check each filter condition and skip thread if it doesn't match
            if dolphie.user_filter and dolphie.user_filter != thread.user:
                continue

            if dolphie.db_filter and dolphie.db_filter != thread.db:
                continue

            if dolphie.host_filter and dolphie.host_filter not in thread.host:
                continue

            if dolphie.query_time_filter and thread.time < dolphie.query_time_filter:
                continue

            if (
                dolphie.query_filter
                and dolphie.query_filter not in thread.formatted_query.code
            ):
                continue

            if (
                dolphie.hostgroup_filter
                and dolphie.hostgroup_filter != thread.hostgroup
            ):
                continue

            # If all checks passed, add it to the visible list
            threads_to_render[thread_id] = thread
    else:
        # Not a replay file, so fetch_data() already filtered.
        threads_to_render = dolphie.processlist_threads

    for thread_id, thread in threads_to_render.items():
        thread: ProxySQLProcesslistThread

        if thread_id in processlist_datatable.rows:
            datatable_row = processlist_datatable.get_row(thread_id)

            for column_id, (column_name, column_field) in enumerate(
                zip(column_names, column_fields)
            ):
                column_value = getattr(thread, column_field)

                thread_value = column_value

                # Use the cached row data
                datatable_value = datatable_row[column_id]

                # Initialize temp values for possible Syntax object comparison below
                temp_thread_value = thread_value
                temp_datatable_value = datatable_value

                # If the column is the query, we need to compare the code of the Syntax object
                update_width = False
                if column_field == "formatted_query":
                    update_width = True
                    if isinstance(thread_value, Syntax):
                        temp_thread_value = thread_value.code[:query_length_max]
                        thread_value = format_query(temp_thread_value)
                    if isinstance(datatable_value, Syntax):
                        temp_datatable_value = datatable_value.code

                # Update the datatable if values differ
                if (
                    temp_thread_value != temp_datatable_value
                    or column_field == "formatted_time"
                    or column_field == "time"
                ):
                    processlist_datatable.update_cell(
                        thread_id, column_name, thread_value, update_width=update_width
                    )
        else:
            row_values = []

            for column_id, (column_name, column_field) in enumerate(
                zip(column_names, column_fields)
            ):
                column_value = getattr(thread, column_field)

                thread_value = column_value

                # Only show the first {query_length_max} characters of the query
                if column_field == "formatted_query" and isinstance(
                    thread_value, Syntax
                ):
                    thread_value = format_query(thread_value.code[:query_length_max])

                # Create an array of values to append to the datatable
                row_values.append(thread_value)

            # Add a new row to the datatable
            if row_values:
                processlist_datatable.add_row(*row_values, key=thread_id)

    if dolphie.replay_file:
        dolphie.processlist_threads = threads_to_render

    # Remove rows from datatable that are no longer in our render list
    if threads_to_render:
        rows_to_remove = set(processlist_datatable.rows.keys()) - set(
            threads_to_render.keys()
        )
        for id in rows_to_remove:
            processlist_datatable.remove_row(id)
    else:
        if processlist_datatable.row_count:
            processlist_datatable.clear()

    processlist_datatable.sort("time_seconds", reverse=dolphie.sort_by_time_descending)

    tab.processlist_title.update(
        f"{dolphie.panels.get_panel_title(dolphie.panels.processlist.name)} "
        f"([$highlight]{processlist_datatable.row_count}[/$highlight])"
    )


def fetch_data(tab: Tab) -> dict[str, ProcesslistThread]:
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
        where_clause.append(f"user = '{dolphie.user_filter}'")

    # Filter database
    if dolphie.db_filter:
        where_clause.append(f"db = '{dolphie.db_filter}'")

    # Filter hostname/IP
    if dolphie.host_filter:
        where_clause.append(f"srv_host = '{dolphie.host_filter}'")

    # Filter time
    if dolphie.query_time_filter:
        # Convert to milliseconds
        where_clause.append(f"time_ms >= '{dolphie.query_time_filter * 1000}'")

    # Filter query
    if dolphie.query_filter:
        where_clause.append(f"info LIKE '%%{dolphie.query_filter}%%'")

    # Filter hostgroup
    if dolphie.hostgroup_filter:
        where_clause.append(f"hostgroup = '{dolphie.hostgroup_filter}'")

    # Add in our dynamic WHERE clause for filtering
    if where_clause:
        processlist_query = ProxySQLQueries.processlist.replace(
            "$1", " AND ".join(where_clause)
        )
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
        thread["query"] = thread["query"] or ""

        processlist_threads[str(thread["id"])] = ProxySQLProcesslistThread(thread)

    return processlist_threads
