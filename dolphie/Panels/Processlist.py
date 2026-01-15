
from rich.syntax import Syntax
from textual.widgets import DataTable

from dolphie.DataTypes import ProcesslistThread
from dolphie.Modules.Functions import format_number, format_query
from dolphie.Modules.Queries import MySQLQueries
from dolphie.Modules.TabManager import Tab


def create_panel(tab: Tab) -> DataTable:
    dolphie = tab.dolphie

    columns = [
        {"name": "Thread ID", "field": "id", "width": None, "format_number": False},
    ]

    if dolphie.use_performance_schema_for_processlist:
        columns.extend(
            [
                {
                    "name": "Protocol",
                    "field": "protocol",
                    "width": 8,
                    "format_number": False,
                }
            ]
        )

    columns.extend(
        [
            {"name": "Username", "field": "user", "width": 20, "format_number": False},
        ]
    )

    if dolphie.show_additional_query_columns:
        columns.extend(
            [
                {
                    "name": "Hostname/IP",
                    "field": "host",
                    "width": 25,
                    "format_number": False,
                },
                {
                    "name": "Database",
                    "field": "db",
                    "width": 15,
                    "format_number": False,
                },
            ]
        )

    columns.extend(
        [
            {"name": "Command", "field": "command", "width": 8, "format_number": False},
            {"name": "State", "field": "state", "width": 20, "format_number": False},
            {
                "name": "TRX State",
                "field": "trx_state",
                "width": 9,
                "format_number": False,
            },
            {
                "name": "R-Lock",
                "field": "trx_rows_locked",
                "width": 7,
                "format_number": True,
            },
            {
                "name": "R-Mod",
                "field": "trx_rows_modified",
                "width": 7,
                "format_number": True,
            },
        ]
    )

    if (
        dolphie.show_additional_query_columns
        and dolphie.global_variables.get("innodb_thread_concurrency")
    ) or dolphie.show_threads_with_concurrency_tickets:
        columns.append(
            {
                "name": "Tickets",
                "field": "trx_concurrency_tickets",
                "width": 8,
                "format_number": False,
            }
        )

    if dolphie.show_trxs_only:
        columns.append(
            {
                "name": "TRX Age",
                "field": "trx_time",
                "width": 9,
                "format_number": False,
            },
        )

    columns.extend(
        [
            {
                "name": "Age",
                "field": "formatted_time",
                "width": 9,
                "format_number": False,
            },
            {
                "name": "Query",
                "field": "formatted_query",
                "width": None,
                "format_number": False,
            },
            {
                "name": "time_seconds",
                "field": "time",
                "width": 0,
                "format_number": False,
            },
        ]
    )

    query_length_max = 300
    processlist_datatable = tab.processlist_datatable

    if len(processlist_datatable.columns) != len(columns):
        processlist_datatable.clear(columns=True)

    column_names = []
    column_fields = []
    column_format_numbers = []

    if not processlist_datatable.columns:
        for column_data in columns:
            processlist_datatable.add_column(
                column_data["name"], key=column_data["name"], width=column_data["width"]
            )

    for column_data in columns:
        column_names.append(column_data["name"])
        column_fields.append(column_data["field"])
        column_format_numbers.append(column_data["format_number"])

    threads_to_render: dict[str, ProcesslistThread] = {}
    # We use filter here for replays since the original way requires changing WHERE clause
    if dolphie.replay_file:
        for thread_id, thread in dolphie.processlist_threads.items():
            thread: ProcesslistThread

            # Check each filter condition and skip thread if it doesn't match
            if dolphie.show_trxs_only and thread.trx_state == "[dark_gray]N/A":
                continue

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
                dolphie.show_threads_with_concurrency_tickets
                and thread.trx_concurrency_tickets == "[dark_gray]0"
            ):
                continue

            # If all checks passed, add it to the visible list
            threads_to_render[thread_id] = thread
    else:
        # Not a replay file, so fetch_data() already filtered.
        threads_to_render = dolphie.processlist_threads

    for thread_id, thread in threads_to_render.items():
        thread: ProcesslistThread

        if thread_id in processlist_datatable.rows:
            datatable_row = processlist_datatable.get_row(thread_id)

            for column_id, (
                column_name,
                column_field,
                column_format_number,
            ) in enumerate(zip(column_names, column_fields, column_format_numbers)):
                column_value = getattr(thread, column_field)
                thread_value = (
                    format_number(column_value)
                    if column_format_number
                    else column_value
                )

                # Use the cached row data
                datatable_value = datatable_row[column_id]

                # Cell comparison logic
                temp_thread_value = thread_value
                temp_datatable_value = datatable_value
                update_width = False

                if column_field == "formatted_query":
                    update_width = True
                    if isinstance(thread_value, Syntax):
                        temp_thread_value = thread_value.code[:query_length_max]
                        thread_value = format_query(temp_thread_value)
                    if isinstance(datatable_value, Syntax):
                        temp_datatable_value = datatable_value.code

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
            for column_field, column_format_number in zip(
                column_fields, column_format_numbers
            ):
                column_value = getattr(thread, column_field)
                thread_value = (
                    format_number(column_value)
                    if column_format_number
                    else column_value
                )

                if column_field == "formatted_query" and isinstance(
                    thread_value, Syntax
                ):
                    thread_value = format_query(thread_value.code[:query_length_max])

                row_values.append(thread_value)

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
        # No threads to show, clear the table
        if processlist_datatable.row_count:
            processlist_datatable.clear()

    processlist_datatable.sort("time_seconds", reverse=dolphie.sort_by_time_descending)

    title = (
        f"{dolphie.panels.get_panel_title(dolphie.panels.processlist.name)} "
        f"([$highlight]{processlist_datatable.row_count}[/$highlight]"
    )
    if dolphie.show_threads_with_concurrency_tickets:
        title += f"/[$highlight]{dolphie.global_variables.get('innodb_thread_concurrency')}[/$highlight]"
    title += ")"
    tab.processlist_title.update(title)


def fetch_data(tab: Tab) -> dict[str, ProcesslistThread]:
    dolphie = tab.dolphie

    # Determine query and column names based on whether performance_schema is used
    if (
        dolphie.performance_schema_enabled
        and dolphie.use_performance_schema_for_processlist
    ):
        processlist_query = MySQLQueries.ps_query
        if not dolphie.is_mysql_version_at_least("5.7"):
            processlist_query = processlist_query.replace("connection_type", '""')
        user_col, db_col, host_col, time_col, info_col, state_col, command_col = (
            "processlist_user",
            "processlist_db",
            "processlist_host",
            "processlist_time",
            "processlist_info",
            "processlist_state",
            "processlist_command",
        )
    else:
        processlist_query = MySQLQueries.pl_query
        user_col, db_col, host_col, time_col, info_col, state_col, command_col = (
            "User",
            "db",
            "Host",
            "Time",
            "Info",
            "State",
            "Command",
        )

    # Build the WHERE clause
    where_clause = []
    if not dolphie.show_idle_threads:
        where_clause.append(
            f"({command_col} != 'Sleep' AND {command_col} NOT LIKE 'Binlog Dump%') AND ({info_col}"
            f" IS NOT NULL OR trx_query IS NOT NULL) AND IFNULL({state_col}, '') NOT LIKE 'Group Replication"
            " Module%'"
        )
    if dolphie.show_trxs_only:
        where_clause.append("trx_state != ''")
    if dolphie.show_threads_with_concurrency_tickets:
        where_clause.append("trx_concurrency_tickets > 0")
    if dolphie.user_filter:
        where_clause.append(f"{user_col} = '{dolphie.user_filter}'")
    if dolphie.db_filter:
        where_clause.append(f"{db_col} = '{dolphie.db_filter}'")
    if dolphie.host_filter:
        where_clause.append(f"{host_col} LIKE '{dolphie.host_filter}%'")
    if dolphie.query_time_filter:
        where_clause.append(f"{time_col} >= '{dolphie.query_time_filter}'")
    if dolphie.query_filter:
        if dolphie.use_performance_schema_for_processlist:
            where_clause.append(
                f"({info_col} LIKE '%%{dolphie.query_filter}%%' OR trx_query LIKE '%%{dolphie.query_filter}%%')"
            )
        else:
            where_clause.append(f"{info_col} LIKE '%%{dolphie.query_filter}%%'")

    # Add the WHERE clause to the query
    if where_clause:
        processlist_query = processlist_query.replace(
            "$1", "AND " + " AND ".join(where_clause)
        )
    else:
        processlist_query = processlist_query.replace("$1", "")

    # Execute the query and fetch the results
    dolphie.main_db_connection.execute(processlist_query)
    threads = dolphie.main_db_connection.fetchall()

    processlist_threads = {}
    for thread in threads:
        # Don't include Dolphie's own threads
        if dolphie.main_db_connection.connection_id == thread["id"] or (
            dolphie.secondary_db_connection
            and dolphie.secondary_db_connection.connection_id == thread["id"]
        ):
            continue

        # Use trx_query from InnoDB since it's more accurate than P_S
        if dolphie.use_performance_schema_for_processlist and thread["trx_query"]:
            thread["query"] = thread["trx_query"]
        thread["query"] = thread["query"] or ""

        # Resolve hostname if possible
        if thread["host"]:
            host = thread["host"].split(":")[0]
            thread["host"] = dolphie.get_hostname(host)

        # We don't need trx_query anymore
        thread.pop("trx_query", None)

        processlist_threads[str(thread["id"])] = ProcesslistThread(thread)

    return processlist_threads
