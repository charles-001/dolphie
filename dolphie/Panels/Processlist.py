from __future__ import annotations

from rich.syntax import Syntax

from dolphie.DataTypes import ProcesslistThread, ProxySQLProcesslistThread
from dolphie.Modules.Functions import (
    coerce_int,
    coerce_str,
    filter_excludes,
    filter_sql_condition,
    format_number,
    format_query,
    host_without_port,
)
from dolphie.Modules.Queries import MySQLQueries
from dolphie.Modules.TabManager import Tab


def create_panel(tab: Tab) -> None:
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
                    "width": 25,
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
        dolphie.show_additional_query_columns and dolphie.global_variables.get("innodb_thread_concurrency")
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
            processlist_datatable.add_column(column_data["name"], key=column_data["name"], width=column_data["width"])

    for column_data in columns:
        column_names.append(column_data["name"])
        column_fields.append(column_data["field"])
        column_format_numbers.append(column_data["format_number"])

    # Has to happen before the filtering below so replays remember the values being filtered out
    dolphie.record_filter_dropdown_values()

    threads_to_render: dict[int, ProcesslistThread | ProxySQLProcesslistThread] = {}
    # We use filter here for replays since the original way requires changing WHERE clause
    if dolphie.replay_file:
        for thread_id, thread in dolphie.processlist_threads.items():
            if not isinstance(thread, ProcesslistThread):
                continue

            # Check each filter condition and skip thread if it doesn't match
            if dolphie.show_trxs_only and thread.trx_state == "[$dark_gray]N/A":
                continue

            if dolphie.user_filter and filter_excludes(dolphie.user_filter, thread.user):
                continue

            if dolphie.db_filter and filter_excludes(dolphie.db_filter, thread.db):
                continue

            if dolphie.host_filter and filter_excludes(dolphie.host_filter, thread.host, partial=True):
                continue

            if dolphie.query_time_filter and thread.time < dolphie.query_time_filter:
                continue

            if dolphie.query_filter and filter_excludes(
                dolphie.query_filter, thread.formatted_query.code, partial=True
            ):
                continue

            if dolphie.show_threads_with_concurrency_tickets and thread.trx_concurrency_tickets == "[$dark_gray]0":
                continue

            # If all checks passed, add it to the visible list
            threads_to_render[thread_id] = thread
    else:
        # Not a replay file, so fetch_data() already filtered.
        threads_to_render = dolphie.processlist_threads

    changed = False

    with dolphie.app.batch_update():
        # Remove stale rows first so updates/adds operate on a cleaner table
        if threads_to_render:
            active_row_keys = {str(thread_id) for thread_id in threads_to_render}
            rows_to_remove = set(processlist_datatable.rows.keys()) - active_row_keys
            if rows_to_remove:
                changed = True
                # Bulk-clear and re-add when most rows are stale
                if len(rows_to_remove) > len(threads_to_render):
                    processlist_datatable.clear()
                else:
                    for id in rows_to_remove:
                        processlist_datatable.remove_row(id)
        else:
            # No threads to show, clear the table
            if processlist_datatable.row_count:
                changed = True
                processlist_datatable.clear()

        for thread_id, thread in threads_to_render.items():
            if not isinstance(thread, ProcesslistThread):
                continue

            row_key = str(thread_id)

            row_values = []
            for column_field, column_format_number in zip(column_fields, column_format_numbers, strict=True):
                value = getattr(thread, column_field)
                if column_format_number:
                    value = format_number(value)
                if column_field == "formatted_query" and isinstance(value, Syntax):
                    value = format_query(value.code[:query_length_max])
                row_values.append(value)

            row_values = processlist_datatable.normalize_cells(row_values)
            if row_key in processlist_datatable.rows:
                datatable_row = processlist_datatable.get_row(row_key)

                for column_id, (column_name, column_field) in enumerate(zip(column_names, column_fields, strict=True)):
                    new_val = row_values[column_id]
                    old_val = datatable_row[column_id]

                    # Compare text content for Syntax objects
                    cmp_new = new_val.code if isinstance(new_val, Syntax) else new_val
                    cmp_old = old_val.code if isinstance(old_val, Syntax) else old_val

                    if cmp_new != cmp_old or column_field == "formatted_time" or column_field == "time":
                        changed = True
                        processlist_datatable.update_cell(
                            row_key,
                            column_name,
                            new_val,
                            update_width=(column_field == "formatted_query"),
                        )
            else:
                changed = True
                processlist_datatable.add_row(*row_values, key=row_key)

        if changed:
            processlist_datatable.sort("time_seconds", reverse=dolphie.sort_by_time_descending)

    if dolphie.replay_file:
        dolphie.processlist_threads = threads_to_render

    title = f"{dolphie.panels.processlist.title} ([$highlight]{processlist_datatable.row_count}[/$highlight]"
    if dolphie.show_threads_with_concurrency_tickets:
        title += f"/[$highlight]{dolphie.global_variables.get('innodb_thread_concurrency')}[/$highlight]"
    title += ")"
    tab.processlist_title.update(title)


def fetch_data(tab: Tab) -> dict[int, ProcesslistThread | ProxySQLProcesslistThread]:
    dolphie = tab.dolphie

    # Determine query and column names based on whether performance_schema is used
    if dolphie.performance_schema_enabled and dolphie.use_performance_schema_for_processlist:
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
        where_clause.append(filter_sql_condition(user_col, dolphie.user_filter))
    if dolphie.db_filter:
        where_clause.append(filter_sql_condition(db_col, dolphie.db_filter))
    if dolphie.host_filter:
        where_clause.append(filter_sql_condition(host_col, dolphie.host_filter, "{}%"))
    if dolphie.query_time_filter:
        where_clause.append(f"{time_col} >= '{dolphie.query_time_filter}'")
    if dolphie.query_filter:
        if dolphie.use_performance_schema_for_processlist:
            where_clause.append(filter_sql_condition([info_col, "trx_query"], dolphie.query_filter, "%%{}%%"))
        else:
            where_clause.append(filter_sql_condition(info_col, dolphie.query_filter, "%%{}%%"))

    # Add the WHERE clause to the query
    if where_clause:
        processlist_query = processlist_query.replace("$1", "AND " + " AND ".join(where_clause))
    else:
        processlist_query = processlist_query.replace("$1", "")

    # Execute the query and fetch the results
    dolphie.main_db_connection.execute(processlist_query)
    threads = dolphie.main_db_connection.fetchall()

    processlist_threads = {}
    for thread in threads:
        # Don't include Dolphie's own threads
        if dolphie.main_db_connection.connection_id == thread["id"] or (
            dolphie.secondary_db_connection and dolphie.secondary_db_connection.connection_id == thread["id"]
        ):
            continue

        # Use trx_query from InnoDB since it's more accurate than P_S
        if dolphie.use_performance_schema_for_processlist and thread["trx_query"]:
            thread["query"] = thread["trx_query"]
        thread["query"] = thread["query"] or ""

        # Resolve hostname if possible
        if thread["host"]:
            thread["host"] = dolphie.get_hostname(host_without_port(coerce_str(thread["host"])))

        # We don't need trx_query anymore
        thread.pop("trx_query", None)

        processlist_threads[coerce_int(thread["id"])] = ProcesslistThread(thread)

    return processlist_threads
