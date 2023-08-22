import re
from datetime import timedelta

from dolphie import Dolphie
from dolphie.Modules.Functions import format_number, format_time
from dolphie.Modules.Queries import MySQLQueries
from rich.text import Text
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
        and "innodb_thread_concurrency" in dolphie.global_variables
        and dolphie.global_variables["innodb_thread_concurrency"]
    ):
        columns.append({"name": "Tickets", "field": "trx_concurrency_tickets", "width": 8, "format_number": True})

    columns.extend(
        [
            {"name": "Time", "field": "formatted_time", "width": 9, "format_number": False},
            {"name": "Query", "field": "query", "width": None, "format_number": False},
        ]
    )

    processlist_datatable = dolphie.app.query_one("#panel_processlist", DataTable)

    # Clear table if columns change
    if len(processlist_datatable.columns) != len(columns):
        processlist_datatable.clear(columns=True)

    # Add columns to the datatable if it is empty
    if not processlist_datatable.columns:
        for column_data in columns:
            column_name = column_data["name"]
            column_key = column_data["field"]
            column_width = column_data["width"]
            processlist_datatable.add_column(column_name, key=column_key, width=column_width)

    # Iterate through dolphie.processlist_threads
    for thread_id, thread in dolphie.processlist_threads.items():
        # Add or modify the "command" field based on the condition
        if thread["command"] == "Killed":
            thread["command"] = "[#fc7979]Killed"

        # Check if the thread_id exists in the datatable
        if thread_id in processlist_datatable.rows:
            datatable_row = processlist_datatable.get_row(thread_id)

            # Update the datatable if values differ
            for column_id, column_data in enumerate(columns):
                column_name = column_data["field"]
                column_format_number = column_data["format_number"]

                update_width = False
                if column_name == "query":
                    value = re.sub(r"\s+", " ", thread[column_name])
                    update_width = True
                else:
                    value = format_number(thread[column_name]) if column_format_number else thread[column_name]

                if value != datatable_row[column_id] or column_name == "formatted_time":
                    processlist_datatable.update_cell(thread_id, column_name, value, update_width=update_width)
        else:
            # Add a new row to the datatable if thread_id does not exist
            row_values = []
            for column_data in columns:
                column_name = column_data["field"]
                column_format_number = column_data["format_number"]

                if column_name == "query":
                    value = re.sub(r"\s+", " ", thread[column_name])
                else:
                    value = format_number(thread[column_name]) if column_format_number else thread[column_name]

                row_values.append(value)

            processlist_datatable.add_row(*row_values, key=thread_id)

    # Remove rows from processlist_datatable that no longer exist in dolphie.processlist_threads
    rows_to_remove = set(processlist_datatable.rows.keys()) - set(dolphie.processlist_threads.keys())
    for id in rows_to_remove:
        processlist_datatable.remove_row(id)

    processlist_datatable.sort("formatted_time", reverse=dolphie.sort_by_time_descending)


def fetch_data(dolphie: Dolphie):
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
        # Don't include Dolphie's threads
        if dolphie.main_db_connection_id == thread["id"] or dolphie.secondary_db_connection_id == thread["id"]:
            continue

        command = thread["command"]
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
        elif query:
            if time >= 10:
                thread_color = "#fc7979"
            elif time >= 5:
                thread_color = "#f1fb82"
            else:
                thread_color = "#54efae"

        formatted_time = TextPlus(format_time(time), style=thread_color)
        formatted_time_with_days = TextPlus("{:0>8}".format(str(timedelta(seconds=time))), style=thread_color)

        host = thread["host"].split(":")[0]
        host = dolphie.get_hostname(host)

        mysql_thread_id = thread.get("mysql_thread_id")

        processlist_threads[str(thread["id"])] = {
            "id": str(thread["id"]),
            "mysql_thread_id": mysql_thread_id,
            "user": thread["user"],
            "host": host,
            "db": thread["db"],
            "time": time,
            "formatted_time_with_days": formatted_time_with_days,
            "formatted_time": formatted_time,
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


class TextPlus(Text):
    """Custom patch for a Rich `Text` object to allow Textual `DataTable`
    sorting when a Text object is included in a row."""

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, Text):
            return NotImplemented
        return self.plain < other.plain

    def __le__(self, other: object) -> bool:
        if not isinstance(other, Text):
            return NotImplemented
        return self.plain <= other.plain

    def __gt__(self, other: object) -> bool:
        if not isinstance(other, Text):
            return NotImplemented
        return self.plain > other.plain

    def __ge__(self, other: object) -> bool:
        if not isinstance(other, Text):
            return NotImplemented
        return self.plain >= other.plain
