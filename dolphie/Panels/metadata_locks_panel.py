from typing import Dict

from dolphie.Modules.Functions import format_query, format_time
from dolphie.Modules.Queries import MySQLQueries
from dolphie.Modules.TabManager import Tab
from rich.syntax import Syntax
from textual.widgets import DataTable


def create_panel(tab: Tab) -> DataTable:
    dolphie = tab.dolphie

    columns = {
        "OBJECT_TYPE": {"name": "Object Type", "width": 13},
        "OBJECT_SCHEMA": {"name": "Object Schema", "width": 13},
        "OBJECT_NAME": {"name": "Object Name", "width": 25},
        "LOCK_TYPE": {"name": "Lock Type", "width": 20},
        "LOCK_STATUS": {"name": "Lock Status", "width": 11},
        "CODE_SOURCE": {"name": "Code Source", "width": 15},
        "THREAD_SOURCE": {"name": "Thread Source", "width": 15},
        "PROCESSLIST_ID": {"name": "Process ID", "width": 13},
        "PROCESSLIST_USER": {"name": "User", "width": 20},
        "PROCESSLIST_TIME": {"name": "Age", "width": 8},
        "PROCESSLIST_INFO": {"name": "Query", "width": None},
    }

    metadata_locks_datatable = tab.metadata_locks_datatable

    if not metadata_locks_datatable.columns:
        for column_key, column_data in columns.items():
            column_name = column_data["name"]
            metadata_locks_datatable.add_column(column_name, key=column_name, width=column_data["width"])

    for lock in dolphie.metadata_locks:
        lock_id = str(lock["id"])
        row_values = []
        row_height = 1

        for column_id, (column_key, column_data) in enumerate(columns.items()):
            column_name = column_data["name"]
            column_value = lock[column_key]

            # Get height of row based on the how many objects are in the OBJECT_NAME field
            if (
                column_key == "OBJECT_NAME"
                and column_value
                and len(column_value) > column_data["width"]
                and "," in column_value
            ):
                # Truncate the object names to the width of the column
                object_names = [object_name[: column_data["width"]] for object_name in column_value.split(",")]
                thread_value = "\n".join(object_names)

                row_height = len(object_names)
            else:
                thread_value = format_value(lock, column_key, column_value)

            if lock_id in metadata_locks_datatable.rows:
                datatable_value = metadata_locks_datatable.get_row(lock_id)[column_id]

                temp_thread_value = thread_value
                temp_datatable_value = datatable_value

                # If the column is the query, we need to compare the code of the Syntax object
                update_width = False
                if column_key == "PROCESSLIST_INFO":
                    update_width = True

                    if isinstance(thread_value, Syntax):
                        temp_thread_value = thread_value.code
                    if isinstance(datatable_value, Syntax):
                        temp_datatable_value = datatable_value.code

                # Update the datatable if values differ
                if temp_thread_value != temp_datatable_value:
                    metadata_locks_datatable.update_cell(lock_id, column_name, thread_value, update_width=update_width)
            else:
                # Create an array of values to append to the datatable
                row_values.append(thread_value)

        # Add a new row to the datatable
        if row_values:
            metadata_locks_datatable.add_row(*row_values, key=lock_id, height=row_height)

    # Find the ids that exist in datatable but not in metadata_locks
    if dolphie.metadata_locks:
        rows_to_remove = set(metadata_locks_datatable.rows.keys()) - {
            str(lock["id"]) for lock in dolphie.metadata_locks
        }
        for id in rows_to_remove:
            metadata_locks_datatable.remove_row(id)
    else:
        if metadata_locks_datatable.row_count:
            metadata_locks_datatable.clear()

    metadata_locks_datatable.sort("Age", reverse=dolphie.sort_by_time_descending)

    tab.metadata_locks_title.update(f"Metadata Locks ([highlight]{metadata_locks_datatable.row_count}[/highlight])")


def fetch_data(tab: Tab) -> Dict[str, str]:
    dolphie = tab.dolphie

    ########################
    # WHERE clause filters #
    ########################
    where_clause = []

    # Filter user
    if dolphie.user_filter:
        where_clause.append("processlist_user = '%s'" % dolphie.user_filter)

    # Filter database
    if dolphie.db_filter:
        where_clause.append("processlist_db = '%s'" % dolphie.db_filter)

    # Filter hostname/IP
    if dolphie.host_filter:
        # Have to use LIKE since there's a port at the end
        where_clause.append("processlist_host LIKE '%s%%'" % dolphie.host_filter)

    # Filter time
    if dolphie.query_time_filter:
        where_clause.append("processlist_time >= '%s'" % dolphie.query_time_filter)

    # Filter query
    if dolphie.query_filter:
        where_clause.append("(processlist_info LIKE '%%%s%%')" % (dolphie.query_filter))

    if where_clause:
        # Add in our dynamic WHERE clause for filtering
        query = MySQLQueries.metadata_locks.replace("$1", "AND " + " AND ".join(where_clause))
    else:
        query = MySQLQueries.metadata_locks.replace("$1", "")

    dolphie.main_db_connection.execute(query)
    threads = dolphie.main_db_connection.fetchall()

    return threads


def format_value(lock: dict, column_key: str, value: str) -> str:
    formatted_value = value

    # OBJECT_NAME is in the format "schema/table" sometimes where OBJECT_SCHEMA is empty,
    # so I want to split OBJECT_NAME and correct it if necessary
    if column_key == "OBJECT_SCHEMA" and not value and lock["OBJECT_NAME"] and "/" in lock["OBJECT_NAME"]:
        formatted_value = lock["OBJECT_NAME"].split("/")[0]
    elif column_key == "OBJECT_NAME" and value and "/" in value:
        formatted_value = value.split("/")[1]
    elif value is None or value == "":
        formatted_value = "[dark_gray]N/A"
    elif column_key == "PROCESSLIST_INFO":
        formatted_value = format_query(value)
    elif column_key == "LOCK_STATUS":
        if value == "GRANTED":
            formatted_value = f"[green]{value}[/green]"
        elif value == "PENDING":
            formatted_value = f"[red]{value}[/red]"
    elif column_key == "LOCK_TYPE":
        if value == "EXCLUSIVE":
            formatted_value = f"[yellow]{value}[/yellow]"
    elif column_key == "PROCESSLIST_TIME":
        formatted_value = format_time(value)
    elif column_key == "CODE_SOURCE":
        formatted_value = value.split(":")[0]
    elif column_key == "THREAD_SOURCE":
        formatted_value = value.split("/")[-1]

        if formatted_value == "one_connection":
            formatted_value = "user_connection"

    return formatted_value
