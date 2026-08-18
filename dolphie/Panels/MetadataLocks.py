from __future__ import annotations

from collections.abc import Mapping

from rich.syntax import Syntax

from dolphie.DataTypes import DatabaseRow
from dolphie.Modules.Functions import coerce_int, coerce_str, filter_sql_condition, format_query, format_time
from dolphie.Modules.Queries import MySQLQueries
from dolphie.Modules.TabManager import Tab


def create_panel(tab: Tab) -> None:
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

    # Refresh optimization
    query_length_max = 300
    metadata_locks_datatable = tab.metadata_locks_datatable

    column_keys = []
    column_names = []
    column_widths = []

    if not metadata_locks_datatable.columns:
        for column_key, column_data in columns.items():
            metadata_locks_datatable.add_column(
                column_data["name"], key=column_data["name"], width=column_data["width"]
            )

    for column_key, column_data in columns.items():
        column_keys.append(column_key)
        column_names.append(column_data["name"])
        column_widths.append(column_data["width"])

    changed = False

    with dolphie.app.batch_update():
        # Remove stale rows first
        if dolphie.metadata_locks:
            rows_to_remove = set(metadata_locks_datatable.rows.keys()) - {
                str(lock["id"]) for lock in dolphie.metadata_locks
            }
            if rows_to_remove:
                changed = True
                if len(rows_to_remove) > len(dolphie.metadata_locks):
                    metadata_locks_datatable.clear()
                else:
                    for id in rows_to_remove:
                        metadata_locks_datatable.remove_row(id)
        else:
            if metadata_locks_datatable.row_count:
                changed = True
                metadata_locks_datatable.clear()

        for lock in dolphie.metadata_locks:
            lock_id = str(lock["id"])
            row_height = 1

            row_values = []
            for column_key, column_name, column_width in zip(column_keys, column_names, column_widths, strict=True):
                column_value = lock[column_key]
                column_text = coerce_str(column_value)

                # Get height of row based on the how many objects are in the OBJECT_NAME field
                if (
                    column_key == "OBJECT_NAME"
                    and column_text
                    and len(column_text) > column_width
                    and "," in column_text
                ):
                    object_names = [object_name[:column_width] for object_name in column_text.split(",")]
                    value = "\n".join(object_names)
                    row_height = len(object_names)
                else:
                    value = format_value(lock, column_key, column_value)

                # Truncate query Syntax objects
                if column_key == "PROCESSLIST_INFO" and isinstance(value, Syntax):
                    value = format_query(value.code[:query_length_max])

                row_values.append(value)

            row_values = metadata_locks_datatable.normalize_cells(row_values)
            if lock_id in metadata_locks_datatable.rows:
                datatable_row = metadata_locks_datatable.get_row(lock_id)

                for column_id, column_name in enumerate(column_names):
                    new_val = row_values[column_id]
                    old_val = datatable_row[column_id]

                    # Compare text content for Syntax objects
                    cmp_new = new_val.code if isinstance(new_val, Syntax) else new_val
                    cmp_old = old_val.code if isinstance(old_val, Syntax) else old_val

                    if cmp_new != cmp_old:
                        changed = True
                        metadata_locks_datatable.update_cell(lock_id, column_name, new_val)
            else:
                changed = True
                metadata_locks_datatable.add_row(*row_values, key=lock_id, height=row_height)

        if changed:
            metadata_locks_datatable.sort("Age", reverse=dolphie.sort_by_time_descending)

    tab.metadata_locks_title.update(
        f"{dolphie.panels.metadata_locks.title} ([$highlight]{metadata_locks_datatable.row_count}[/$highlight])"
    )


def fetch_data(tab: Tab) -> list[DatabaseRow]:
    dolphie = tab.dolphie

    ########################
    # WHERE clause filters #
    ########################
    where_clause = []

    # Filter user
    if dolphie.user_filter:
        where_clause.append(filter_sql_condition("processlist_user", dolphie.user_filter))

    # Filter database
    if dolphie.db_filter:
        where_clause.append(filter_sql_condition("processlist_db", dolphie.db_filter))

    # Filter hostname/IP
    if dolphie.host_filter:
        # Have to use LIKE since there's a port at the end
        where_clause.append(filter_sql_condition("processlist_host", dolphie.host_filter, "{}%"))

    # Filter time
    if dolphie.query_time_filter:
        where_clause.append(f"processlist_time >= '{dolphie.query_time_filter}'")

    # Filter query
    if dolphie.query_filter:
        where_clause.append(filter_sql_condition("processlist_info", dolphie.query_filter, "%{}%"))

    if where_clause:
        # Add in our dynamic WHERE clause for filtering
        query = MySQLQueries.metadata_locks.replace("$1", "AND " + " AND ".join(where_clause))
    else:
        query = MySQLQueries.metadata_locks.replace("$1", "")

    dolphie.main_db_connection.execute(query)
    threads = dolphie.main_db_connection.fetchall()

    return threads


def format_value(lock: Mapping[str, object], column_key: str, value: object) -> str | Syntax:
    value_text = coerce_str(value)
    object_name = coerce_str(lock.get("OBJECT_NAME"))
    formatted_value: str | Syntax = value_text

    # OBJECT_NAME is in the format "schema/table" sometimes where OBJECT_SCHEMA is empty,
    # so I want to split OBJECT_NAME and correct it if necessary
    if column_key == "OBJECT_SCHEMA" and not value_text and "/" in object_name:
        formatted_value = object_name.split("/")[0]
    elif column_key == "OBJECT_NAME" and value_text and "/" in value_text:
        formatted_value = value_text.split("/")[1]
    elif value is None or value == "":
        formatted_value = "[$dark_gray]N/A"
    elif column_key == "PROCESSLIST_INFO":
        formatted_value = format_query(value_text)
    elif column_key == "LOCK_STATUS":
        if value_text == "GRANTED":
            formatted_value = f"[$green]{value_text}[/$green]"
        elif value_text == "PENDING":
            formatted_value = f"[$red]{value_text}[/$red]"
    elif column_key == "LOCK_TYPE":
        if value_text == "EXCLUSIVE":
            formatted_value = f"[$yellow]{value_text}[/$yellow]"
    elif column_key == "PROCESSLIST_TIME":
        formatted_value = format_time(coerce_int(value))
    elif column_key == "CODE_SOURCE":
        formatted_value = value_text.split(":")[0]
    elif column_key == "THREAD_SOURCE":
        formatted_value = value_text.split("/")[-1]

        if formatted_value == "one_connection":
            formatted_value = "user_connection"

    return formatted_value
