from re import sub

from dolphie.Modules.Functions import format_time
from dolphie.Modules.TabManager import Tab
from rich.markup import escape as markup_escape
from textual.widgets import DataTable


def create_panel(tab: Tab) -> DataTable:
    dolphie = tab.dolphie

    columns = {
        "OBJECT_TYPE": {"name": "Object Type", "width": 13},
        "OBJECT_SCHEMA": {"name": "Object Schema", "width": 13},
        "OBJECT_NAME": {"name": "Object Name", "width": 15},
        "LOCK_TYPE": {"name": "Lock Type", "width": 20},
        "LOCK_STATUS": {"name": "Lock Status", "width": 11},
        "SOURCE": {"name": "Source", "width": 15},
        "PROCESSLIST_ID": {"name": "PID", "width": 13},
        "PROCESSLIST_USER": {"name": "User", "width": 13},
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

        for column_id, (column_key, column_data) in enumerate(columns.items()):
            column_name = column_data["name"]

            value = format_value(lock, column_key, lock[column_key])
            if lock_id in metadata_locks_datatable.rows:
                # Update the datatable if values differ
                if value != metadata_locks_datatable.get_row(lock_id)[column_id]:
                    metadata_locks_datatable.update_cell(lock_id, column_name, value, update_width=True)
            else:
                # Create an array of values to append to the datatable
                row_values.append(value)

        # Add a new row to the datatable
        if row_values:
            metadata_locks_datatable.add_row(*row_values, key=lock_id)

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

    tab.metadata_locks_title.update(f"Metadata Locks ([highlight]{len(dolphie.metadata_locks)}[/highlight])")


def format_value(lock, column_key, value: str) -> str:
    # OBJECT_NAME is in the format "schema/table" sometimes where OBJECT_SCHEMA is empty,
    # so I want to split OBJECT_NAME and correct it if necessary
    if column_key == "OBJECT_SCHEMA" and not value and lock["OBJECT_NAME"] and "/" in lock["OBJECT_NAME"]:
        formatted_value = lock["OBJECT_NAME"].split("/")[0]
    elif column_key == "OBJECT_NAME" and value and "/" in value:
        formatted_value = value.split("/")[1]
    elif value is None or value == "":
        formatted_value = "N/A"
    elif column_key == "PROCESSLIST_INFO":
        if value:
            formatted_value = markup_escape(sub(r"\s+", " ", value))
        else:
            formatted_value = ""
    elif column_key == "LOCK_STATUS":
        if value == "GRANTED":
            formatted_value = f"[green]{value}[/green]"
        elif value == "PENDING":
            formatted_value = f"[red]{value}[/red]"
        else:
            formatted_value = value
    elif column_key == "LOCK_TYPE":
        if value == "EXCLUSIVE":
            formatted_value = f"[yellow]{value}[/yellow]"
        else:
            formatted_value = value
    elif column_key == "PROCESSLIST_TIME":
        formatted_value = format_time(value)
    elif column_key == "SOURCE":
        formatted_value = value.split(":")[0]
    else:
        formatted_value = value

    return formatted_value
