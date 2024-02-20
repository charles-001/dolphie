from re import sub

from dolphie.Modules.Functions import format_number
from dolphie.Modules.TabManager import Tab
from rich.markup import escape as markup_escape
from textual.widgets import DataTable


def create_panel(tab: Tab) -> DataTable:
    dolphie = tab.dolphie

    columns = {
        "object_type": {"name": "Object Type", "width": 11, "format_number": False},
        "object_name": {"name": "Object Name", "width": 20, "format_number": False},
        "lock_type": {"name": "Lock Type", "width": 20, "format_number": False},
        "waiting_pid": {"name": "[highlight]Waiting PID[/highlight]", "width": 13, "format_number": False},
        "waiting_time": {"name": "Age", "width": 8, "format_number": False},
        "waiting_query": {"name": "Query", "width": None, "format_number": False},
        "blocking_pid": {"name": "[red]Blocking PID[/red]", "width": 13, "format_number": False},
        "blocking_time": {"name": "Age", "width": 8, "format_number": False},
        "blocking_query": {"name": "Query", "width": None, "format_number": False},
    }

    # Hacky way to calculate the width of the query columns
    query_characters = round((dolphie.app.console.size.width / 2) - ((len(columns) * 6) + 3))
    columns["waiting_query"]["width"] = query_characters
    columns["blocking_query"]["width"] = query_characters

    metadata_locks_datatable = tab.metadata_locks_datatable
    metadata_locks_datatable.clear(columns=True)

    for column_key, column_data in columns.items():
        metadata_locks_datatable.add_column(column_data["name"], key=column_key, width=column_data["width"])

    for lock in dolphie.metadata_locks:
        row_values = []

        for column_key, column_data in columns.items():
            column_name = column_data["name"]
            column_format_number = column_data["format_number"]

            if column_name == "Query":
                if lock[column_key]:
                    value = markup_escape(sub(r"\s+", " ", lock[column_key][0:query_characters]))
                else:
                    value = ""
            else:
                if column_format_number:
                    value = format_number(lock[column_key])
                else:
                    value = lock[column_key]

            row_values.append(value)

        lock_key = f"{lock['waiting_pid']}-{lock['blocking_pid']}"
        metadata_locks_datatable.add_row(*row_values, key=lock_key)

    tab.metadata_locks_title.update(f"Metadata Locks ([highlight]{len(dolphie.metadata_locks)}[/highlight])")
