from re import sub

from dolphie.Modules.Functions import format_number
from dolphie.Modules.TabManager import Tab
from rich.markup import escape as markup_escape
from textual.widgets import DataTable


def create_panel(tab: Tab) -> DataTable:
    dolphie = tab.dolphie

    columns = {
        "wait_age": {"name": "Lock Age", "width": 8, "format_number": False},
        "locked_type": {"name": "Lock Type", "width": 10, "format_number": False},
        "waiting_pid": {"name": "[highlight]Waiting PID[/highlight]", "width": 13, "format_number": False},
        "waiting_trx_age": {"name": "Age", "width": 8, "format_number": False},
        "waiting_lock_mode": {"name": "Mode", "width": 5, "format_number": False},
        "waiting_trx_rows_locked": {"name": "R-Lock", "width": 6, "format_number": True},
        "waiting_trx_rows_modified": {"name": "R-Mod", "width": 6, "format_number": True},
        "waiting_query": {"name": "Query", "width": None, "format_number": False},
        "blocking_pid": {"name": "[red]Blocking PID[/red]", "width": 13, "format_number": False},
        "blocking_trx_age": {"name": "Age", "width": 8, "format_number": False},
        "blocking_lock_mode": {"name": "Mode", "width": 5, "format_number": False},
        "blocking_trx_rows_locked": {"name": "R-Lock", "width": 6, "format_number": True},
        "blocking_trx_rows_modified": {"name": "R-Mod", "width": 6, "format_number": True},
        "blocking_query": {"name": "Query", "width": None, "format_number": False},
    }

    # Hacky way to calculate the width of the query columns
    query_characters = round((dolphie.app.console.size.width / 2) - ((len(columns) * 4) + 4))
    columns["waiting_query"]["width"] = query_characters
    columns["blocking_query"]["width"] = query_characters

    locks_datatable = tab.innodb_trx_locks_datatable
    locks_datatable.clear(columns=True)

    for column_key, column_data in columns.items():
        locks_datatable.add_column(column_data["name"], width=column_data["width"])

    for lock in dolphie.lock_transactions:
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

        locks_datatable.add_row(*row_values)

    tab.innodb_trx_locks_title.update(
        f"InnoDB Transaction Locks ([highlight]{len(dolphie.lock_transactions)}[/highlight])"
    )
