from re import sub

from dolphie import Dolphie
from dolphie.Modules.Functions import format_number
from textual.widgets import DataTable


def create_panel(dolphie: Dolphie) -> DataTable:
    locks_data = {}

    for lock in dolphie.lock_transactions:
        key = f"{lock['waiting_trx_id']}-{lock['blocking_trx_id']}"
        locks_data[key] = {
            "lock_age": lock["wait_age"],
            "lock_type": lock["locked_type"],
            "w_thread": lock["waiting_trx_id"],
            "w_query": sub(r"\s+", " ", lock["waiting_query"]),
            "w_rows_modified": lock["waiting_trx_rows_modified"],
            "w_rows_locked": lock["waiting_trx_rows_locked"],
            "w_age": lock["waiting_trx_age"],
            "w_lock_mode": lock["waiting_lock_mode"],
            "b_thread": lock["blocking_trx_id"],
            "b_query": sub(r"\s+", " ", lock["blocking_query"]),
            "b_rows_modified": lock["blocking_trx_rows_modified"],
            "b_rows_locked": lock["blocking_trx_rows_locked"],
            "b_age": lock["blocking_trx_age"],
            "b_lock_mode": lock["blocking_lock_mode"],
        }

    columns = {
        "lock_age": {"name": "Lock Age", "width": 8, "format_number": False},
        "lock_type": {"name": "Lock Type", "width": 10, "format_number": False},
        "w_thread": {"name": "[yellow]Waiting TRX[/yellow]", "width": 13, "format_number": False},
        "w_age": {"name": "Age", "width": 8, "format_number": False},
        "w_lock_mode": {"name": "Mode", "width": 5, "format_number": False},
        "w_rows_locked": {"name": "R-Lock", "width": 6, "format_number": True},
        "w_rows_modified": {"name": "R-Mod", "width": 6, "format_number": True},
        "w_query": {"name": "Query", "width": None, "format_number": False},
        "b_thread": {"name": "[red]Blocking TRX[/red]", "width": 13, "format_number": False},
        "b_age": {"name": "Age", "width": 8, "format_number": False},
        "b_lock_mode": {"name": "Mode", "width": 5, "format_number": False},
        "b_rows_locked": {"name": "R-Lock", "width": 6, "format_number": True},
        "b_rows_modified": {"name": "R-Mod", "width": 6, "format_number": True},
        "b_query": {"name": "Query", "width": None, "format_number": False},
    }

    # Hacky way to calculate the width of the query columns
    query_characters = round((dolphie.app.console.size.width / 2) - ((len(columns) * 4) + 6))
    columns["w_query"]["width"] = query_characters
    columns["b_query"]["width"] = query_characters

    locks_datatable = dolphie.app.query_one("#panel_locks", DataTable)
    locks_datatable.clear(columns=True)

    for column_key, column_data in columns.items():
        locks_datatable.add_column(column_data["name"], key=column_key, width=column_data["width"])

    for key, transaction in locks_data.items():
        row_values = []
        for column_key, column_data in columns.items():
            column_name = column_data["name"]
            column_format_number = column_data["format_number"]

            if column_name == "Query":
                value = sub(r"\s+", " ", transaction[column_key][0:query_characters])
            else:
                if column_format_number:
                    value = format_number(transaction[column_key])
                else:
                    value = transaction[column_key]

            row_values.append(value)

        locks_datatable.add_row(*row_values, key=key)
