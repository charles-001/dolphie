from re import sub

from dolphie import Dolphie
from dolphie.Modules.Functions import format_number
from textual.widgets import DataTable


def create_panel(dolphie: Dolphie) -> DataTable:
    locks_data = {}

    for lock in dolphie.lock_transactions:
        key = f"{lock['waiting_thread']}-{lock['blocking_thread']}"
        locks_data[key] = {
            "w_thread": lock["waiting_thread"],
            "w_query": sub(r"\s+", " ", lock["waiting_query"]),
            "w_rows_modified": lock["waiting_rows_modified"],
            "w_age": lock["waiting_age"],
            "w_wait_secs": lock["waiting_wait_secs"],
            "b_thread": lock["blocking_thread"],
            "b_query": sub(r"\s+", " ", lock["blocking_query"]),
            "b_rows_modified": lock["blocking_rows_modified"],
            "b_age": lock["blocking_age"],
            "b_wait_secs": lock["blocking_wait_secs"],
            "lock_mode": lock["lock_mode"],
            "lock_type": lock["lock_type"],
        }

    columns = {
        "w_thread": {"name": "[yellow]Waiting TRX[/yellow]", "width": 13, "format_number": False},
        "w_age": {"name": "Age", "width": 4, "format_number": False},
        "w_wait_secs": {"name": "Wait", "width": 5, "format_number": False},
        "w_rows_modified": {"name": "Rows Mod", "width": 8, "format_number": True},
        "w_query": {"name": "Query", "width": None, "format_number": False},
        "b_thread": {"name": "[red]Blocking TRX[/red]", "width": 13, "format_number": False},
        "b_age": {"name": "Age", "width": 4, "format_number": False},
        "b_wait_secs": {"name": "Wait", "width": 5, "format_number": False},
        "b_rows_modified": {"name": "Rows Mod", "width": 8, "format_number": True},
        "b_query": {"name": "Query", "width": None, "format_number": False},
        "lock_mode": {"name": "Mode", "width": 7, "format_number": False},
        "lock_type": {"name": "Type", "width": 8, "format_number": False},
    }

    # Hacky way to calculate the width of the query columns
    query_characters = round((dolphie.app.console.size.width / 2) - ((len(columns) * 4) + 4))
    columns["w_query"]["width"] = query_characters
    columns["b_query"]["width"] = query_characters

    locks_datatable = dolphie.app.query_one("#panel_locks", DataTable)
    locks_datatable.clear(columns=True)

    for column_key, column_data in columns.items():
        locks_datatable.add_column(column_data["name"], key=column_key, width=column_data["width"])

    for key, transaction in locks_data.items():
        row_values = []
        for column_key, column_data in columns.items():
            column_format_number = column_data["format_number"]

            if "query" in column_key:
                value = sub(r"\s+", " ", transaction[column_key][0:query_characters])
            else:
                value = format_number(transaction[column_key]) if column_format_number else transaction[column_key]

            row_values.append(value)

        locks_datatable.add_row(*row_values, key=key)
