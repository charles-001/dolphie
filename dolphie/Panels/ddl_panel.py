from dolphie.Modules.Functions import format_time
from dolphie.Modules.TabManager import Tab
from textual.widgets import DataTable


def create_panel(tab: Tab) -> DataTable:
    dolphie = tab.dolphie

    columns = {
        "processlist_id": {"name": "Thread ID", "width": 10, "format_time": False},
        "percentage_completed": {"name": "Completed", "width": 9, "format_time": False},
        "memory": {"name": "Memory", "width": 10, "format_time": False},
        "started_ago": {"name": "Age", "width": 8, "format_time": True},
        "estimated_remaining_time": {"name": "ETA", "width": 8, "format_time": True},
        "state": {"name": "State", "width": 60, "format_time": False},
    }

    ddl_datatable = tab.ddl_datatable
    ddl_datatable.clear(columns=True)

    for column_key, column_data in columns.items():
        ddl_datatable.add_column(column_data["name"], key=column_key, width=column_data["width"])

    for ddl in dolphie.ddl:
        row_values = []

        for column_key, column_data in columns.items():
            column_format_time = column_data["format_time"]

            if column_format_time:
                value = format_time(ddl[column_key], picoseconds=True)
            else:
                value = ddl[column_key]

            row_values.append(value)

        ddl_datatable.add_row(*row_values, key=ddl["processlist_id"])

    tab.ddl_title.update(f"DDL ([highlight]{len(dolphie.ddl)}[/highlight])")
