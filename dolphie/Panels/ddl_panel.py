from dolphie.Modules.Functions import format_bytes, format_time
from dolphie.Modules.TabManager import Tab
from textual.widgets import DataTable


def create_panel(tab: Tab) -> DataTable:
    dolphie = tab.dolphie

    columns = {
        "processlist_id": {"name": "Thread ID", "width": 11, "format": None},
        "percentage_completed": {"name": "Completed", "width": 9, "format": None},
        "memory": {"name": "Memory", "width": 10, "format": "bytes"},
        "started_ago": {"name": "Current Time", "width": 12, "format": "time"},
        "estimated_remaining_time": {"name": "Remaining Time", "width": 14, "format": "time"},
        "state": {"name": "State", "width": None, "format": None},
    }

    ddl_datatable = tab.ddl_datatable
    ddl_datatable.clear(columns=True)

    for column_key, column_data in columns.items():
        ddl_datatable.add_column(column_data["name"], key=column_key, width=column_data["width"])

    for ddl in dolphie.ddl:
        row_values = []

        for column_key, column_data in columns.items():
            column_format = column_data["format"]

            if column_format == "time":
                value = format_time(ddl[column_key], picoseconds=True)
            elif column_format == "bytes":
                value = format_bytes(ddl[column_key])
            else:
                value = ddl[column_key]

            row_values.append(value)

        ddl_datatable.add_row(*row_values, key=ddl["processlist_id"])

    tab.ddl_title.update(f"DDL ([highlight]{ddl_datatable.row_count}[/highlight])")
