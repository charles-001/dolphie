import os

from textual.widgets import DataTable

from dolphie.Modules.Functions import format_bytes, format_number, format_time
from dolphie.Modules.TabManager import Tab


def create_panel(tab: Tab) -> DataTable:
    dolphie = tab.dolphie

    if not dolphie.file_io_by_instance_tracker:
        return

    columns = {
        "Instance": {"field": "FILE_NAME", "width": None},
        "Latency": {"field": "Latency", "width": 10, "format": "time"},
        "Read Ops": {"field": "ReadOps", "width": 10, "format": "number"},
        "Write Ops": {"field": "WriteOps", "width": 10, "format": "number"},
        "Misc Ops": {"field": "MiscOps", "width": 10, "format": "number"},
        "Read Bytes": {"field": "ReadBytes", "width": 10, "format": "bytes"},
        "Write Bytes": {"field": "WriteBytes", "width": 11, "format": "bytes"},
        "latency_ps": {"field": "Latency", "width": 0},
    }

    # Get the DataTable object
    datatable = tab.performance_schema_metrics_datatable

    # Add columns to the datatable if it is empty
    if not datatable.columns:
        for column_key, column_data in columns.items():
            column_width = column_data["width"]
            datatable.add_column(column_key, key=column_key, width=column_width)

    data = dolphie.file_io_by_instance_tracker.filtered_data
    for file_name, metrics in data.items():
        row_id = file_name
        row_values = []

        match = dolphie.file_io_by_instance_tracker.combined_table_pattern.search(file_name)
        if file_name.endswith("mysql.ibd"):
            file_name = f"[dark_gray]{os.path.dirname(file_name)}[/dark_gray]/{os.path.basename(file_name)}"
        elif match:
            file_name = f"{match.group(1)}.{match.group(2)}"
        elif "/" in file_name:
            file_name = f"[dark_gray]{os.path.dirname(file_name)}[/dark_gray]/{os.path.basename(file_name)}"
        else:
            file_name = f"[b][light_blue]*[/light_blue][/b] [highlight]{file_name}"

        row_values.append(file_name)

        # Check if row already exists before processing columns
        if row_id in datatable.rows:
            row_exists = True
        else:
            row_exists = False

        for column_id, (column_name, column_data) in enumerate(columns.items()):
            if column_name == "Instance":
                continue

            column_value = metrics.get(column_data["field"], {})
            if tab.performance_schema_metrics_radio_set.pressed_button.id == "total":
                column_value = column_value.get("total", None)
            else:
                column_value = column_value.get("delta", None)

            # Handle special formatting
            if column_value == 0 or column_value is None:
                if column_name == "latency_ps":
                    column_value = 0
                else:
                    column_value = "[dark_gray]0"
            elif column_data.get("format") == "time":
                column_value = format_time(column_value, picoseconds=True)
            elif column_data.get("format") == "number":
                column_value = format_number(column_value)
            elif column_data.get("format") == "bytes":
                column_value = format_bytes(column_value)

            if row_exists:
                # Check and update only if the value has changed
                current_value = datatable.get_row(row_id)[column_id]
                if column_value != current_value:
                    datatable.update_cell(row_id, column_name, column_value)
            else:
                # Add new row values
                row_values.append(column_value)

        # Add the row if it's new
        if not row_exists and row_values:
            datatable.add_row(*row_values, key=row_id)

    # Clean up rows that no longer exist in the data
    if data:
        current_rows = set(data.keys())
        existing_rows = set(datatable.rows.keys())

        rows_to_remove = existing_rows - current_rows
        for row_id in rows_to_remove:
            datatable.remove_row(row_id)
    else:
        if datatable.row_count:
            datatable.clear()

    datatable.sort("latency_ps", reverse=True)

    # Update the title to reflect the number of active rows
    tab.performance_schema_metrics_title.update(f"File IO by Instance ([highlight]{datatable.row_count}[/highlight])")
