from dolphie.Modules.Functions import format_number
from dolphie.Modules.TabManager import Tab
from textual.widgets import DataTable


def create_panel(tab: Tab) -> DataTable:
    dolphie = tab.dolphie

    columns = {
        "Command": {"name": "Command", "width": None, "format": None},
        "Total_cnt": {"name": "Total", "width": 10, "format": "number"},
        "Total_cnt_s": {"name": "Total/s", "width": 10, "format": "number"},
        "cnt_100us": {"name": "100μs/s", "width": 8, "format": "number"},
        "cnt_500us": {"name": "500μs/s", "width": 8, "format": "number"},
        "cnt_1ms": {"name": "1ms/s", "width": 8, "format": "number"},
        "cnt_5ms": {"name": "5ms/s", "width": 8, "format": "number"},
        "cnt_10ms": {"name": "10ms/s", "width": 8, "format": "number"},
        "cnt_50ms": {"name": "50ms/s", "width": 8, "format": "number"},
        "cnt_100ms": {"name": "100ms/s", "width": 8, "format": "number"},
        "cnt_500ms": {"name": "500ms/s", "width": 8, "format": "number"},
        "cnt_1s": {"name": "1s/s", "width": 8, "format": "number"},
        "cnt_5s": {"name": "5s/s", "width": 8, "format": "number"},
        "cnt_10s": {"name": "10s/s", "width": 8, "format": "number"},
        "cnt_INFs": {"name": "10s+/s", "width": 8, "format": "number"},
    }

    command_stats = tab.proxysql_command_stats_datatable

    # Add columns to the datatable if it is empty
    if not command_stats.columns:
        for column_key, column_data in columns.items():
            column_name = column_data["name"]
            column_width = column_data["width"]
            command_stats.add_column(column_name, key=column_key, width=column_width)

    for row in dolphie.proxysql_command_stats:
        row_id = row["Command"]
        row_values = []

        for column_id, (column_key, column_data) in enumerate(columns.items()):
            column_name = column_data["name"]
            column_format = column_data["format"]
            column_value = row.get(column_key)

            # Calculate the values per second for the following columns
            if "cnt_" in column_key:
                if not dolphie.proxysql_per_second_data.get(row_id, {}).get(column_key, 0):
                    column_value = 0
                else:
                    value_diff = int(column_value) - dolphie.proxysql_per_second_data.get(row_id, {}).get(column_key, 0)
                    column_value = round(value_diff / dolphie.polling_latency)

                dolphie.proxysql_per_second_data.setdefault(row_id, {})[column_key] = int(row.get(column_key, 0))

            if column_format == "number":
                column_value = format_number(column_value)

            if column_value == "0":
                column_value = "[dark_gray]0"

            if row_id in command_stats.rows:
                datatable_value = command_stats.get_row(row_id)[column_id]

                # Update the datatable if values differ
                if column_value != datatable_value:
                    command_stats.update_cell(row_id, column_key, column_value)
            else:
                # Create an array of values to append to the datatable
                row_values.append(column_value)

        # Add a new row to the datatable
        if row_values:
            command_stats.add_row(*row_values, key=row_id)

    # Remove rows from datatable that no longer exist in the data
    if dolphie.proxysql_command_stats:
        current_rows = {row["Command"] for row in dolphie.proxysql_command_stats}
        existing_rows = set(command_stats.rows.keys())

        rows_to_remove = existing_rows - current_rows
        for row_id in rows_to_remove:
            command_stats.remove_row(row_id)
    else:
        if command_stats.row_count:
            command_stats.clear()

    tab.proxysql_command_stats_title.update(f"Command Statistics ([highlight]{command_stats.row_count}[/highlight])")
