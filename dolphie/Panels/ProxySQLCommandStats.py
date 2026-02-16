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

    column_keys = []
    column_names = []
    column_formats = []

    # Add columns to the datatable if it is empty
    if not command_stats.columns:
        for column_key, column_data in columns.items():
            command_stats.add_column(column_data["name"], key=column_key, width=column_data["width"])

    for column_key, column_data in columns.items():
        column_keys.append(column_key)
        column_names.append(column_data["name"])
        column_formats.append(column_data["format"])

    polling_latency = dolphie.polling_latency

    with dolphie.app.batch_update():
        # Remove stale rows first
        if dolphie.proxysql_command_stats:
            current_rows = {row["Command"] for row in dolphie.proxysql_command_stats}
            existing_rows = set(command_stats.rows.keys())

            rows_to_remove = existing_rows - current_rows
            if rows_to_remove:
                if len(rows_to_remove) > len(dolphie.proxysql_command_stats):
                    command_stats.clear()
                else:
                    for row_id in rows_to_remove:
                        command_stats.remove_row(row_id)
        else:
            if command_stats.row_count:
                command_stats.clear()

        for row in dolphie.proxysql_command_stats:
            row_id = row["Command"]

            row_values = []
            for column_key, column_format in zip(column_keys, column_formats):
                column_value = row.get(column_key)

                # Calculate the values per second for the following columns
                if "cnt_" in column_key:
                    current_value = int(column_value or 0)
                    previous_value = dolphie.proxysql_per_second_data.get(row_id, {}).get(column_key, 0)
                    if not previous_value:
                        column_value = 0
                    else:
                        value_diff = current_value - previous_value
                        column_value = round(value_diff / polling_latency) if polling_latency > 0 else 0

                    dolphie.proxysql_per_second_data.setdefault(row_id, {})[column_key] = current_value

                if column_format == "number":
                    column_value = format_number(column_value)

                if column_value == "0":
                    column_value = "[dark_gray]0"

                row_values.append(column_value)

            if row_id in command_stats.rows:
                datatable_row = command_stats.get_row(row_id)

                for column_id, column_key in enumerate(column_keys):
                    if row_values[column_id] != datatable_row[column_id]:
                        command_stats.update_cell(row_id, column_key, row_values[column_id])
            else:
                command_stats.add_row(*row_values, key=row_id)

    tab.proxysql_command_stats_title.update(
        f"{dolphie.panels.proxysql_command_stats.title} " f"([$highlight]{command_stats.row_count}[/$highlight])"
    )
