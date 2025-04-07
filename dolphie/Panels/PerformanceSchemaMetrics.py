import os
from datetime import datetime

from textual.widgets import DataTable

from dolphie.Modules.Functions import format_bytes, format_number, format_time
from dolphie.Modules.TabManager import Tab


def create_panel(tab: Tab):
    dolphie = tab.dolphie

    update_file_io_by_instance(tab)
    update_table_io_waits_summary_by_table(tab)

    if dolphie.replay_file:
        time = dolphie.global_status.get("replay_pfs_metrics_last_reset_time", 0)
    else:
        time = (
            (datetime.now() - dolphie.pfs_metrics_last_reset_time).total_seconds()
            if dolphie.pfs_metrics_last_reset_time
            else 0
        )
    tab.pfs_metrics_delta.label = f"Delta since last reset ([$light_blue]{format_time(time)}[/$light_blue])"


def update_table_io_waits_summary_by_table(tab: Tab) -> DataTable:
    dolphie = tab.dolphie
    datatable = tab.pfs_metrics_table_io_waits_datatable

    if not dolphie.table_io_waits_data or not dolphie.table_io_waits_data.filtered_data:
        datatable.display = False
        tab.pfs_metrics_tabs.get_tab("pfs_metrics_table_io_waits_tab").label = (
            "Table I/O Waits ([$highlight]0[/$highlight])"
        )

        return

    datatable.display = True

    columns = {
        "Table": {"field": "TABLE_NAME", "width": None},
        "Total": {"field": ["COUNT_STAR", "SUM_TIMER_WAIT"], "width": 23},
        "Fetch": {"field": ["COUNT_FETCH", "SUM_TIMER_FETCH"], "width": 23},
        "Insert": {"field": ["COUNT_INSERT", "SUM_TIMER_INSERT"], "width": 23},
        "Update": {"field": ["COUNT_UPDATE", "SUM_TIMER_UPDATE"], "width": 23},
        "Delete": {"field": ["COUNT_DELETE", "SUM_TIMER_DELETE"], "width": 23},
        "wait_time_ps": {"field": "SUM_TIMER_WAIT", "width": 0},
    }

    # Add columns to the datatable if it is empty
    if not datatable.columns:
        for column_key, column_data in columns.items():
            column_width = column_data["width"]
            datatable.add_column(column_key, key=column_key, width=column_width)

    data = dolphie.table_io_waits_data.filtered_data
    for file_name, metrics in data.items():
        row_id = file_name
        row_values = []

        row_values.append(file_name)

        # Check if row already exists before processing columns
        if row_id in datatable.rows:
            row_exists = True
        else:
            row_exists = False

        for column_id, (column_name, column_data) in enumerate(columns.items()):
            if column_name == "Table":
                continue

            field = column_data["field"]
            column_value = None

            # Handle fields that may contain arrays
            if isinstance(field, list):
                # If the field is an array, it contains two fields to be combined
                count_field, wait_time_field = field

                # Get the count and wait_time values from the combined fields
                count_value = metrics.get(count_field, {})
                wait_time_value = metrics.get(wait_time_field, {})

                if tab.pfs_metrics_radio_set.pressed_button.id == "pfs_metrics_total":
                    count_value = count_value.get("t", 0)
                    wait_time_value = wait_time_value.get("t", 0)
                else:
                    count_value = count_value.get("d", 0)
                    wait_time_value = wait_time_value.get("d", 0)

                if count_value and wait_time_value:
                    column_value = f"{format_time(wait_time_value, picoseconds=True)} ({format_number(count_value)})"
                else:
                    column_value = "[dark_gray]N/A"
            else:
                column_value = metrics.get(field, {})
                if tab.pfs_metrics_radio_set.pressed_button.id == "pfs_metrics_total":
                    column_value = column_value.get("t", 0)
                else:
                    column_value = column_value.get("d", 0)

            # Handle row updates
            if row_exists:
                current_value = datatable.get_row(row_id)[column_id]
                if column_value != current_value:
                    datatable.update_cell(row_id, column_name, column_value)
            else:
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

    datatable.sort("wait_time_ps", reverse=True)

    # Update the title to reflect the number of active rows
    tab.pfs_metrics_tabs.get_tab("pfs_metrics_table_io_waits_tab").label = (
        f"Table I/O Waits ([$highlight]{datatable.row_count}[/$highlight])"
    )


def update_file_io_by_instance(tab: Tab) -> DataTable:
    dolphie = tab.dolphie
    datatable = tab.pfs_metrics_file_io_datatable

    if not dolphie.file_io_data or not dolphie.file_io_data.filtered_data:
        datatable.display = False
        tab.pfs_metrics_tabs.get_tab("pfs_metrics_file_io_tab").label = "File I/O ([$highlight]0[/$highlight])"

        return

    datatable.display = True

    columns = {
        "File or Table": {"field": "FILE_NAME", "width": None},
        "Wait Time": {"field": "SUM_TIMER_WAIT", "width": 10, "format": "time"},
        "Read Ops": {"field": "COUNT_READ", "width": 10, "format": "number"},
        "Write Ops": {"field": "COUNT_WRITE", "width": 10, "format": "number"},
        "Misc Ops": {"field": "COUNT_MISC", "width": 10, "format": "number"},
        "Read Bytes": {"field": "SUM_NUMBER_OF_BYTES_READ", "width": 10, "format": "bytes"},
        "Write Bytes": {"field": "SUM_NUMBER_OF_BYTES_WRITE", "width": 11, "format": "bytes"},
        "wait_time_ps": {"field": "SUM_TIMER_WAIT", "width": 0},
    }

    # Add columns to the datatable if it is empty
    if not datatable.columns:
        for column_key, column_data in columns.items():
            column_width = column_data["width"]
            datatable.add_column(column_key, key=column_key, width=column_width)

    data = dolphie.file_io_data.filtered_data
    for file_name, metrics in data.items():
        row_id = file_name
        row_values = []

        table_match = dolphie.file_io_data.table_pattern.search(file_name)
        if file_name.endswith("/mysql.ibd"):
            file_name = f"[dark_gray]{os.path.dirname(file_name)}[/dark_gray]/{os.path.basename(file_name)}"
        elif table_match:
            file_name = f"{table_match.group(1)}.{table_match.group(2)}"
        elif "/" in file_name:
            file_name = f"[dark_gray]{os.path.dirname(file_name)}[/dark_gray]/{os.path.basename(file_name)}"
        else:
            file_name = f"[b][light_blue][[/light_blue][/b][highlight]{file_name}[b][light_blue]][/light_blue][/b]"

        row_values.append(file_name)

        # Check if row already exists before processing columns
        if row_id in datatable.rows:
            row_exists = True
        else:
            row_exists = False

        for column_id, (column_name, column_data) in enumerate(columns.items()):
            if column_data["field"] == "FILE_NAME":
                continue

            column_value = metrics.get(column_data["field"], {})
            if tab.pfs_metrics_radio_set.pressed_button.id == "pfs_metrics_total":
                column_value = column_value.get("t", 0)
            else:
                column_value = column_value.get("d", 0)

            # Handle special formatting
            if column_data.get("format") == "time":
                column_value = format_time(column_value, picoseconds=True)
            elif column_value == 0 or column_value is None:
                if column_name == "wait_time_ps":
                    column_value = 0
                else:
                    column_value = "[dark_gray]0"
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

    datatable.sort("wait_time_ps", reverse=True)

    # Update the title to reflect the number of active rows
    tab.pfs_metrics_tabs.get_tab("pfs_metrics_file_io_tab").label = (
        f"File I/O ([$highlight]{datatable.row_count}[/$highlight])"
    )
