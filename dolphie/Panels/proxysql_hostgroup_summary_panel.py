from dolphie.Modules.Functions import format_bytes, format_number
from dolphie.Modules.TabManager import Tab
from textual.widgets import DataTable


def create_panel(tab: Tab) -> DataTable:
    dolphie = tab.dolphie

    columns = {
        "hostgroup": {"name": "Hostgroup", "width": None, "format": None},
        "srv_host": {"name": "Backend Host", "width": 35, "format": None},
        "srv_port": {"name": "Port", "width": 5, "format": None},
        "status": {"name": "Status", "width": None, "format": None},
        "weight": {"name": "Weight", "width": None, "format": None},
        "use_ssl": {"name": "SSL", "width": 5, "format": None},
        "ConnUsed": {"name": "Conn Used", "width": 11, "format": "number"},
        "ConnFree": {"name": "Conn Free", "width": 10, "format": "number"},
        "ConnOK": {"name": "Conn OK", "width": 10, "format": "number"},
        "ConnERR": {"name": "Conn ERR", "width": 10, "format": "number"},
        "MaxConnUsed": {"name": "Max Conn", "width": 11, "format": "number"},
        "Queries_per_sec": {"name": "Queries/s", "width": 10, "format": "number"},
        "Bytes_data_sent_per_sec": {"name": "Data Sent/s", "width": 11, "format": "bytes"},
        "Bytes_data_recv_per_sec": {"name": "Data Recvd/s", "width": 12, "format": "bytes"},
        "Latency_us": {"name": "Latency (ms)", "width": 12, "format": "time"},
    }

    hostgroup_summary_datatable = tab.proxysql_hostgroup_summary_datatable

    # Add columns to the datatable if it is empty
    if not hostgroup_summary_datatable.columns:
        for column_key, column_data in columns.items():
            column_name = column_data["name"]
            column_width = column_data["width"]
            hostgroup_summary_datatable.add_column(column_name, key=column_key, width=column_width)

    for row in dolphie.proxysql_hostgroup_summary:
        row_id = f"{row['hostgroup']}_{row['srv_host']}_{row['srv_port']}"
        row_values = []

        for column_id, (column_key, column_data) in enumerate(columns.items()):
            column_name = column_data["name"]
            column_format = column_data["format"]
            column_value = row.get(column_key, 0)

            if column_format == "time":
                column_value = f"{round(int(column_value) / 1000, 2)}"
            elif column_format == "bytes":
                column_value = format_bytes(column_value)
            elif column_format == "number":
                column_value = format_number(column_value)
            elif column_key == "hostgroup":
                column_value = int(column_value)
            elif column_key == "srv_host":
                column_value = dolphie.get_hostname(column_value)
            elif column_key == "status":
                column_value = "[green]ONLINE" if column_value == "ONLINE" else f"[red]{column_value}"
            elif column_key == "use_ssl":
                column_value = "ON" if column_value == "1" else "OFF"

            if column_value == "0" or column_value == 0:
                column_value = "[dark_gray]0"

            if row_id in hostgroup_summary_datatable.rows:
                datatable_value = hostgroup_summary_datatable.get_row(row_id)[column_id]

                # Update the datatable if values differ
                if column_value != datatable_value:
                    hostgroup_summary_datatable.update_cell(row_id, column_key, column_value)
            else:
                # Create an array of values to append to the datatable
                row_values.append(column_value)

        # Add a new row to the datatable
        if row_values:
            hostgroup_summary_datatable.add_row(*row_values, key=row_id)

    # Remove rows from datatable that no longer exist in the data
    if dolphie.proxysql_hostgroup_summary:
        current_rows = {
            f"{row['hostgroup']}_{row['srv_host']}_{row['srv_port']}" for row in dolphie.proxysql_hostgroup_summary
        }
        existing_rows = set(hostgroup_summary_datatable.rows.keys())

        rows_to_remove = existing_rows - current_rows
        for row_id in rows_to_remove:
            hostgroup_summary_datatable.remove_row(row_id)
    else:
        if hostgroup_summary_datatable.row_count:
            hostgroup_summary_datatable.clear()

    hostgroup_summary_datatable.sort("hostgroup")

    tab.proxysql_hostgroup_summary_title.update(
        f"Hostgroups ([highlight]{hostgroup_summary_datatable.row_count}[/highlight])"
    )
