from textual.widgets import DataTable

from dolphie.Modules.Functions import format_number
from dolphie.Modules.TabManager import Tab


def create_panel(tab: Tab) -> DataTable:
    dolphie = tab.dolphie

    all_columns = {
        "rule_id": {"name": "Rule ID", "format": None, "always_show": True},
        "hits": {"name": "Total Hits", "format": "number", "always_show": True},
        "hits_s": {"name": "Hits/s", "format": "number", "always_show": True},
        "apply": {"name": "Apply", "format": None, "always_show": True},
        "log": {"name": "Log", "format": None, "always_show": True},
        "flagIN": {"name": "flagIN", "format": None, "always_show": False},
        "flagOUT": {"name": "flagOUT", "format": None, "always_show": False},
        "destination_hostgroup": {"name": "Dest HG", "format": None, "always_show": False},
        "username": {"name": "Username", "format": None, "always_show": False},
        "match_pattern": {"name": "Match Pattern", "format": None, "always_show": False},
        "match_digest": {"name": "Match Digest", "format": None, "always_show": False},
        "schemaname": {"name": "Schema", "format": None, "always_show": False},
        "client_addr": {"name": "Client Addr", "format": None, "always_show": False},
        "proxy_addr": {"name": "Proxy Addr", "format": None, "always_show": False},
        "proxy_port": {"name": "Proxy Port", "format": None, "always_show": False},
        "digest": {"name": "Digest", "format": None, "always_show": False},
        "negate_match_pattern": {"name": "Negate Match", "format": None, "always_show": False},
        "re_modifiers": {"name": "RE Modifiers", "format": None, "always_show": False},
        "replace_pattern": {"name": "Replace Pattern", "format": None, "always_show": False},
        "cache_ttl": {"name": "Cache TTL", "format": None, "always_show": False},
        "cache_empty_result": {"name": "Cache Empty", "format": None, "always_show": False},
        "cache_timeout": {"name": "Cache Timeout", "format": None, "always_show": False},
        "reconnect": {"name": "Reconnect", "format": None, "always_show": False},
        "timeout": {"name": "Timeout", "format": None, "always_show": False},
        "retries": {"name": "Retries", "format": None, "always_show": False},
        "delay": {"name": "Delay", "format": None, "always_show": False},
        "next_query_flagIN": {"name": "Next flagIN", "format": None, "always_show": False},
        "mirror_flagOUT": {"name": "Mirror flagOUT", "format": None, "always_show": False},
        "mirror_hostgroup": {"name": "Mirror HG", "format": None, "always_show": False},
        "error_msg": {"name": "Error Msg", "format": None, "always_show": False},
        "OK_msg": {"name": "OK Msg", "format": None, "always_show": False},
        "sticky_conn": {"name": "Sticky Conn", "format": None, "always_show": False},
        "multiplex": {"name": "Multiplex", "format": None, "always_show": False},
        "gtid_from_hostgroup": {"name": "GTID from HG", "format": None, "always_show": False},
        "attributes": {"name": "Attributes", "format": None, "always_show": False},
        "comment": {"name": "Comment", "format": None, "always_show": False},
    }

    # Filter only relevant columns from all_columns based on data presence or always_show flag
    columns_with_data = {
        column
        for row in dolphie.proxysql_mysql_query_rules
        for column, value in row.items()
        if value not in (None, "", "NULL", 0, "0") or all_columns[column]["always_show"]
    }

    # Build the filtered columns dictionary using only columns_with_data
    columns_filtered = {
        column: {
            "name": props["name"],
            "width": None,
            "format": props["format"],
        }
        for column, props in all_columns.items()
        if column in columns_with_data
    }

    mysql_query_rules = tab.proxysql_mysql_query_rules_datatable

    # Clear table if columns change
    if len(mysql_query_rules.columns) != len(columns_filtered):
        mysql_query_rules.clear(columns=True)

    # Add columns to the datatable if it is empty
    if not mysql_query_rules.columns:
        for column_key, column_data in columns_filtered.items():
            column_name = column_data["name"]
            column_width = column_data["width"]
            mysql_query_rules.add_column(column_name, key=column_key, width=column_width)

    for row in dolphie.proxysql_mysql_query_rules:
        row_id = row["rule_id"]
        row_values = []

        for column_id, (column_key, column_data) in enumerate(columns_filtered.items()):
            column_name = column_data["name"]
            column_format = column_data["format"]
            column_value = row.get(column_key)

            # Calculate the values per second for the following columns
            if column_key in ["hits_s"]:
                if not dolphie.proxysql_per_second_data.get(row_id, {}).get(column_key, 0):
                    column_value = "[dark_gray]0"
                else:
                    value_diff = int(column_value) - dolphie.proxysql_per_second_data.get(row_id, {}).get(column_key, 0)
                    column_value = round(value_diff / dolphie.polling_latency)

                dolphie.proxysql_per_second_data.setdefault(row_id, {})[column_key] = int(row.get(column_key, 0))

            if column_key in ["apply", "log"]:
                column_value = "Yes" if column_value == "1" else "No"

            if column_format == "number":
                column_value = format_number(column_value)

            if column_value is None:
                column_value = "[dark_gray]N/A"
            elif column_value == "0":
                column_value = "[dark_gray]0"

            if row_id in mysql_query_rules.rows:
                datatable_value = mysql_query_rules.get_row(row_id)[column_id]

                # Update the datatable if values differ
                if column_value != datatable_value:
                    mysql_query_rules.update_cell(row_id, column_key, column_value)
            else:
                # Create an array of values to append to the datatable
                row_values.append(column_value)

        # Add a new row to the datatable
        if row_values:
            mysql_query_rules.add_row(*row_values, key=row_id)

    # Remove rows from datatable that no longer exist in the data
    if dolphie.proxysql_mysql_query_rules:
        current_rows = {row["rule_id"] for row in dolphie.proxysql_mysql_query_rules}
        existing_rows = set(mysql_query_rules.rows.keys())

        rows_to_remove = existing_rows - current_rows
        for row_id in rows_to_remove:
            mysql_query_rules.remove_row(row_id)
    else:
        if mysql_query_rules.row_count:
            mysql_query_rules.clear()

    tab.proxysql_mysql_query_rules_title.update(
        f"{dolphie.panels.get_panel_title(dolphie.panels.proxysql_mysql_query_rules.name)} "
        f"([$highlight]{mysql_query_rules.row_count}[/$highlight])"
    )
