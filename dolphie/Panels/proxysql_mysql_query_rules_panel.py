from dolphie.Modules.Functions import format_number
from dolphie.Modules.TabManager import Tab
from textual.widgets import DataTable


def create_panel(tab: Tab) -> DataTable:
    dolphie = tab.dolphie

    columns = {
        "rule_id": {"name": "Rule ID", "width": 9, "format": None},
        "hits": {"name": "Total Hits", "width": 10, "format": "number"},
        "hits_s": {"name": "Hits/s", "width": 10, "format": "number"},
        "apply": {"name": "Apply", "width": 5, "format": None},
        "flagIN": {"name": "flagIN", "width": 6, "format": None},
        "flagOUT": {"name": "flagOUT", "width": 7, "format": None},
        "destination_hostgroup": {"name": "Dest HG", "width": 8, "format": None},
        "match_pattern": {"name": "Match Pattern", "width": None, "format": None},
        "match_digest": {"name": "Match Digest", "width": None, "format": None},
    }

    if dolphie.show_additional_query_columns:
        all_columns = [i for i in dolphie.proxysql_mysql_query_rules[0]]
        for column in all_columns:
            if column not in columns:
                columns[column] = {"name": column, "width": None, "format": None}

    mysql_query_rules = tab.proxysql_mysql_query_rules_datatable

    # Clear table if columns change
    if len(mysql_query_rules.columns) != len(columns):
        mysql_query_rules.clear(columns=True)

    # Add columns to the datatable if it is empty
    if not mysql_query_rules.columns:
        for column_key, column_data in columns.items():
            column_name = column_data["name"]
            column_width = column_data["width"]
            mysql_query_rules.add_column(column_name, key=column_key, width=column_width)

    for row in dolphie.proxysql_mysql_query_rules:
        row_id = row["rule_id"]
        row_values = []

        for column_id, (column_key, column_data) in enumerate(columns.items()):
            column_name = column_data["name"]
            column_format = column_data["format"]
            column_value = row.get(column_key)

            # Calculate the values per second for the following columns
            if column_key in ["hits_s"]:
                if not dolphie.proxysql_per_second_data.get(row_id, {}).get(column_key, 0):
                    column_value = 0
                else:
                    value_diff = int(column_value) - dolphie.proxysql_per_second_data.get(row_id, {}).get(column_key, 0)
                    column_value = round(value_diff / dolphie.polling_latency)

                dolphie.proxysql_per_second_data.setdefault(row_id, {})[column_key] = int(row.get(column_key, 0))

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
        f"Active Query Rules ([highlight]{mysql_query_rules.row_count}[/highlight])"
    )
