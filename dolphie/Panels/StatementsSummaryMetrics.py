from rich.syntax import Syntax

from dolphie.DataTypes import ConnectionSource
from dolphie.Modules.Functions import format_number, format_picoseconds, format_query
from dolphie.Modules.TabManager import Tab

MAX_ROWS = 100


def create_panel(tab: Tab):
    dolphie = tab.dolphie
    datatable = tab.statements_summary_datatable
    query_length_max = 300

    columns = [
        {"name": "Schema", "field": "schema_name", "width": 14, "format_number": False},
        {"name": "Count", "field": "count_star", "width": 8, "format_number": True},
        {
            "name": "Latency",
            "field": "sum_timer_wait",
            "width": 10,
            "format_number": False,
        },
    ]

    if (
        dolphie.is_mysql_version_at_least("8.0")
        and dolphie.connection_source_alt != ConnectionSource.mariadb
    ):
        columns.extend(
            [
                {
                    "name": "95th %",
                    "field": "quantile_95",
                    "width": 10,
                    "format_number": False,
                },
                {
                    "name": "99th %",
                    "field": "quantile_99",
                    "width": 10,
                    "format_number": False,
                },
            ]
        )

    columns.extend(
        [
            {
                "name": "Lock time",
                "field": "sum_lock_time",
                "width": 9,
                "format_number": False,
            },
            {
                "name": "Rows examined",
                "field": "sum_rows_examined",
                "width": 13,
                "format_number": True,
            },
            {
                "name": "Rows affected",
                "field": "sum_rows_affected",
                "width": 13,
                "format_number": True,
            },
            {
                "name": "Rows sent",
                "field": "sum_rows_sent",
                "width": 9,
                "format_number": True,
            },
            {
                "name": "Errors",
                "field": "sum_errors",
                "width": 6,
                "format_number": True,
            },
            {
                "name": "Warnings",
                "field": "sum_warnings",
                "width": 8,
                "format_number": True,
            },
            {
                "name": "Bad idx",
                "field": "sum_no_good_index_used",
                "width": 7,
                "format_number": True,
            },
            {
                "name": "No idx",
                "field": "sum_no_index_used",
                "width": 6,
                "format_number": True,
            },
            {"name": "Query", "field": "query", "width": None, "format_number": False},
            {
                "name": "latency_total",
                "field": "sum_timer_wait",
                "width": 0,
                "format_number": False,
            },
        ]
    )

    if (
        not dolphie.statements_summary_data
        or not dolphie.statements_summary_data.filtered_data
    ):
        datatable.display = False
        tab.statements_summary_title.update(

                f"{tab.dolphie.panels.get_panel_title(tab.dolphie.panels.statements_summary.name)} "
                f"([$highlight]0[/$highlight])"

        )

        return

    datatable.display = True

    if len(datatable.columns) != len(columns):
        datatable.clear(columns=True)

    column_names = []
    column_fields = []
    column_format_numbers = []

    if not tab.statements_summary_datatable.columns:
        for column_data in columns:
            datatable.add_column(
                column_data["name"], key=column_data["name"], width=column_data["width"]
            )

    for column_data in columns:
        column_names.append(column_data["name"])
        column_fields.append(column_data["field"])
        column_format_numbers.append(column_data["format_number"])

    data = tab.dolphie.statements_summary_data.filtered_data
    display_mode = ""
    if tab.statements_summary_radio_set.pressed_button.id == "statements_summary_total":
        display_mode = "t"
    elif (
        tab.statements_summary_radio_set.pressed_button.id
        == "statements_summarys_delta"
    ):
        display_mode = "d"
    else:
        display_mode = "d_last_sample"

    # We sort by sum_timer_wait and retain only the top MAX_ROWS elements to minimize useless
    # UI processing of many rows - at the expense of the sort & slice operations.
    display_data = dict(
        sorted(
            data.items(),
            key=lambda element: element[1]["sum_timer_wait"][display_mode],
            reverse=True,
        )[: MAX_ROWS + 1]
    )

    if display_data:
        for digest, metrics in display_data.items():
            if digest in tab.statements_summary_datatable.rows:
                datatable_row = tab.statements_summary_datatable.get_row(digest)

                for column_id, (
                    column_name,
                    column_field,
                    column_format_number,
                ) in enumerate(zip(column_names, column_fields, column_format_numbers)):
                    column_value = metrics.get(column_field, {})

                    if isinstance(column_value, dict):
                        column_value = column_value.get(display_mode, 0)

                    if column_name == "Query":
                        if tab.dolphie.show_statements_summary_query_digest_text_sample:
                            column_value = metrics.get("query_sample_text")
                        else:
                            column_value = metrics.get("digest_text")
                        column_value = format_query(column_value)
                    elif column_name == "Schema":
                        column_value = column_value or "[dark_gray]N/A"
                    elif column_format_number:
                        column_value = format_number(column_value)
                    elif column_name in ("Latency", "Lock time", "CPU time") or column_name in ["95th %", "99th %"]:
                        column_value = format_picoseconds(column_value)

                    if column_name != "latency_total" and (
                        column_value == "0" or column_value == 0
                    ):
                        column_value = "[dark_gray]0"

                    # Use the cached row data
                    datatable_value = datatable_row[column_id]

                    temp_column_value = column_value
                    temp_datatable_value = datatable_value

                    update_width = False
                    if column_name == "Query":
                        update_width = True
                        if isinstance(column_value, Syntax):
                            temp_column_value = column_value.code[:query_length_max]
                            column_value = format_query(temp_column_value)
                        if isinstance(datatable_value, Syntax):
                            temp_datatable_value = datatable_value.code

                    if temp_column_value != temp_datatable_value:
                        tab.statements_summary_datatable.update_cell(
                            digest, column_name, column_value, update_width=update_width
                        )
            else:
                row_values = []

                for column_id, (
                    column_name,
                    column_field,
                    column_format_number,
                ) in enumerate(zip(column_names, column_fields, column_format_numbers)):
                    column_value = metrics.get(column_field, {})

                    if isinstance(column_value, dict):
                        column_value = column_value.get(display_mode, 0)

                    if column_name == "Query":
                        if tab.dolphie.show_statements_summary_query_digest_text_sample:
                            column_value = metrics.get("query_sample_text")
                        else:
                            column_value = metrics.get("digest_text")
                        column_value = format_query(column_value)
                    elif column_name == "Schema":
                        column_value = column_value or "[dark_gray]N/A"
                    elif column_format_number:
                        column_value = format_number(column_value)
                    elif column_name in ("Latency", "Lock time", "CPU time") or column_name in ["95th %", "99th %"]:
                        column_value = format_picoseconds(column_value)

                    if column_name != "latency_total" and (
                        column_value == "0" or column_value == 0
                    ):
                        column_value = "[dark_gray]0"

                    # Only show the first {query_length_max} characters of the query
                    if column_name == "Query" and isinstance(column_value, Syntax):
                        column_value = format_query(
                            column_value.code[:query_length_max]
                        )

                    row_values.append(column_value)

                if row_values:
                    tab.statements_summary_datatable.add_row(*row_values, key=digest)

    if display_data:
        current_rows = set(display_data.keys())
        existing_rows = set(datatable.rows.keys())

        rows_to_remove = existing_rows - current_rows
        for row_id in rows_to_remove:
            datatable.remove_row(row_id)
    else:
        if datatable.row_count:
            datatable.clear()

    title = (
        f"{tab.dolphie.panels.get_panel_title(tab.dolphie.panels.statements_summary.name)} "
        f"([$highlight]{tab.statements_summary_datatable.row_count}[/$highlight])"
    )
    tab.statements_summary_title.update(title)

    tab.statements_summary_datatable.sort(
        "latency_total", reverse=tab.dolphie.sort_by_time_descending
    )
