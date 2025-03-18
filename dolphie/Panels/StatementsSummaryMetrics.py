from rich.syntax import Syntax

from dolphie.Modules.Functions import format_number, format_picoseconds, format_query
from dolphie.Modules.TabManager import Tab

COLUMNS = {
    "Queries": {"field": "count_star", "width": 7, "format_number": True},
    "Latency": {"field": "sum_timer_wait", "width": 10, "format_number": False},
    "Lock time": {"field": "sum_lock_time", "width": 9, "format_number": False},
    "Rows examined": {"field": "sum_rows_examined", "width": 13, "format_number": True},
    "Rows affected": {"field": "sum_rows_affected", "width": 13, "format_number": True},
    "Rows sent": {"field": "sum_rows_sent", "width": 9, "format_number": True},
    "Errors": {"field": "sum_errors", "width": 6, "format_number": True},
    "Warnings": {"field": "sum_warnings", "width": 8, "format_number": True},
    "Bad idx": {"field": "sum_no_good_index_used", "width": 7, "format_number": True},
    "No idx": {"field": "sum_no_index_used", "width": 6, "format_number": True},
    "Schema": {"field": "schema_name", "width": None, "format_number": False},
    "Query": {"field": "query", "width": None, "format_number": False},
    "latency_total": {"field": "sum_timer_wait", "width": 0, "format_number": False},
}


def create_panel(tab: Tab):
    dolphie = tab.dolphie
    datatable = tab.statements_summary_datatable

    if not dolphie.statements_summary_data or not dolphie.statements_summary_data.filtered_data:
        datatable.display = False
        tab.statements_summary_title.update(
            (
                f"{tab.dolphie.panels.get_panel_title(tab.dolphie.panels.statements_summary.name)} "
                f"([highlight]0[/highlight])"
            )
        )
        return

    datatable.display = True

    if not tab.statements_summary_datatable.columns:
        for column_name, column_data in COLUMNS.items():
            column_width = column_data["width"]
            datatable.add_column(column_name, key=column_name, width=column_width)

    data = tab.dolphie.statements_summary_data.filtered_data
    if data:
        for digest, metrics in data.items():
            row_values = []

            for column_id, (column_name, c) in enumerate(COLUMNS.items()):
                column_value = metrics.get(c["field"], {})
                if isinstance(column_value, dict):
                    if tab.statements_summary_radio_set.pressed_button.id == "statements_summary_total":
                        delta_value = column_value.get("t", 0)
                    else:
                        delta_value = column_value.get("d", 0)

                if column_name == "Query":
                    if tab.dolphie.show_statements_summary_query_digest_text_sample:
                        column_value = metrics.get("query_sample_text")
                    else:
                        column_value = metrics.get("digest_text")
                    digest_value = format_query(column_value)
                elif column_name == "Schema":
                    column_value = metrics.get("schema_name")
                    digest_value = column_value
                elif c["format_number"]:
                    digest_value = format_number(delta_value)
                elif column_name in ("Latency", "Lock time", "CPU time"):
                    digest_value = format_picoseconds(delta_value)
                else:
                    digest_value = delta_value

                if digest in tab.statements_summary_datatable.rows:
                    datatable_value = tab.statements_summary_datatable.get_row(digest)[column_id]

                    temp_digest_value = digest_value
                    temp_datatable_value = datatable_value

                    update_width = False
                    if column_name == "Query":
                        update_width = True
                        if isinstance(digest_value, Syntax):
                            temp_digest_value = digest_value.code
                            digest_value = format_query(digest_value.code)
                        if isinstance(datatable_value, Syntax):
                            temp_datatable_value = datatable_value.code

                    if temp_digest_value != temp_datatable_value:
                        tab.statements_summary_datatable.update_cell(
                            digest, column_name, digest_value, update_width=update_width
                        )
                else:
                    if column_name == "Query" and isinstance(digest_value, Syntax):
                        temp_digest_value = digest_value.code
                    row_values.append(digest_value)

            if row_values:
                tab.statements_summary_datatable.add_row(*row_values, key=digest)

    if data:
        current_rows = set(data.keys())
        existing_rows = set(datatable.rows.keys())

        rows_to_remove = existing_rows - current_rows
        for row_id in rows_to_remove:
            datatable.remove_row(row_id)
    else:
        if datatable.row_count:
            datatable.clear()

    title = (
        f"{tab.dolphie.panels.get_panel_title(tab.dolphie.panels.statements_summary.name)} "
        f"([highlight]{tab.statements_summary_datatable.row_count}[/highlight])"
    )
    tab.statements_summary_title.update(title)

    tab.statements_summary_datatable.sort("latency_total", reverse=tab.dolphie.sort_by_time_descending)
