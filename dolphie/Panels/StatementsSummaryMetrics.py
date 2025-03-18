from rich.syntax import Syntax

from dolphie.DataTypes import StatementsSummaryMetricsColumn
from dolphie.Modules.Functions import format_number, format_picoseconds, format_query
from dolphie.Modules.TabManager import Tab

COLUMNS = [
    StatementsSummaryMetricsColumn("Queries", "count_star", 7, True),
    StatementsSummaryMetricsColumn("Latency", "sum_timer_wait", 10, False),
    StatementsSummaryMetricsColumn("Lock time", "sum_lock_time", 9, False),
    StatementsSummaryMetricsColumn("CPU time", "sum_cpu_time", 8, False),
    StatementsSummaryMetricsColumn("Rows examined", "sum_rows_examined", 13, True),
    StatementsSummaryMetricsColumn("Rows affected", "sum_rows_affected", 13, True),
    StatementsSummaryMetricsColumn("Rows sent", "sum_rows_sent", 9, True),
    StatementsSummaryMetricsColumn("Errors", "sum_errors", 6, True),
    StatementsSummaryMetricsColumn("Warnings", "sum_warnings", 8, True),
    StatementsSummaryMetricsColumn("Bad idx", "sum_no_good_index_used", 7, True),
    StatementsSummaryMetricsColumn("No idx", "sum_no_index_used", 6, True),
    StatementsSummaryMetricsColumn("Schema", "schema_name", None, False),
    StatementsSummaryMetricsColumn("Query", "query", None, False),
    StatementsSummaryMetricsColumn("latency_total", "sum_timer_wait", 0, False),
]


def create_panel(tab: Tab):
    if not tab.statements_summary_datatable.columns:
        for column_data in COLUMNS:
            column_name = column_data.name
            column_width = column_data.width
            tab.statements_summary_datatable.add_column(column_name, key=column_name, width=column_width)

    if tab.dolphie.statements_summary_data is not None:
        for digest, row in tab.dolphie.statements_summary_data.cumulative_diff.items():
            row_values = []

            if row.count_star == 0:
                continue

            for column_id, (c) in enumerate(COLUMNS):
                if c.name == "Query":
                    if tab.dolphie.show_statements_summary_query_digest_text_sample:
                        query = row.query_sample_text
                    else:
                        query = row.digest_text
                    digest_value = format_query(query)
                elif c.name in ("Latency", "Lock time", "CPU time"):
                    digest_value = format_picoseconds(getattr(row, c.field))
                elif c.format_number:
                    digest_value = format_number(getattr(row, c.field))
                else:
                    digest_value = getattr(row, c.field)

                if digest in tab.statements_summary_datatable.rows:
                    datatable_value = tab.statements_summary_datatable.get_row(digest)[column_id]

                    # Initialize temp values for possible Syntax object comparison below
                    temp_digest_value = digest_value
                    temp_datatable_value = datatable_value

                    # If the column is the query, we need to compare the code of the Syntax object
                    update_width = False
                    if c.name == "Query":
                        update_width = True
                        if isinstance(digest_value, Syntax):
                            temp_digest_value = digest_value.code

                            # Only show the first {query_length_max} characters of the query
                            digest_value = format_query(digest_value.code)
                        if isinstance(datatable_value, Syntax):
                            temp_datatable_value = datatable_value.code

                    # Update the datatable if values differ
                    if temp_digest_value != temp_datatable_value:
                        tab.statements_summary_datatable.update_cell(
                            digest, c.name, digest_value, update_width=update_width
                        )
                else:
                    if c.name == "Query" and isinstance(digest_value, Syntax):
                        temp_digest_value = digest_value.code

                    row_values.append(digest_value)

            # Add a new row to the datatable
            if row_values:
                tab.statements_summary_datatable.add_row(*row_values, key=digest)

    # Clean up rows that no longer exist
    if tab.dolphie.statements_summary_data is not None and tab.dolphie.statements_summary_data.cumulative_diff:
        current_rows = set(tab.dolphie.statements_summary_data.cumulative_diff.keys())
        existing_rows = set(tab.statements_summary_datatable.rows.keys())

        rows_to_remove = existing_rows - current_rows
        for row_id in rows_to_remove:
            tab.statements_summary_datatable.remove_row(row_id)
    else:
        if tab.statements_summary_datatable.row_count:
            tab.statements_summary_datatable.clear()

    title = (
        f"{tab.dolphie.panels.get_panel_title(tab.dolphie.panels.statements_summary.name)} "
        f"([highlight]{tab.statements_summary_datatable.row_count}[/highlight])"
    )
    tab.statements_summary_title.update(title)

    tab.statements_summary_datatable.sort("latency_total", reverse=tab.dolphie.sort_by_time_descending)
