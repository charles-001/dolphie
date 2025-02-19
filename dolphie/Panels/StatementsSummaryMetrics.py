from rich.syntax import Syntax

from dolphie.DataTypes import StatementsSummaryMetricsColumn
from dolphie.Modules.Functions import format_number, format_query
from dolphie.Modules.TabManager import Tab

COLUMNS = [
    StatementsSummaryMetricsColumn("Queries", "count_star", 7, True, True),
    StatementsSummaryMetricsColumn("Latency", "sum_timer_wait", 8, False, True),
    StatementsSummaryMetricsColumn("Lock time", "sum_lock_time", 9, False, True),
    StatementsSummaryMetricsColumn("CPU time", "sum_cpu_time", 8, False, True),
    StatementsSummaryMetricsColumn("Rows examined", "sum_rows_examined", 13, True, True),
    StatementsSummaryMetricsColumn("Rows affected", "sum_rows_affected", 13, True, True),
    StatementsSummaryMetricsColumn("Rows sent", "sum_rows_sent", 9, True, True),
    StatementsSummaryMetricsColumn("Errors", "sum_errors", 6, True, True),
    StatementsSummaryMetricsColumn("Warnings", "sum_warnings", 8, True, True),
    StatementsSummaryMetricsColumn("Bad idx", "sum_no_good_index_used", 7, True, True),
    StatementsSummaryMetricsColumn("No idx", "sum_no_index_used", 6, True, True),
    StatementsSummaryMetricsColumn("Schema", "schema_name", None, False, True),
    StatementsSummaryMetricsColumn("Digest", "digest", 64, True, False),
    StatementsSummaryMetricsColumn("Digest text", "digest_text", None, False, True),
    StatementsSummaryMetricsColumn("Sample query", "query_sample_text", None, False, False),
]
COLUMNS_BY_FIELD = {x.field: x for x in COLUMNS}


def create_panel(tab: Tab):
    visible_columns = [x for x in COLUMNS if COLUMNS_BY_FIELD[x.field].visible]

    if len(tab.statements_summary_datatable.columns) != len(visible_columns):
        tab.statements_summary_datatable.clear(columns=True)

    if not tab.statements_summary_datatable.columns:
        for column_data in visible_columns:
            column_name = column_data.name
            column_width = column_data.width
            tab.statements_summary_datatable.add_column(column_name, key=column_name, width=column_width)

    if tab.dolphie.statements_summary_data is not None:
        for digest, row in tab.dolphie.statements_summary_data.data_to_display.items():
            row_values = []

            for column_id, (c) in enumerate(visible_columns):
                if c.field in {"digest_text", "query_sample_text"}:
                    digest_value = format_query(getattr(row, c.field))
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
                    if c.field in {"digest_text", "query_sample_text"}:
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
                    if c.field in {"digest_text", "query_sample_text"} and isinstance(digest_value, Syntax):
                        temp_digest_value = digest_value.code

                    row_values.append(digest_value)

            # Add a new row to the datatable
            if row_values:
                tab.statements_summary_datatable.add_row(*row_values, key=digest)

    title = (
        f"{tab.dolphie.panels.get_panel_title(tab.dolphie.panels.statements_summary.name)} "
        f"([highlight]{tab.statements_summary_datatable.row_count}[/highlight])"
    )
    tab.statements_summary_title.update(title)

    tab.statements_summary_datatable.sort(
        COLUMNS_BY_FIELD["sum_timer_wait"].name, reverse=tab.dolphie.sort_by_time_descending
    )


def toggle_query_digest_text_or_sample(tab: Tab):
    tab.statements_summary_datatable.clear(columns=True)
    COLUMNS_BY_FIELD["digest_text"].visible = not COLUMNS_BY_FIELD["digest_text"].visible
    COLUMNS_BY_FIELD["query_sample_text"].visible = not COLUMNS_BY_FIELD["query_sample_text"].visible
    create_panel(tab)
