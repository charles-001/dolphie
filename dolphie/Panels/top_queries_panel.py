import re
from datetime import datetime

from dolphie import Dolphie
from dolphie.Modules.Functions import format_number, format_picoseconds
from textual.widgets import DataTable


def create_panel(dolphie: Dolphie) -> DataTable:
    columns = [
        {"name": "Count", "field": "total_count", "width": 13},
        {"name": "QPS", "field": "qps", "width": 6},
        {"name": "Average Time", "field": "average_time", "width": 13},
        {"name": "Query Digest", "field": "query_digest", "width": None},
        {"name": "Total Time", "field": "total_time", "width": 13},
    ]

    top_queries_datatable = dolphie.app.query_one("#panel_top_queries", DataTable)

    # Add columns to the datatable if it is empty
    if not top_queries_datatable.columns:
        for column_data in columns:
            column_name = column_data["name"]
            column_key = column_data["field"]
            column_width = column_data["width"]
            top_queries_datatable.add_column(column_name, key=column_key, width=column_width)

    sorted_digests = sorted(
        dolphie.metric_manager.metrics.query_digests.digests.items(),
        key=lambda item: item[1].total_count,  # Sort by the total_count attribute
        reverse=True,  # To sort in descending order (highest count first)
    )

    # Create a set to keep track of row keys that should exist in the DataTable
    expected_rows = set()

    for digest, digest_data in sorted_digests[:15]:
        # Check if the thread_id exists in the datatable
        if digest in top_queries_datatable.rows:
            datatable_row = top_queries_datatable.get_row(digest)

            # Update the datatable if values differ
            for column_id, column_data in enumerate(columns):
                column_name = column_data["field"]

                if column_name == "query_digest":
                    value = re.sub(r"\s+", " ", getattr(digest_data, column_name))
                elif column_name == "qps":
                    value = format_number(
                        getattr(digest_data, "total_count")
                        / (datetime.now() - dolphie.dolphie_start_time).total_seconds(),
                        small_number=True,
                    )
                elif column_name == "average_time":
                    value = format_picoseconds(getattr(digest_data, column_name))
                elif column_name == "total_time":
                    value = getattr(digest_data, "total_count") * getattr(digest_data, "average_time")
                else:
                    value = getattr(digest_data, column_name)

                if value != datatable_row[column_id]:
                    top_queries_datatable.update_cell(digest, column_name, value)
        else:
            row_values = []
            for column_data in columns:
                column_name = column_data["field"]

                if column_name == "query_digest":
                    value = re.sub(r"\s+", " ", getattr(digest_data, column_name))
                elif column_name == "qps":
                    value = format_number(
                        getattr(digest_data, "total_count")
                        / (datetime.now() - dolphie.dolphie_start_time).total_seconds(),
                        small_number=True,
                    )
                elif column_name == "average_time":
                    value = format_picoseconds(getattr(digest_data, column_name))
                elif column_name == "total_time":
                    value = getattr(digest_data, "total_count") * getattr(digest_data, "average_time")
                else:
                    value = getattr(digest_data, column_name)

                row_values.append(value)

            top_queries_datatable.add_row(*row_values, key=digest)

        # Add the current row's key to the expected_rows set
        expected_rows.add(digest)

    # Remove rows from top_queries_datatable that do not belong
    rows_to_remove = set(top_queries_datatable.rows.keys()) - expected_rows
    for id in rows_to_remove:
        top_queries_datatable.remove_row(id)

    top_queries_datatable.sort("total_time", reverse=True)
