import re
from typing import Any, Dict, List

from dolphie.DataTypes import StatementsSummaryMetricsRow as Row


class StatementsSummaryMetrics:
    def __init__(self, query_data: List[Dict[str, Any]]):
        self._raw_statements_summary_data: Dict[str, Row] = {}
        self._update_internal(query_data)
        self.data_to_display: Dict[str, Row] = self._raw_statements_summary_data

    def _update_internal(self, query_data: List[Dict[str, Any]]):
        self._raw_statements_summary_data = {
            row["digest"]: Row(
                row["digest"],
                row["digest_text"],
                row["query_sample_text"],
                row["schema_name"],
                row["sum_no_good_index_used"],
                row["sum_no_index_used"],
                row["count_star"],
                row["sum_errors"],
                row["sum_warnings"],
                row["sum_timer_wait"] / 1000000.0,
                row["sum_lock_time"] / 1000000.0,
                row["sum_cpu_time"] / 1000000.0,
                row["sum_rows_sent"],
                row["sum_rows_examined"],
                row["sum_rows_affected"],
            )
            for row in query_data
        }

    def update(self, query_data: List[Dict[str, Any]]):
        new_data_to_display: Dict[str, Row] = {}
        for row in query_data:
            digest = row["digest"]
            query_sample_text = (
                re.sub(" +", " ", row["query_sample_text"].replace("\n", " ")) if row["query_sample_text"] else ""
            )

            # Add a zero-ed row for digests this is the first time we see, so we can compute deltas for them below
            if digest not in self._raw_statements_summary_data:
                self._raw_statements_summary_data[digest] = Row(
                    row["digest"],
                    row["digest_text"],
                    row["query_sample_text"],
                    row["schema_name"],
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                )
            # Update the row for the given digest by recomputing deltas for each metric
            new_data_to_display[digest] = Row(
                row["digest"],
                row["digest_text"],
                query_sample_text,
                row["schema_name"],
                row["sum_no_good_index_used"] - self._raw_statements_summary_data[digest].sum_no_good_index_used,
                row["sum_no_index_used"] - self._raw_statements_summary_data[digest].sum_no_index_used,
                row["count_star"] - self._raw_statements_summary_data[digest].count_star,
                row["sum_errors"] - self._raw_statements_summary_data[digest].sum_errors,
                row["sum_warnings"] - self._raw_statements_summary_data[digest].sum_warnings,
                row["sum_timer_wait"] / 1000000.0 - self._raw_statements_summary_data[digest].sum_timer_wait,
                row["sum_lock_time"] / 1000000.0 - self._raw_statements_summary_data[digest].sum_lock_time,
                row["sum_cpu_time"] / 1000000.0 - self._raw_statements_summary_data[digest].sum_cpu_time,
                row["sum_rows_sent"] - self._raw_statements_summary_data[digest].sum_rows_sent,
                row["sum_rows_examined"] - self._raw_statements_summary_data[digest].sum_rows_examined,
                row["sum_rows_affected"] - self._raw_statements_summary_data[digest].sum_rows_affected,
            )

        self.data_to_display = new_data_to_display
        self._update_internal(query_data)
