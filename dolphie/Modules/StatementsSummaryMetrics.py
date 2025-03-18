import re
from typing import Any, Dict, List

from dolphie.DataTypes import StatementsSummaryMetricsRow as Row


class StatementsSummaryMetrics:
    def __init__(self, query_data: List[Dict[str, Any]]):
        self._original_data: Dict[str, Row] = {}
        self.cumulative_diff: Dict[str, Row] = {}
        self._raw_statements_summary_data: Dict[str, Row] = {}

        # Initialize with the first set of data
        self._initialize_original(query_data)

    def _initialize_original(self, query_data: List[Dict[str, Any]]):
        """Initialize the original data and cumulative diff from the first input."""
        for row in query_data:
            digest = row["digest"]
            query_sample_text = (
                re.sub(" +", " ", row["query_sample_text"].replace("\n", " ")) if row["query_sample_text"] else ""
            )

            # Store the original row
            original = Row(
                digest,
                row["digest_text"],
                query_sample_text,
                row["schema_name"],
                row["sum_no_good_index_used"],
                row["sum_no_index_used"],
                row["count_star"],
                row["sum_errors"],
                row["sum_warnings"],
                row["sum_timer_wait"],
                row["sum_lock_time"],
                row["sum_cpu_time"],
                row["sum_rows_sent"],
                row["sum_rows_examined"],
                row["sum_rows_affected"],
            )
            self._original_data[digest] = original

            # Initialize cumulative diff with zeros
            self.cumulative_diff[digest] = Row(
                digest, row["digest_text"], query_sample_text, row["schema_name"], 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
            )

            # Set the raw data as the initial state
            self._raw_statements_summary_data[digest] = original

    def reset(self):
        self.cumulative_diff = {}

    def update(self, query_data: List[Dict[str, Any]]):
        """Update cumulative differences based on new data."""
        for row in query_data:
            digest = row["digest"]
            query_sample_text = (
                re.sub(" +", " ", row["query_sample_text"].replace("\n", " ")) if row["query_sample_text"] else ""
            )

            # If a new digest appears, initialize it
            if digest not in self._original_data:
                self._initialize_original([row])

            # Retrieve previous raw data and calculate the individual diff
            previous = self._raw_statements_summary_data[digest]
            individual_diff = Row(
                digest,
                row["digest_text"],
                query_sample_text,
                row["schema_name"],
                row["sum_no_good_index_used"] - previous.sum_no_good_index_used,
                row["sum_no_index_used"] - previous.sum_no_index_used,
                row["count_star"] - previous.count_star,
                row["sum_errors"] - previous.sum_errors,
                row["sum_warnings"] - previous.sum_warnings,
                row["sum_timer_wait"] - previous.sum_timer_wait,
                row["sum_lock_time"] - previous.sum_lock_time,
                row["sum_cpu_time"] - previous.sum_cpu_time,
                row["sum_rows_sent"] - previous.sum_rows_sent,
                row["sum_rows_examined"] - previous.sum_rows_examined,
                row["sum_rows_affected"] - previous.sum_rows_affected,
            )

            # Update the cumulative diff by adding the individual diff
            cumulative = self.cumulative_diff.get(digest)
            if cumulative is None:
                cumulative = Row(
                    digest,
                    row["digest_text"],
                    query_sample_text,
                    row["schema_name"],
                    individual_diff.sum_no_good_index_used,
                    individual_diff.sum_no_index_used,
                    individual_diff.count_star,
                    individual_diff.sum_errors,
                    individual_diff.sum_warnings,
                    individual_diff.sum_timer_wait,
                    individual_diff.sum_lock_time,
                    individual_diff.sum_cpu_time,
                    individual_diff.sum_rows_sent,
                    individual_diff.sum_rows_examined,
                    individual_diff.sum_rows_affected,
                )

            else:
                cumulative = Row(
                    digest,
                    row["digest_text"],
                    query_sample_text,
                    row["schema_name"],
                    cumulative.sum_no_good_index_used + individual_diff.sum_no_good_index_used,
                    cumulative.sum_no_index_used + individual_diff.sum_no_index_used,
                    cumulative.count_star + individual_diff.count_star,
                    cumulative.sum_errors + individual_diff.sum_errors,
                    cumulative.sum_warnings + individual_diff.sum_warnings,
                    cumulative.sum_timer_wait + individual_diff.sum_timer_wait,
                    cumulative.sum_lock_time + individual_diff.sum_lock_time,
                    cumulative.sum_cpu_time + individual_diff.sum_cpu_time,
                    cumulative.sum_rows_sent + individual_diff.sum_rows_sent,
                    cumulative.sum_rows_examined + individual_diff.sum_rows_examined,
                    cumulative.sum_rows_affected + individual_diff.sum_rows_affected,
                )

            # Only keep rows in cumulative_diff that have a non-zero count_star
            if cumulative.count_star > 0:
                self.cumulative_diff[digest] = cumulative
            else:
                self.cumulative_diff.pop(digest, None)

            # Update raw data with current values for the next diff
            self._raw_statements_summary_data[digest] = Row(
                digest,
                row["digest_text"],
                query_sample_text,
                row["schema_name"],
                row["sum_no_good_index_used"],
                row["sum_no_index_used"],
                row["count_star"],
                row["sum_errors"],
                row["sum_warnings"],
                row["sum_timer_wait"],
                row["sum_lock_time"],
                row["sum_cpu_time"],
                row["sum_rows_sent"],
                row["sum_rows_examined"],
                row["sum_rows_affected"],
            )
