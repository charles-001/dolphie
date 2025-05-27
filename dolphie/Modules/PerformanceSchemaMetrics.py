import re
from typing import Any, Dict, List

from dolphie.Modules.Functions import minify_query


class PerformanceSchemaMetrics:
    def __init__(self, query_data: List[Dict[str, Any]], metric_name: str, metric_key: str):
        self.metric_name = metric_name
        self.metric_key = metric_key

        # These are integer columns that should be ignored for delta calculations
        self.ignore_int_columns = ["quantile_95", "quantile_99"]

        self.filtered_data: Dict[str, Dict[str, Dict[str, int]]] = {}
        self.internal_data: Dict[str, Dict[str, Dict[str, Any]]] = {
            row[self.metric_key]: {
                "event_name": row.get("EVENT_NAME"),
                "metrics": {
                    metric: {"total": value, "delta": 0, "delta_last_sample": 0}
                    for metric, value in row.items()
                    if isinstance(value, int) and metric not in self.ignore_int_columns
                },
            }
            for row in query_data
        }

        self.table_pattern = re.compile(r"([^/]+)/([^/]+)\.(frm|ibd|MYD|MYI|CSM|CSV|par)$")
        self.undo_logs_pattern = re.compile(r"undo_\d+$")

        self.events_to_combine = {
            "wait/io/file/innodb/innodb_temp_file": "Temporary files",
            "wait/io/file/sql/binlog": "Binary logs",
            "wait/io/file/sql/relaylog": "Relay logs",
            "wait/io/file/sql/io_cache": "IO cache",
            "wait/io/file/innodb/innodb_dblwr_file": "Doublewrite buffer",
            "wait/io/file/innodb/innodb_log_file": "InnoDB redo logs",
            "wait/io/file/sql/hash_join": "Hash joins",
        }

    def update_internal_data(self, query_data: List[Dict[str, int]]):
        # Track instances and remove missing ones
        current_instance_names = {row[self.metric_key] for row in query_data}
        instances_to_remove = set(self.internal_data) - current_instance_names

        # Process current query data
        for row in query_data:
            instance_name = row[self.metric_key]
            metrics = {
                metric: value
                for metric, value in row.items()
                if isinstance(value, int) and metric not in self.ignore_int_columns
            }

            # Initialize instance in internal_data if not present
            if instance_name not in self.internal_data:
                self.internal_data[instance_name] = {
                    "event_name": row.get("EVENT_NAME"),
                    "metrics": {
                        metric: {"total": value, "delta": 0, "delta_last_sample": 0}
                        for metric, value in metrics.items()
                    },
                }

            deltas_changed = False
            all_deltas_zero = True

            # Update deltas for each metric
            for metric, current_value in metrics.items():
                metric_data = self.internal_data[instance_name]["metrics"][metric]
                initial_value = metric_data["total"]
                delta = current_value - initial_value

                metric_data["total"] = current_value
                if delta > 0:
                    metric_data["delta"] += delta
                    metric_data["delta_last_sample"] = delta
                    deltas_changed = True

                if metric_data["delta"] > 0:
                    all_deltas_zero = False

            # Update filtered_data with new values if deltas changed or instance is new
            if deltas_changed or instance_name not in self.filtered_data:
                self.filtered_data[instance_name] = {}

                for metric, values in self.internal_data[instance_name]["metrics"].items():
                    # Update total with the new value (whether or not delta is positive)
                    total = values["total"]

                    # Only add delta if it's greater than 0
                    delta = values["delta"] if values["delta"] > 0 else 0
                    delta_last_sample = values["delta_last_sample"] if values["delta_last_sample"] > 0 else 0

                    # Only include the metric in filtered_data if it has a delta greater than 0
                    if delta > 0:
                        self.filtered_data[instance_name][metric] = {
                            "t": total,
                            "d": delta,
                            "d_last_sample": delta_last_sample,
                        }
                    else:
                        self.filtered_data[instance_name][metric] = {"t": total}

                    if (
                        self.metric_name == "statements_summary"
                        and "schema_name" not in self.filtered_data[instance_name]
                    ):
                        self.filtered_data[instance_name]["schema_name"] = row.get("schema_name")
                        self.filtered_data[instance_name]["digest_text"] = minify_query(row.get("digest_text"))
                        self.filtered_data[instance_name]["query_sample_text"] = minify_query(
                            row.get("query_sample_text")
                        )
                        self.filtered_data[instance_name]["quantile_95"] = row.get("quantile_95")
                        self.filtered_data[instance_name]["quantile_99"] = row.get("quantile_99")

            if all_deltas_zero:
                self.filtered_data.pop(instance_name, None)

        # Remove instances no longer in the query data
        for instance_name in instances_to_remove:
            del self.internal_data[instance_name]

        if self.metric_name == "file_io":
            self.aggregate_and_combine_data()

    def aggregate_and_combine_data(self):
        combined_results = {}

        # Aggregate deltas for combined events and instances matching the regex
        for instance_name, instance_data in self.internal_data.items():
            event_name = instance_data["event_name"]

            # Determine the target name based on instance name pattern or specific event name
            if self.undo_logs_pattern.search(instance_name):
                target_name = "Undo Logs"
            elif event_name in self.events_to_combine:
                target_name = self.events_to_combine[event_name]
            else:
                continue  # Skip if it doesn't match any pattern or event to combine

            # Remove original instance from filtered_data if it exists
            self.filtered_data.pop(instance_name, None)

            # Initialize target in combined results if not already present
            target_metrics = combined_results.setdefault(target_name, {})

            # Accumulate metrics for each matched or combined event
            for metric_name, metric_data in instance_data["metrics"].items():
                combined_metric = target_metrics.setdefault(metric_name, {"total": 0, "delta": 0})
                combined_metric["total"] += metric_data["total"]
                combined_metric["delta"] += metric_data["delta"]

        # Update filtered_data with combined results only if SUM_TIMER_WAIT delta > 0
        for combined_name, combined_metrics in combined_results.items():
            # Skip if SUM_TIMER_WAIT delta is 0
            if combined_metrics.get("SUM_TIMER_WAIT", {}).get("delta", 0) > 0:
                self.filtered_data[combined_name] = {
                    metric_name: {"t": combined_data["total"], "d": combined_data["delta"]}
                    for metric_name, combined_data in combined_metrics.items()
                }

        # Clean up filtered_data by removing instances with SUM_TIMER_WAIT delta of 0
        self.filtered_data = {
            instance_name: instance_metrics
            for instance_name, instance_metrics in self.filtered_data.items()
            if instance_metrics.get("SUM_TIMER_WAIT", {}).get("d", 0) != 0
        }
