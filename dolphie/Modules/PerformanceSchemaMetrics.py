import re
from typing import Any, Dict, List


class PerformanceSchemaMetrics:
    def __init__(self, query_data: List[Dict[str, Any]], combine_events: bool = False):
        self.combine_events = combine_events
        self.filtered_data: Dict[str, Dict[str, Dict[str, int]]] = {}
        self.internal_data: Dict[str, Dict[str, Dict[str, Any]]] = {
            row["NAME"]: {
                "event_name": row.get("EVENT_NAME"),
                "metrics": {
                    metric: {"total": value, "delta": 0} for metric, value in row.items() if isinstance(value, int)
                },
            }
            for row in query_data
        }

        self.combined_table_pattern = re.compile(r"([^/]+)/([^/]+)\.(frm|ibd|MYD|MYI|CSM|CSV|par)$")
        self.events_to_combine = {
            "wait/io/file/innodb/innodb_temp_file": "Temporary tables",
            "wait/io/file/sql/binlog": "Binary logs",
            "wait/io/file/sql/relaylog": "Relay logs",
            "wait/io/file/sql/io_cache": "IO cache",
            "wait/io/file/innodb/innodb_dblwr_file": "Doublewrite buffer",
            "wait/io/file/innodb/innodb_log_file": "InnoDB redo log files",
        }

    def update_internal_data(self, query_data: List[Dict[str, int]]):
        # Track instances and remove missing ones
        current_instance_names = {row["NAME"] for row in query_data}
        instances_to_remove = set(self.internal_data) - current_instance_names

        # Process current query data
        for row in query_data:
            instance_name = row["NAME"]
            metrics = {metric: value for metric, value in row.items() if isinstance(value, int)}

            # Initialize instance in internal_data if not present
            if instance_name not in self.internal_data:
                self.internal_data[instance_name] = {
                    "event_name": row.get("EVENT_NAME"),
                    "metrics": {metric: {"total": value, "delta": 0} for metric, value in metrics.items()},
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

                    # Only include the metric in filtered_data if it has a delta greater than 0
                    if delta > 0:
                        self.filtered_data[instance_name][metric] = {"total": total, "delta": delta}
                    else:
                        self.filtered_data[instance_name][metric] = {"total": total}

            if all_deltas_zero:
                self.filtered_data.pop(instance_name, None)

        # Remove instances no longer in the query data
        for instance_name in instances_to_remove:
            del self.internal_data[instance_name]

        if self.combine_events:
            self.aggregate_combined_events()

    def aggregate_combined_events(self):
        combined_results = {}
        combined_instances = set()

        # Initialize combined results for events
        for event_name, combined_name in self.events_to_combine.items():
            if combined_name not in combined_results:
                combined_results[combined_name] = {}

        # Aggregate deltas for combined events
        for event_name, combined_name in self.events_to_combine.items():
            for instance_name, instance_data in self.internal_data.items():
                if instance_data["event_name"] == event_name:
                    combined_instances.add(instance_name)

                    if combined_name not in combined_results:
                        combined_results[combined_name] = {}

                    for metric_name, metric_data in instance_data["metrics"].items():
                        if metric_name not in combined_results[combined_name]:
                            combined_results[combined_name][metric_name] = {"total": 0, "delta": 0}

                        # Accumulate deltas for the combined event
                        combined_results[combined_name][metric_name]["total"] += metric_data["total"]
                        combined_results[combined_name][metric_name]["delta"] += metric_data["delta"]

        # Update filtered_data with combined results
        for combined_name, combined_metrics in combined_results.items():
            for metric_name, combined_data in combined_metrics.items():
                if combined_data["delta"] > 0:
                    if combined_name not in self.filtered_data:
                        self.filtered_data[combined_name] = {}

                    self.filtered_data[combined_name][metric_name] = {
                        "total": combined_data["total"],
                        "delta": combined_data["delta"],
                    }

        # Remove combined events from filtered_data
        for instance_name in combined_instances:
            self.filtered_data.pop(instance_name, None)
