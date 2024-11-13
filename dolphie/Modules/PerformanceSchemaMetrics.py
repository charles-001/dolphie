import re
from datetime import datetime, timedelta
from typing import Any, Dict, List


class TableIOWaitsByTable:
    def __init__(self, query_data: List[Dict[str, Any]], daemon_mode: bool):
        self.daemon_mode = daemon_mode
        self.tracked_data: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self.filtered_data: Dict[str, Dict[str, Dict[str, int]]] = {}
        self.last_reset_time: datetime = datetime.now()

        # Initialize the tracked data from query_data
        for row in query_data:
            self.tracked_data[row["TABLE_NAME"]] = {
                metric: {"total": value, "delta": 0} for metric, value in row.items() if isinstance(value, int)
            }

    def update_tracked_data(self, query_data: List[Dict[str, int]]):
        current_time = datetime.now()

        if self.daemon_mode and current_time - self.last_reset_time >= timedelta(minutes=10):
            self.reset_deltas()
            self.last_reset_time = current_time

        # Track current file names and remove missing ones
        current_table_names = {row["TABLE_NAME"] for row in query_data}
        tables_to_remove = set(self.tracked_data) - current_table_names

        # Process current query data
        for row in query_data:
            table_name = row["TABLE_NAME"]
            metrics = {metric: value for metric, value in row.items() if isinstance(value, int)}

            # Initialize file in tracked_data if not present
            if table_name not in self.tracked_data:
                self.tracked_data[table_name] = {
                    metric: {"total": value, "delta": 0} for metric, value in metrics.items()
                }

            deltas_changed = False
            all_deltas_zero = True

            # Update deltas for each metric
            for metric, current_value in metrics.items():
                metric_data = self.tracked_data[table_name][metric]
                initial_value = metric_data["total"]
                delta = current_value - initial_value

                metric_data["total"] = current_value
                if delta > 0:
                    metric_data["delta"] += delta
                    deltas_changed = True

                if metric_data["delta"] > 0:
                    all_deltas_zero = False

            # Update filtered_data if necessary
            if deltas_changed or table_name not in self.filtered_data:
                self.filtered_data[table_name] = {}

                for metric, values in self.tracked_data[table_name].items():
                    # Update total with the new value (whether or not delta is positive)
                    total = values["total"]

                    # Only add delta if it's greater than 0
                    delta = values["delta"] if values["delta"] > 0 else 0

                    # Only include the metric in filtered_data if it has a delta greater than 0
                    if delta > 0:
                        self.filtered_data[table_name][metric] = {"total": total, "delta": delta}
                    else:
                        # If delta is 0, you may decide to only store the total or skip it.
                        self.filtered_data[table_name][metric] = {"total": total}

            if all_deltas_zero:
                self.filtered_data.pop(table_name, None)

        # Remove tables no longer in the query data
        for table_name in tables_to_remove:
            del self.tracked_data[table_name]

    def reset_deltas(self):
        for table_data in self.tracked_data.values():
            for metric_data in table_data.values():
                metric_data["delta"] = 0

        for table_data in self.filtered_data.values():
            for metric_data in table_data.values():
                metric_data["delta"] = 0


class FileIOByInstance:
    def __init__(self, query_data: List[Dict[str, Any]], daemon_mode: bool):
        self.daemon_mode = daemon_mode
        self.tracked_data: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self.filtered_data: Dict[str, Dict[str, Dict[str, int]]] = {}
        self.last_reset_time: datetime = datetime.now()
        self.combined_table_pattern = re.compile(r"([^/]+)/([^/]+)\.(frm|ibd|MYD|MYI|CSM|CSV|par)$")
        self.events_to_combine = {
            "wait/io/file/innodb/innodb_temp_file": "Temporary tables",
            "wait/io/file/sql/binlog": "Binary logs",
            "wait/io/file/sql/relaylog": "Relay logs",
            "wait/io/file/sql/io_cache": "IO cache",
            "wait/io/file/innodb/innodb_dblwr_file": "Doublewrite buffer",
            "wait/io/file/innodb/innodb_log_file": "InnoDB redo log files",
        }

        # Initialize the tracked data from query_data
        for row in query_data:
            self.tracked_data[row["FILE_NAME"]] = {
                "event_name": row["EVENT_NAME"],
                "metrics": {
                    metric: {"total": value, "delta": 0} for metric, value in row.items() if isinstance(value, int)
                },
            }

    def update_tracked_data(self, query_data: List[Dict[str, int]]):
        current_time = datetime.now()

        if self.daemon_mode and current_time - self.last_reset_time >= timedelta(minutes=10):
            self.reset_deltas()
            self.last_reset_time = current_time

        # Track current file names and remove missing ones
        current_file_names = {row["FILE_NAME"] for row in query_data}
        files_to_remove = set(self.tracked_data) - current_file_names

        # Process current query data
        for row in query_data:
            file_name = row["FILE_NAME"]
            event_name = row["EVENT_NAME"]
            metrics = {metric: value for metric, value in row.items() if isinstance(value, int)}

            # Initialize file in tracked_data if not present
            if file_name not in self.tracked_data:
                self.tracked_data[file_name] = {
                    "event_name": event_name,
                    "metrics": {metric: {"total": value, "delta": 0} for metric, value in metrics.items()},
                }

            deltas_changed = False
            all_deltas_zero = True

            # Update deltas for each metric
            for metric, current_value in metrics.items():
                metric_data = self.tracked_data[file_name]["metrics"][metric]
                initial_value = metric_data["total"]
                delta = current_value - initial_value

                metric_data["total"] = current_value
                if delta > 0:
                    metric_data["delta"] += delta
                    deltas_changed = True

                if metric_data["delta"] > 0:
                    all_deltas_zero = False

            # Update filtered_data if necessary
            if deltas_changed or file_name not in self.filtered_data:
                self.filtered_data[file_name] = {}

                for metric, values in self.tracked_data[file_name]["metrics"].items():
                    # Update total with the new value (whether or not delta is positive)
                    total = values["total"]

                    # Only add delta if it's greater than 0
                    delta = values["delta"] if values["delta"] > 0 else 0

                    # Only include the metric in filtered_data if it has a delta greater than 0
                    if delta > 0:
                        self.filtered_data[file_name][metric] = {"total": total, "delta": delta}
                    else:
                        # If delta is 0, you may decide to only store the total or skip it.
                        self.filtered_data[file_name][metric] = {"total": total}

            if all_deltas_zero:
                self.filtered_data.pop(file_name, None)

        # Remove files no longer in the query data
        for file_name in files_to_remove:
            del self.tracked_data[file_name]

        self.aggregate_combined_events()

    def aggregate_combined_events(self):
        combined_results = {}
        combined_files = set()

        # Initialize combined results for events
        for event_name, combined_name in self.events_to_combine.items():
            if combined_name not in combined_results:
                combined_results[combined_name] = {}

        # Aggregate deltas for combined events
        for event_name, combined_name in self.events_to_combine.items():
            for file_name, file_data in self.tracked_data.items():
                if file_data["event_name"] == event_name:
                    combined_files.add(file_name)

                    if combined_name not in combined_results:
                        combined_results[combined_name] = {}

                    for metric_name, metric_data in file_data["metrics"].items():
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
        for file_name in combined_files:
            self.filtered_data.pop(file_name, None)

    def reset_deltas(self):
        for file_data in self.tracked_data.values():
            for metric_data in file_data["metrics"].values():
                metric_data["delta"] = 0

        for file_data in self.filtered_data.values():
            for metric_data in file_data.values():
                metric_data["delta"] = 0
