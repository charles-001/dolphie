import re
from datetime import datetime, timedelta
from typing import Any, Dict, List


class TableIOWaitsByTable:
    def __init__(self, query_data: List[Dict[str, Any]], daemon_mode: bool):
        self.daemon_mode = daemon_mode
        self.tracked_data: Dict[str, Dict[str, Dict[str, Any]]] = {
            row["TABLE_NAME"]: {
                metric: {"total": value, "delta": 0} for metric, value in row.items() if isinstance(value, int)
            }
            for row in query_data
        }
        self.filtered_data: Dict[str, Dict[str, Dict[str, int]]] = {}
        self.last_reset_time = datetime.now()

    def update_tracked_data(self, query_data: List[Dict[str, int]]):
        if self.daemon_mode and datetime.now() - self.last_reset_time >= timedelta(minutes=10):
            self.reset_deltas()
            self.last_reset_time = datetime.now()

        current_table_names = {row["TABLE_NAME"] for row in query_data}
        self.remove_missing_tables(current_table_names)

        for row in query_data:
            self.process_row(row)

    def process_row(self, row: Dict[str, int]):
        table_name = row["TABLE_NAME"]
        metrics = {metric: value for metric, value in row.items() if isinstance(value, int)}

        if table_name not in self.tracked_data:
            self.tracked_data[table_name] = {metric: {"total": value, "delta": 0} for metric, value in metrics.items()}

        deltas_changed = self.update_deltas(table_name, metrics)

        if deltas_changed or table_name not in self.filtered_data:
            self.update_filtered_data(table_name)

    def update_deltas(self, table_name: str, metrics: Dict[str, int]) -> bool:
        deltas_changed = False
        for metric, current_value in metrics.items():
            metric_data = self.tracked_data[table_name][metric]
            delta = current_value - metric_data["total"]
            metric_data["total"] = current_value
            if delta > 0:
                metric_data["delta"] += delta
                deltas_changed = True
        return deltas_changed

    def update_filtered_data(self, table_name: str):
        self.filtered_data[table_name] = {
            metric: {"total": data["total"], "delta": data["delta"]}
            for metric, data in self.tracked_data[table_name].items()
            if data["delta"] > 0
        }
        if not any(data["delta"] > 0 for data in self.tracked_data[table_name].values()):
            self.filtered_data.pop(table_name, None)

    def remove_missing_tables(self, current_table_names: set):
        for table_name in set(self.tracked_data) - current_table_names:
            self.tracked_data.pop(table_name)

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
        self.tracked_data: Dict[str, Dict[str, Any]] = {
            row["FILE_NAME"]: {
                "event_name": row["EVENT_NAME"],
                "metrics": {
                    metric: {"total": value, "delta": 0} for metric, value in row.items() if isinstance(value, int)
                },
            }
            for row in query_data
        }
        self.filtered_data: Dict[str, Dict[str, Dict[str, int]]] = {}
        self.last_reset_time = datetime.now()
        self.combined_table_pattern = re.compile(r"([^/]+)/([^/]+)\.(frm|ibd|MYD|MYI|CSM|CSV|par)$")
        self.events_to_combine = {
            "wait/io/file/innodb/innodb_temp_file": "Temporary tables",
            "wait/io/file/sql/binlog": "Binary logs",
            "wait/io/file/sql/relaylog": "Relay logs",
            "wait/io/file/sql/io_cache": "IO cache",
            "wait/io/file/innodb/innodb_dblwr_file": "Doublewrite buffer",
            "wait/io/file/innodb/innodb_log_file": "InnoDB redo log files",
        }

    def update_tracked_data(self, query_data: List[Dict[str, int]]):
        if self.daemon_mode and datetime.now() - self.last_reset_time >= timedelta(minutes=10):
            self.reset_deltas()
            self.last_reset_time = datetime.now()

        current_file_names = {row["FILE_NAME"] for row in query_data}
        self.remove_missing_files(current_file_names)

        for row in query_data:
            self.process_row(row)

        self.aggregate_combined_events()

    def process_row(self, row: Dict[str, int]):
        file_name = row["FILE_NAME"]
        event_name = row["EVENT_NAME"]
        metrics = {metric: value for metric, value in row.items() if isinstance(value, int)}

        if file_name not in self.tracked_data:
            self.tracked_data[file_name] = {
                "event_name": event_name,
                "metrics": {metric: {"total": value, "delta": 0} for metric, value in metrics.items()},
            }

        deltas_changed = self.update_deltas(file_name, metrics)

        if deltas_changed or file_name not in self.filtered_data:
            self.update_filtered_data(file_name)

    def update_deltas(self, file_name: str, metrics: Dict[str, int]) -> bool:
        deltas_changed = False
        for metric, current_value in metrics.items():
            metric_data = self.tracked_data[file_name]["metrics"][metric]
            delta = current_value - metric_data["total"]
            metric_data["total"] = current_value
            if delta > 0:
                metric_data["delta"] += delta
                deltas_changed = True
        return deltas_changed

    def update_filtered_data(self, file_name: str):
        self.filtered_data[file_name] = {
            metric: {"total": data["total"], "delta": data["delta"]}
            for metric, data in self.tracked_data[file_name]["metrics"].items()
            if data["delta"] > 0
        }
        if not any(data["delta"] > 0 for data in self.tracked_data[file_name]["metrics"].values()):
            self.filtered_data.pop(file_name, None)

    def remove_missing_files(self, current_file_names: set):
        for file_name in set(self.tracked_data) - current_file_names:
            self.tracked_data.pop(file_name)

    def aggregate_combined_events(self):
        combined_results = {name: {} for name in self.events_to_combine.values()}
        combined_files = set()

        for event_name, combined_name in self.events_to_combine.items():
            for file_name, file_data in self.tracked_data.items():
                if file_data["event_name"] == event_name:
                    combined_files.add(file_name)
                    for metric, data in file_data["metrics"].items():
                        if metric not in combined_results[combined_name]:
                            combined_results[combined_name][metric] = {"total": 0, "delta": 0}
                        combined_results[combined_name][metric]["total"] += data["total"]
                        combined_results[combined_name][metric]["delta"] += data["delta"]

        for combined_name, metrics in combined_results.items():
            for metric, values in metrics.items():
                if values["delta"] > 0:
                    self.filtered_data.setdefault(combined_name, {})[metric] = values

        for file_name in combined_files:
            self.filtered_data.pop(file_name, None)

    def reset_deltas(self):
        for file_data in self.tracked_data.values():
            for metric_data in file_data["metrics"].values():
                metric_data["delta"] = 0
        for file_data in self.filtered_data.values():
            for metric_data in file_data.values():
                metric_data["delta"] = 0
