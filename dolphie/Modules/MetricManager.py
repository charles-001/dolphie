from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List

import plotext as plt
from dolphie.Modules.Functions import format_bytes, format_number, format_time
from rich.text import Text
from textual.widgets import Static


@dataclass
class MetricSource:
    global_status = "global_status"
    innodb_metrics = "innodb_metrics"


@dataclass
class MetricData:
    label: str
    color: tuple[int, int, int]
    visible: bool
    save_history: bool
    last_value: int = None
    graphable: bool = True
    values: List[int] = field(default_factory=list)


@dataclass
class DMLMetrics:
    datetimes: List[str]
    metric_source: MetricSource
    Queries: MetricData
    Com_select: MetricData
    Com_insert: MetricData
    Com_update: MetricData
    Com_delete: MetricData
    Com_replace: MetricData
    Com_rollback: MetricData


@dataclass
class ReplicationLagMetrics:
    datetimes: List[str]
    metric_source: MetricSource
    lag: MetricData


@dataclass
class InnoDBCheckpointMetrics:
    datetimes: List[str]
    metric_source: MetricSource
    checkpoint_age: MetricData
    checkpoint_age_sync_flush: int
    checkpoint_age_max: int


@dataclass
class InnoDBActivityMetrics:
    datetimes: List[str]
    metric_source: MetricSource
    Innodb_buffer_pool_write_requests: MetricData
    Innodb_buffer_pool_read_requests: MetricData
    Innodb_buffer_pool_reads: MetricData


@dataclass
class AdaptiveHashIndexMetrics:
    datetimes: List[str]
    metric_source: str
    adaptive_hash_searches: MetricData
    adaptive_hash_searches_btree: MetricData


@dataclass
class InnoDBRedoLogMetrics:
    datetimes: List[str]
    metric_source: str
    Innodb_os_log_written: MetricData
    redo_log_size: int


class MetricManager:
    def __init__(self):
        self.worker_start_time: datetime = None
        self.worker_job_time: float = None
        self.global_variables: Dict[str, int] = None
        self.global_status: Dict[str, int] = None
        self.replication_lag: int = None

        self.metrics_dml = DMLMetrics(
            datetimes=[],
            metric_source=MetricSource.global_status,
            Queries=MetricData(label="Queries", color=(172, 207, 231), visible=False, save_history=True),
            Com_select=MetricData(label="SELECT", color=(68, 180, 255), visible=True, save_history=True),
            Com_insert=MetricData(label="INSERT", color=(84, 239, 174), visible=True, save_history=True),
            Com_update=MetricData(label="UPDATE", color=(252, 213, 121), visible=True, save_history=True),
            Com_delete=MetricData(label="DELETE", color=(255, 73, 112), visible=True, save_history=True),
            Com_replace=MetricData(
                label="REPLACE", color=(255, 73, 112), visible=False, save_history=False, graphable=False
            ),
            Com_rollback=MetricData(
                label="ROLLBACK", color=(255, 73, 112), visible=False, save_history=False, graphable=False
            ),
        )

        self.metrics_replication_lag = ReplicationLagMetrics(
            datetimes=[],
            metric_source=None,
            lag=MetricData(label="Lag", color=(68, 180, 255), visible=True, save_history=True),
        )

        self.metrics_innodb_checkpoint = InnoDBCheckpointMetrics(
            datetimes=[],
            metric_source=MetricSource.global_status,
            checkpoint_age=MetricData(label="Uncheckpointed", color=(68, 180, 255), visible=True, save_history=True),
            checkpoint_age_max=0,
            checkpoint_age_sync_flush=0,
        )

        self.metrics_innodb_activity = InnoDBActivityMetrics(
            datetimes=[],
            metric_source=MetricSource.global_status,
            Innodb_buffer_pool_read_requests=MetricData(
                label="Read Requests", color=(68, 180, 255), visible=True, save_history=True
            ),
            Innodb_buffer_pool_write_requests=MetricData(
                label="Write Requests", color=(84, 239, 174), visible=True, save_history=True
            ),
            Innodb_buffer_pool_reads=MetricData(
                label="Disk Reads", color=(255, 73, 112), visible=True, save_history=True
            ),
        )

        self.metrics_adaptive_hash_index = AdaptiveHashIndexMetrics(
            datetimes=[],
            metric_source=MetricSource.innodb_metrics,
            adaptive_hash_searches_btree=MetricData(
                label="Miss", color=(255, 73, 112), visible=True, save_history=True
            ),
            adaptive_hash_searches=MetricData(label="Hit", color=(84, 239, 174), visible=True, save_history=True),
        )

        self.metrics_innodb_redo_log = InnoDBRedoLogMetrics(
            datetimes=[],
            metric_source=MetricSource.global_status,
            Innodb_os_log_written=MetricData(
                label="Data Written/sec", color=(68, 180, 255), visible=True, save_history=True
            ),
            redo_log_size=0,
        )

        self.metrics_with_per_second_values = [
            self.metrics_dml,
            self.metrics_innodb_activity,
            self.metrics_adaptive_hash_index,
            self.metrics_innodb_redo_log,
        ]

    def refresh_data(
        self,
        worker_start_time: datetime,
        worker_job_time: float,
        global_variables: Dict[str, int],
        global_status: Dict[str, int],
        innodb_metrics: Dict[str, int],
        replication_lag: int,
    ):
        self.worker_start_time = worker_start_time
        self.worker_job_time = worker_job_time
        self.global_variables = global_variables
        self.global_status = global_status
        self.innodb_metrics = innodb_metrics
        self.replication_lag = replication_lag

    def add_metric(self, metric_data: MetricData, value: int):
        if metric_data.save_history:
            metric_data.values.append(value)

    def update_metrics_with_per_second_values(self):
        for metric_instance in self.metrics_with_per_second_values:
            added = False

            if metric_instance.metric_source == MetricSource.global_status:
                metrics_data = self.global_status
            elif metric_instance.metric_source == MetricSource.innodb_metrics:
                metrics_data = self.innodb_metrics

            for metric_name in dir(metric_instance):
                metric_data = getattr(metric_instance, metric_name)

                if isinstance(metric_data, MetricData):
                    if metric_data.last_value is None:
                        if metric_name in metrics_data:
                            metric_data.last_value = metrics_data.get(metric_name, 0)
                    else:
                        metric_status_per_sec = self.get_metric_calculate_per_sec(metric_name, format=False)
                        self.add_metric(metric_data, metric_status_per_sec)
                        added = True

            if added:
                metric_instance.datetimes.append(self.worker_start_time.strftime("%H:%M:%S"))

    def update_metrics_replication_lag(self, replication_status):
        if replication_status:
            self.add_metric(self.metrics_replication_lag.lag, self.replication_lag)
            self.metrics_replication_lag.datetimes.append(self.worker_start_time.strftime("%H:%M:%S"))

    def update_metrics_innodb_checkpoint(self):
        (
            max_checkpoint_age_bytes,
            checkpoint_age_sync_flush_bytes,
            checkpoint_age_bytes,
        ) = self.get_metric_checkpoint_age(format=False)

        self.metrics_innodb_checkpoint.checkpoint_age_max = max_checkpoint_age_bytes
        self.metrics_innodb_checkpoint.checkpoint_age_sync_flush = checkpoint_age_sync_flush_bytes

        self.add_metric(self.metrics_innodb_checkpoint.checkpoint_age, checkpoint_age_bytes)
        self.metrics_innodb_checkpoint.datetimes.append(self.worker_start_time.strftime("%H:%M:%S"))

    def get_metric_calculate_per_sec(self, metric_name, format=True):
        for metric_instance in self.metrics_with_per_second_values:
            if hasattr(metric_instance, metric_name):
                if metric_instance.metric_source == MetricSource.global_status:
                    metrics_data = self.global_status
                elif metric_instance.metric_source == MetricSource.innodb_metrics:
                    metrics_data = self.innodb_metrics

                last_value = getattr(metric_instance, metric_name).last_value
                metric_diff = metrics_data.get(metric_name, 0) - last_value
                metric_per_sec = round(metric_diff / self.worker_job_time)

                if format:
                    return format_number(metric_per_sec)
                else:
                    return metric_per_sec

    def get_metric_checkpoint_age(self, format):
        innodb_log_files_in_group = self.global_variables.get("innodb_log_files_in_group", 1)
        checkpoint_age_bytes = round(self.global_status.get("Innodb_checkpoint_age", 0))
        max_checkpoint_age_bytes = round(
            self.global_variables.get("innodb_log_file_size", 0) * innodb_log_files_in_group
        )

        self.metrics_innodb_redo_log.redo_log_size = max_checkpoint_age_bytes

        if checkpoint_age_bytes == 0 and max_checkpoint_age_bytes == 0:
            return "N/A"

        checkpoint_age_sync_flush_bytes = round(max_checkpoint_age_bytes * 0.825)
        checkpoint_age_ratio = round(checkpoint_age_bytes / max_checkpoint_age_bytes * 100, 2)

        if format:
            if checkpoint_age_ratio >= 80:
                color_code = "#fc7979"
            elif checkpoint_age_ratio >= 60:
                color_code = "#f1fb82"
            else:
                color_code = "#54efae"

            return f"[{color_code}]{checkpoint_age_ratio}%"
        else:
            return max_checkpoint_age_bytes, checkpoint_age_sync_flush_bytes, checkpoint_age_bytes

    def get_metric_adaptive_hash_index(self):
        if self.global_variables.get("innodb_adaptive_hash_index") == "OFF":
            return "OFF"

        # Get per second value
        previous_hits = self.metrics_adaptive_hash_index.adaptive_hash_searches.last_value
        previous_misses = self.metrics_adaptive_hash_index.adaptive_hash_searches_btree.last_value
        current_hits = self.innodb_metrics.get("adaptive_hash_searches", None)
        current_misses = self.innodb_metrics.get("adaptive_hash_searches_btree", None)

        if current_hits is None or current_misses is None:
            return "N/A"

        hits = current_hits - previous_hits
        misses = current_misses - previous_misses

        if hits == 0 and misses == 0:
            return "Inactive"

        efficiency = (hits / (hits + misses)) * 100

        if efficiency > 70:
            color_code = "#54efae"
        elif efficiency > 50:
            color_code = "#f1fb82"
        else:
            color_code = "#fc7979"

        return f"[{color_code}]{efficiency:.2f}%"

    def update_metrics_with_last_value(self):
        # We set the last value for specific metrics that need it so they can get per second values
        for metric_instance in self.metrics_with_per_second_values:
            if metric_instance.metric_source == MetricSource.global_status:
                metrics_data = self.global_status
            elif metric_instance.metric_source == MetricSource.innodb_metrics:
                metrics_data = self.innodb_metrics

            for metric_name in dir(metric_instance):
                metric_data = getattr(metric_instance, metric_name)
                if isinstance(metric_data, MetricData):
                    metric_data.last_value = metrics_data.get(metric_name, 0)


class Graph(Static):
    def __init__(self, bar=False, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.data = None
        self.bar = bar

    def on_show(self) -> None:
        self.render_graph(self.data)

    def on_resize(self) -> None:
        self.render_graph(self.data)

    def render_graph(self, data) -> None:
        self.data = data

        plt.clf()
        plt.date_form("H:M:S")
        plt.canvas_color((3, 9, 24))
        plt.axes_color((3, 9, 24))
        plt.ticks_color((144, 169, 223))

        plt.plotsize(self.size.width, self.size.height)

        max_y_value = 0
        if type(self.data) == InnoDBCheckpointMetrics:
            x = self.data.datetimes
            y = self.data.checkpoint_age.values

            if y:
                plt.hline(0, (3, 9, 24))
                plt.hline(self.data.checkpoint_age_sync_flush, (241, 251, 130))
                plt.hline(self.data.checkpoint_age_max, (252, 121, 121))
                plt.text(
                    "Critical",
                    y=self.data.checkpoint_age_max,
                    x=max(x),
                    alignment="right",
                    color="white",
                    style="bold",
                )
                plt.text(
                    "Warning",
                    y=self.data.checkpoint_age_sync_flush,
                    x=max(x),
                    alignment="right",
                    color="white",
                    style="bold",
                )
                plt.text(
                    format_bytes(y[0], color=False), y=max(y), x=max(x), alignment="right", color="white", style="bold"
                )

                plt.plot(
                    x,
                    y,
                    marker="braille",
                    label=self.data.checkpoint_age.label,
                    color=self.data.checkpoint_age.color,
                )
                max_y_value = self.data.checkpoint_age_max
        elif type(self.data) == InnoDBRedoLogMetrics and self.bar:
            if self.data.Innodb_os_log_written.values:
                x = [0]
                y = [
                    round(
                        sum(self.data.Innodb_os_log_written.values)
                        * (3600 / len(self.data.Innodb_os_log_written.values))
                    )
                ]

                plt.hline(self.data.redo_log_size, (252, 121, 121))
                plt.text(
                    "Log Size",
                    y=self.data.redo_log_size,
                    x=0,
                    alignment="center",
                    color="white",
                    style="bold",
                )

                bar_color = (46, 124, 175)
                if y[0] >= self.data.redo_log_size:
                    bar_color = (252, 121, 121)

                plt.text(
                    format_bytes(y[0], color=False) + "/hr",
                    y=y[0],
                    x=0,
                    alignment="center",
                    color="white",
                    style="bold",
                    background=bar_color,
                )

                plt.bar(
                    x,
                    y,
                    marker="hd",
                    color=bar_color,
                )
                max_y_value = max(self.data.redo_log_size, max(y))
        else:
            for metric_data in self.data.__dict__.values():
                if isinstance(metric_data, MetricData) and metric_data.visible:
                    x = self.data.datetimes
                    y = metric_data.values

                    if y:
                        plt.plot(x, y, marker="braille", label=metric_data.label, color=metric_data.color)
                        max_y_value = max(max_y_value, max(y))

        max_y_ticks = 5
        y_tick_interval = max_y_value / max_y_ticks

        if y_tick_interval >= 1:
            y_ticks = [i * y_tick_interval for i in range(max_y_ticks + 1) if i * y_tick_interval >= 0]
        else:
            y_ticks = [i for i in range(max_y_value + 1)]

        data_formatters = {
            ReplicationLagMetrics: lambda val: format_time(val),
            DMLMetrics: lambda val: format_number(val, for_plot=True, decimal=1),
            InnoDBActivityMetrics: lambda val: format_number(val, for_plot=True, decimal=1),
            InnoDBCheckpointMetrics: lambda val: format_bytes(val, color=False),
            AdaptiveHashIndexMetrics: lambda val: format_number(val, for_plot=True, decimal=1),
            InnoDBRedoLogMetrics: lambda val: format_bytes(val, color=False),
        }

        data_type = type(self.data)
        if data_type in data_formatters:
            y_labels = [data_formatters[data_type](val) for val in y_ticks]
        else:
            y_labels = [val for val in y_ticks]

        plt.yticks(y_ticks, y_labels)

        self.update(Text.from_ansi(plt.build()))
