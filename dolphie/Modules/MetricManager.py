from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List

import plotext as plt
from dolphie.Modules.Functions import format_bytes, format_number, format_time
from rich.ansi import AnsiDecoder
from rich.console import Group
from rich.jupyter import JupyterMixin


@dataclass
class MetricData:
    label: str
    color: tuple[int, int, int]
    visible: bool
    save_history: bool
    last_value: int = None
    values: List[int] = field(default_factory=list)


@dataclass
class GlobalStatusMetrics:
    datetimes: List[str]
    Queries: MetricData
    Com_select: MetricData
    Com_insert: MetricData
    Com_update: MetricData
    Com_delete: MetricData
    Com_replace: MetricData
    Com_rollback: MetricData


@dataclass
class ReplicaLagMetrics:
    datetimes: List[str]
    lag: MetricData


@dataclass
class InnoDBCheckpointMetrics:
    datetimes: List[str]
    checkpoint_age: MetricData
    checkpoint_age_sync_flush: int
    checkpoint_age_max: int


class MetricManager:
    def __init__(self):
        self.worker_start_time: datetime = None
        self.worker_job_time: float = None
        self.global_variables: Dict[str, int] = None
        self.global_status: Dict[str, int] = None
        self.replica_lag: int = None

        self.global_status_metrics = GlobalStatusMetrics(
            datetimes=[],
            Queries=MetricData(label="Queries", color=(172, 207, 231), visible=False, save_history=True),
            Com_select=MetricData(label="SELECT", color=(68, 180, 255), visible=True, save_history=True),
            Com_insert=MetricData(label="INSERT", color=(84, 239, 174), visible=True, save_history=True),
            Com_update=MetricData(label="UPDATE", color=(252, 213, 121), visible=True, save_history=True),
            Com_delete=MetricData(label="DELETE", color=(255, 73, 185), visible=True, save_history=True),
            Com_replace=MetricData(label="REPLACE", color=(255, 73, 185), visible=False, save_history=False),
            Com_rollback=MetricData(label="ROLLBACK", color=(255, 73, 185), visible=False, save_history=False),
        )

        self.replica_lag_metrics = ReplicaLagMetrics(
            datetimes=[],
            lag=MetricData(label="Lag", color=(68, 180, 255), visible=False, save_history=True),
        )

        self.innodb_checkpoint_metrics = InnoDBCheckpointMetrics(
            datetimes=[datetime.now().strftime("%H:%M:%S")],
            checkpoint_age=MetricData(label="Age", color=(68, 180, 255), visible=True, save_history=True, values=[0]),
            checkpoint_age_max=0,
            checkpoint_age_sync_flush=0,
        )

    def refresh_data(
        self,
        worker_start_time: datetime,
        worker_job_time: float,
        global_variables: Dict[str, int],
        global_status: Dict[str, int],
        replica_lag: int,
    ):
        self.worker_start_time = worker_start_time
        self.worker_job_time = worker_job_time
        self.global_variables = global_variables
        self.global_status = global_status
        self.replica_lag = replica_lag

    def add_metric(self, metric: MetricData, value: int):
        if metric.save_history:
            metric.values.append(value)

    def update_global_status_metrics(self):
        for metric_name in dir(self.global_status_metrics):
            metric = getattr(self.global_status_metrics, metric_name)
            if isinstance(metric, MetricData):
                if metric.last_value is None:
                    metric.last_value = self.global_status[metric_name]
                else:
                    self.add_metric(metric, self.metric_status_per_sec(metric_name))

        self.global_status_metrics.datetimes.append(self.worker_start_time.strftime("%H:%M:%S"))

    def update_replica_lag_metrics(self, replication_status):
        if replication_status:
            self.add_metric(self.replica_lag_metrics.lag, self.replica_lag)

            self.replica_lag_metrics.datetimes.append(self.worker_start_time.strftime("%H:%M:%S"))

    def update_last_value_metrics(self):
        # We set the last value for specific metrics that need it so they can get per second values
        for metric_name in dir(self.global_status_metrics):
            metric = getattr(self.global_status_metrics, metric_name)
            if isinstance(metric, MetricData):
                metric.last_value = self.global_status[metric_name]

    def metric_status_per_sec(self, metric_name: str, format=False):
        last_value = getattr(self.global_status_metrics, metric_name).last_value
        metric_diff = self.global_status[metric_name] - last_value
        metric_per_sec = round(metric_diff / self.worker_job_time)

        if format:
            return format_number(metric_per_sec)
        else:
            return metric_per_sec

    def metric_checkpoint_age(self, save_metric=False):
        CHECKPOINT_AGE_FORMATS = {
            "high": "[#fc7979]%s%%",
            "medium": "[#f1fb82]%s%%",
            "low": "[#54efae]%s%%",
        }

        # MariaDB support
        innodb_log_files_in_group = self.global_variables.get("innodb_log_files_in_group", 1)

        checkpoint_age_bytes = round(self.global_status["Innodb_checkpoint_age"])
        max_checkpoint_age_bytes = round(self.global_variables["innodb_log_file_size"] * innodb_log_files_in_group)
        checkpoint_age_sync_flush_bytes = round(max_checkpoint_age_bytes * 0.825)
        checkpoint_age_ratio = checkpoint_age_bytes / max_checkpoint_age_bytes * 100

        if not save_metric:
            if checkpoint_age_ratio >= 80:
                checkpoint_age_format = CHECKPOINT_AGE_FORMATS["high"]
            elif checkpoint_age_ratio >= 60:
                checkpoint_age_format = CHECKPOINT_AGE_FORMATS["medium"]
            else:
                checkpoint_age_format = CHECKPOINT_AGE_FORMATS["low"]

            return checkpoint_age_format % checkpoint_age_ratio
        else:
            self.innodb_checkpoint_metrics.checkpoint_age_max = max_checkpoint_age_bytes
            self.innodb_checkpoint_metrics.checkpoint_age_sync_flush = checkpoint_age_sync_flush_bytes

            self.add_metric(self.innodb_checkpoint_metrics.checkpoint_age, checkpoint_age_bytes)
            self.innodb_checkpoint_metrics.datetimes.append(self.worker_start_time.strftime("%H:%M:%S"))

    def create_dml_qps_graph(self):
        return CreateGraph(self.global_status_metrics)

    def create_replica_lag_graph(self):
        return CreateGraph(self.replica_lag_metrics)

    def create_innodb_checkpoint_graph(self):
        return CreateGraph(self.innodb_checkpoint_metrics)


class CreateGraph(JupyterMixin):
    def __init__(self, graph_data):
        self.graph_data = graph_data

    def __rich_console__(self, console, options):
        width = options.max_width or console.width
        height = 15
        max_y_value = 0

        plt.clf()

        plt.date_form("H:M:S")
        plt.canvas_color((3, 9, 24))
        plt.axes_color((3, 9, 24))
        plt.ticks_color((144, 169, 223))

        plt.plotsize(width, height)

        if isinstance(self.graph_data, ReplicaLagMetrics):
            x = self.graph_data.datetimes
            y = self.graph_data.lag.values

            if y:
                plt.plot(x, y, marker="braille", label=self.graph_data.lag.label, color=self.graph_data.lag.color)
                max_y_value = max(max_y_value, max(y))
        elif isinstance(self.graph_data, GlobalStatusMetrics):
            for metric_data in self.graph_data.__dict__.values():
                if isinstance(metric_data, MetricData) and metric_data.visible:
                    x = self.graph_data.datetimes
                    y = metric_data.values

                    if y:
                        plt.plot(x, y, marker="braille", label=metric_data.label, color=metric_data.color)
                        max_y_value = max(max_y_value, max(y))
        elif isinstance(self.graph_data, InnoDBCheckpointMetrics):
            x = self.graph_data.datetimes
            y = self.graph_data.checkpoint_age.values

            if y:
                plt.hline(self.graph_data.checkpoint_age_sync_flush, (241, 251, 130))
                plt.hline(self.graph_data.checkpoint_age_max, (252, 121, 121))
                plt.text(
                    "Critical",
                    y=self.graph_data.checkpoint_age_max,
                    x=max(x),
                    alignment="right",
                    color="white",
                    style="bold",
                )
                plt.text(
                    "Warning",
                    y=self.graph_data.checkpoint_age_sync_flush,
                    x=max(x),
                    alignment="right",
                    color="white",
                    style="bold",
                )

                plt.plot(
                    x,
                    y,
                    marker="braille",
                    label=self.graph_data.checkpoint_age.label,
                    color=self.graph_data.checkpoint_age.color,
                )
                max_y_value = self.graph_data.checkpoint_age_max

        # I create my own y ticks to format the numbers how I like them
        max_y_ticks = 5

        y_tick_interval = max_y_value / max_y_ticks
        if y_tick_interval >= 1:
            y_ticks = [i * y_tick_interval for i in range(max_y_ticks + 1) if i * y_tick_interval >= 0]
            if isinstance(self.graph_data, ReplicaLagMetrics):
                y_labels = [format_time(val) for val in y_ticks]
            elif isinstance(self.graph_data, GlobalStatusMetrics):
                y_labels = [format_number(val, for_plot=True, decimal=1) for val in y_ticks]
            elif isinstance(self.graph_data, InnoDBCheckpointMetrics):
                y_labels = [format_bytes(val, format=False) for val in y_ticks]
        else:
            y_ticks = [i for i in range(int(max_y_value) + 1)]
            if isinstance(self.graph_data, ReplicaLagMetrics):
                y_labels = [format_time(val) for val in y_ticks]
            elif isinstance(self.graph_data, GlobalStatusMetrics):
                y_labels = [format_number(val, for_plot=True, decimal=1) for val in y_ticks]
            elif isinstance(self.graph_data, InnoDBCheckpointMetrics):
                y_labels = [format_bytes(val, format=False) for val in y_ticks]

        plt.yticks(y_ticks, y_labels)

        yield Group(*AnsiDecoder().decode(plt.build()))
