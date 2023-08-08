from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List

import plotext as plt
from dolphie.Functions import format_number, format_time
from rich.ansi import AnsiDecoder
from rich.console import Group
from rich.jupyter import JupyterMixin


@dataclass
class MetricData:
    key: str
    color: tuple
    visible: bool
    values: List[int]


@dataclass
class GlobalStatusMetrics:
    datetimes: List[str]
    queries: MetricData
    select: MetricData
    insert: MetricData
    update: MetricData
    delete: MetricData


@dataclass
class ReplicaLagMetrics:
    datetimes: List[str]
    lag: MetricData


class MetricManager:
    def __init__(self):
        self.worker_start_time: datetime = None
        self.worker_job_time: float = None
        self.global_status: Dict[str, int] = None
        self.global_saved_status: Dict[str, int] = None
        self.replica_lag: int = None

        self.global_status_metrics = GlobalStatusMetrics(
            datetimes=[],
            queries=MetricData(key="Queries", color=(172, 207, 231), visible=False, values=[]),
            select=MetricData(key="Com_select", color=(68, 180, 255), visible=True, values=[]),
            insert=MetricData(key="Com_insert", color=(84, 239, 174), visible=True, values=[]),
            update=MetricData(key="Com_update", color=(252, 213, 121), visible=True, values=[]),
            delete=MetricData(key="Com_delete", color=(255, 73, 185), visible=True, values=[]),
        )
        self.replica_lag_metrics = ReplicaLagMetrics(
            datetimes=[],
            lag=MetricData(key=None, color=(68, 180, 255), visible=None, values=[]),
        )

    def refresh_data(
        self,
        worker_start_time: datetime,
        worker_job_time: float,
        status: Dict[str, int],
        saved_status: Dict[str, int],
        replica_lag: int,
    ):
        self.worker_start_time = worker_start_time
        self.worker_job_time = worker_job_time
        self.status = status
        self.previous_status = saved_status
        self.replica_lag = replica_lag

    def update_global_status_metrics(self):
        for metric_data in self.global_status_metrics.__dict__.values():
            if not self.previous_status:
                return

            if isinstance(metric_data, MetricData):
                metric_data.values.append(self.calculate_datapoint(metric_data.key))

        self.global_status_metrics.datetimes.append(self.worker_start_time.strftime("%H:%M:%S"))

    def update_replica_lag_metrics(self):
        self.replica_lag_metrics.lag.values.append(self.replica_lag)
        self.replica_lag_metrics.datetimes.append(self.worker_start_time.strftime("%H:%M:%S"))

    def calculate_datapoint(self, key: str):
        return round((self.status[key] - self.previous_status[key]) / self.worker_job_time)

    def create_dml_qps_graph(self):
        return CreateGraph(self.global_status_metrics)

    def create_replica_lag_graph(self):
        return CreateGraph(self.replica_lag_metrics)


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
                plt.plot(x, y, marker="braille", label="Lag", color=self.graph_data.lag.color)
                max_y_value = max(max_y_value, max(y))
        elif isinstance(self.graph_data, GlobalStatusMetrics):
            for metric, metric_data in self.graph_data.__dict__.items():
                if isinstance(metric_data, MetricData) and metric_data.visible:
                    x = self.graph_data.datetimes
                    y = metric_data.values

                    if y:
                        plt.plot(x, y, marker="braille", label=metric.upper(), color=metric_data.color)
                        max_y_value = max(max_y_value, max(y))

        # I create my own y ticks to format the numbers how I like them
        max_y_ticks = 5

        y_tick_interval = max_y_value / max_y_ticks
        if y_tick_interval >= 1:
            y_ticks = [i * y_tick_interval for i in range(max_y_ticks + 1)]
            if isinstance(self.graph_data, ReplicaLagMetrics):
                y_labels = [format_time(val) for val in y_ticks]
            elif isinstance(self.graph_data, GlobalStatusMetrics):
                y_labels = [format_number(val, for_plot=True, decimal=1) for val in y_ticks]
        else:
            y_ticks = [i for i in range(int(max_y_value) + 1)]
            if isinstance(self.graph_data, ReplicaLagMetrics):
                y_labels = [format_time(val) for val in y_ticks]
            elif isinstance(self.graph_data, GlobalStatusMetrics):
                y_labels = [format_number(val, for_plot=True, decimal=1) for val in y_ticks]

        plt.yticks(y_ticks, y_labels)

        yield Group(*AnsiDecoder().decode(plt.build()))
