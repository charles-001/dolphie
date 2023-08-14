from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List

import plotext as plt
from dolphie.Modules.Functions import format_bytes, format_number, format_time
from rich.text import Text
from textual.widgets import Static


class Graph(Static):
    def __init__(self, bar=False, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.bar = bar
        self.graph_data = None

    def on_show(self) -> None:
        self.render_graph(self.graph_data)

    def on_resize(self) -> None:
        self.render_graph(self.graph_data)

    def render_graph(self, graph_data) -> None:
        # self.metric_data = metric_data
        self.graph_data = graph_data

        plt.clf()
        plt.date_form("H:M:S")
        plt.canvas_color((3, 9, 24))
        plt.axes_color((3, 9, 24))
        plt.ticks_color((144, 169, 223))

        plt.plotsize(self.size.width, self.size.height)

        max_y_value = 0
        if type(self.graph_data) == CheckpointMetrics:
            x = self.graph_data.datetimes
            y = self.graph_data.checkpoint_age.values

            if y:
                plt.hline(0, (3, 9, 24))
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
                plt.text(
                    format_bytes(y[0], color=False), y=max(y), x=max(x), alignment="right", color="white", style="bold"
                )

                plt.plot(
                    x,
                    y,
                    marker="braille",
                    label=self.graph_data.checkpoint_age.label,
                    color=self.graph_data.checkpoint_age.color,
                )
                max_y_value = self.graph_data.checkpoint_age_max
        elif type(self.graph_data) == RedoLogMetrics and self.bar:
            if self.graph_data.Innodb_os_log_written.values:
                x = [0]
                y = [
                    round(
                        sum(self.graph_data.Innodb_os_log_written.values)
                        * (3600 / len(self.graph_data.Innodb_os_log_written.values))
                    )
                ]

                plt.hline(self.graph_data.redo_log_size, (252, 121, 121))
                plt.text(
                    "Log Size",
                    y=self.graph_data.redo_log_size,
                    x=0,
                    alignment="center",
                    color="white",
                    style="bold",
                )

                bar_color = (46, 124, 175)
                if y[0] >= self.graph_data.redo_log_size:
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
                max_y_value = max(self.graph_data.redo_log_size, max(y))
        else:
            for metric_data in self.graph_data.__dict__.values():
                if isinstance(metric_data, MetricData) and metric_data.visible:
                    x = self.graph_data.datetimes
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
            CheckpointMetrics: lambda val: format_bytes(val, color=False),
            RedoLogMetrics: lambda val: format_bytes(val, color=False),
        }

        data_type = type(self.graph_data)
        if data_type in data_formatters:
            y_labels = [data_formatters[data_type](val) for val in y_ticks]
        else:
            y_labels = [format_number(val, for_plot=True, decimal=1) for val in y_ticks]

        plt.yticks(y_ticks, y_labels)

        self.update(Text.from_ansi(plt.build()))


@dataclass
class MetricSource:
    global_status: str = "global_status"
    innodb_metrics: str = "innodb_metrics"
    none: str = "none"


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
    Queries: MetricData
    Com_select: MetricData
    Com_insert: MetricData
    Com_update: MetricData
    Com_delete: MetricData
    Com_replace: MetricData
    Com_rollback: MetricData
    graphs: List[str]
    metric_source: MetricSource = MetricSource.global_status
    per_second_metric: bool = True
    datetimes: List[str] = field(default_factory=list)


@dataclass
class ReplicationLagMetrics:
    lag: MetricData
    graphs: List[str]
    metric_source: MetricSource = MetricSource.none
    per_second_metric: bool = False
    datetimes: List[str] = field(default_factory=list)


@dataclass
class CheckpointMetrics:
    checkpoint_age: MetricData
    graphs: List[str]
    metric_source: MetricSource = MetricSource.global_status
    per_second_metric: bool = False
    datetimes: List[str] = field(default_factory=list)
    checkpoint_age_max: int = 0
    checkpoint_age_sync_flush: int = 0


@dataclass
class BufferPoolRequestsMetrics:
    Innodb_buffer_pool_read_requests: MetricData
    Innodb_buffer_pool_write_requests: MetricData
    Innodb_buffer_pool_reads: MetricData
    graphs: List[str]
    metric_source: MetricSource = MetricSource.global_status
    per_second_metric: bool = True
    datetimes: List[str] = field(default_factory=list)


@dataclass
class AdaptiveHashIndexMetrics:
    adaptive_hash_searches_btree: MetricData
    adaptive_hash_searches: MetricData
    graphs: List[str]
    metric_source: MetricSource = MetricSource.innodb_metrics
    per_second_metric: bool = True
    datetimes: List[str] = field(default_factory=list)


@dataclass
class RedoLogMetrics:
    Innodb_os_log_written: MetricData
    graphs: List[str]
    redo_log_size: int = 0
    metric_source: MetricSource = MetricSource.global_status
    per_second_metric: bool = True
    datetimes: List[str] = field(default_factory=list)


@dataclass
class TableCacheMetrics:
    Table_open_cache_hits: MetricData
    Table_open_cache_misses: MetricData
    Table_open_cache_overflows: MetricData
    graphs: List[str]
    metric_source: MetricSource = MetricSource.global_status
    per_second_metric: bool = True
    datetimes: List[str] = field(default_factory=list)


@dataclass
class ThreadsMetrics:
    Threads_connected: MetricData
    Threads_running: MetricData
    graphs: List[str]
    metric_source: MetricSource = MetricSource.global_status
    per_second_metric: bool = False
    datetimes: List[str] = field(default_factory=list)


class Metrics:
    dml: DMLMetrics
    replication_lag: ReplicationLagMetrics
    checkpoint: CheckpointMetrics
    buffer_pool_requests: BufferPoolRequestsMetrics
    adaptive_hash_index: AdaptiveHashIndexMetrics
    redo_log: RedoLogMetrics
    table_cache: TableCacheMetrics
    threads: ThreadsMetrics


class MetricManager:
    def __init__(self):
        self.worker_start_time: datetime = None
        self.worker_job_time: float = None
        self.global_variables: Dict[str, int] = None
        self.global_status: Dict[str, int] = None
        self.replication_lag: int = None
        self.redo_log_size: int = 0

        self.metrics = Metrics()

        self.metrics.dml = DMLMetrics(
            graphs=["graph_dml"],
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

        self.metrics.replication_lag = ReplicationLagMetrics(
            graphs=["graph_replication_lag"],
            lag=MetricData(label="Lag", color=(68, 180, 255), visible=True, save_history=True),
        )

        self.metrics.checkpoint = CheckpointMetrics(
            graphs=["graph_checkpoint"],
            checkpoint_age=MetricData(label="Uncheckpointed", color=(68, 180, 255), visible=True, save_history=True),
        )

        self.metrics.buffer_pool_requests = BufferPoolRequestsMetrics(
            graphs=["graph_buffer_pool_requests"],
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

        self.metrics.adaptive_hash_index = AdaptiveHashIndexMetrics(
            graphs=["graph_adaptive_hash_index"],
            adaptive_hash_searches_btree=MetricData(
                label="Miss", color=(255, 73, 112), visible=True, save_history=True
            ),
            adaptive_hash_searches=MetricData(label="Hit", color=(84, 239, 174), visible=True, save_history=True),
        )

        self.metrics.redo_log = RedoLogMetrics(
            graphs=["graph_redo_log", "graph_redo_log_bar"],
            Innodb_os_log_written=MetricData(
                label="Data Written/sec", color=(68, 180, 255), visible=True, save_history=True
            ),
        )

        self.metrics.table_cache = TableCacheMetrics(
            graphs=["graph_table_cache"],
            Table_open_cache_hits=MetricData(label="Hits", color=(84, 239, 174), visible=True, save_history=True),
            Table_open_cache_misses=MetricData(label="Misses", color=(255, 73, 112), visible=True, save_history=True),
            Table_open_cache_overflows=MetricData(
                label="Overflows", color=(252, 213, 121), visible=True, save_history=True
            ),
        )

        self.metrics.threads = ThreadsMetrics(
            graphs=["graph_threads"],
            Threads_connected=MetricData(label="Connected", color=(84, 239, 174), visible=True, save_history=True),
            Threads_running=MetricData(label="Running", color=(255, 73, 112), visible=True, save_history=True),
        )

    def refresh_data(
        self,
        worker_start_time: datetime,
        worker_job_time: float,
        global_variables: Dict[str, int],
        global_status: Dict[str, int],
        innodb_metrics: Dict[str, int],
        replication_status: Dict[str, str],
        replication_lag: int,  # this can be from SHOW SLAVE STatus/Performance Schema/heartbeat table
    ):
        self.worker_start_time = worker_start_time
        self.worker_job_time = worker_job_time
        self.global_variables = global_variables
        self.global_status = global_status
        self.innodb_metrics = innodb_metrics
        self.replication_status = replication_status
        self.replication_lag = replication_lag

        # Support MySQL 8.0.30+ redo log size variable
        innodb_redo_log_capacity = self.global_variables.get("innodb_redo_log_capacity", 0) * 32
        innodb_log_file_size = round(
            self.global_variables.get("innodb_log_file_size", 0)
            * self.global_variables.get("innodb_log_files_in_group", 1)
        )
        self.redo_log_size = max(innodb_redo_log_capacity, innodb_log_file_size)

        self.update_metrics_with_per_second_values()
        self.update_metrics_replication_lag()
        self.update_metrics_checkpoint()
        self.update_metrics_threads()

        self.metrics.redo_log.redo_log_size = self.redo_log_size

    def add_metric(self, metric_data: MetricData, value: int):
        if metric_data.save_history:
            metric_data.values.append(value)

    def update_metrics_with_per_second_values(self):
        for metric_instance in self.metrics.__dict__.values():
            added = False

            if not metric_instance.per_second_metric:
                continue

            metrics_data = None  # Initialize as None

            if metric_instance.metric_source == MetricSource.global_status:
                metrics_data = self.global_status
            elif metric_instance.metric_source == MetricSource.innodb_metrics:
                metrics_data = self.innodb_metrics

            if metrics_data is None:
                continue  # Skip if metrics_data is not properly assigned

            for metric_data_name, metric_data in metric_instance.__dict__.items():
                if isinstance(metric_data, MetricData):
                    if metric_data.last_value is None:
                        metric_data.last_value = metrics_data.get(metric_data_name, 0)
                    else:
                        metric_status_per_sec = self.get_metric_calculate_per_sec(metric_data_name, format=False)
                        self.add_metric(metric_data, metric_status_per_sec)
                        added = True

            if added:
                metric_instance.datetimes.append(self.worker_start_time.strftime("%H:%M:%S"))

    def update_metrics_replication_lag(self):
        if self.replication_status:
            metric_instance = self.metrics.replication_lag
            self.add_metric(metric_instance.lag, self.replication_lag)
            metric_instance.datetimes.append(self.worker_start_time.strftime("%H:%M:%S"))

    def update_metrics_threads(self):
        self.metrics.threads.Threads_connected.values.append(self.global_status.get("Threads_connected", 0))
        self.metrics.threads.Threads_running.values.append(self.global_status.get("Threads_running", 0))
        self.metrics.threads.datetimes.append(self.worker_start_time.strftime("%H:%M:%S"))

    def update_metrics_checkpoint(self):
        (
            max_checkpoint_age_bytes,
            checkpoint_age_sync_flush_bytes,
            checkpoint_age_bytes,
        ) = self.get_metric_checkpoint_age(format=False)

        metric_instance = self.metrics.checkpoint
        metric_instance.checkpoint_age_max = max_checkpoint_age_bytes
        metric_instance.checkpoint_age_sync_flush = checkpoint_age_sync_flush_bytes

        self.add_metric(metric_instance.checkpoint_age, checkpoint_age_bytes)
        metric_instance.datetimes.append(self.worker_start_time.strftime("%H:%M:%S"))

    def get_metric_calculate_per_sec(self, metric_name, format=True):
        for metric_instance in self.metrics.__dict__.values():
            if hasattr(metric_instance, metric_name):
                metric_data: MetricData = getattr(metric_instance, metric_name)

                if metric_instance.metric_source == MetricSource.global_status:
                    metrics_data = self.global_status
                elif metric_instance.metric_source == MetricSource.innodb_metrics:
                    metrics_data = self.innodb_metrics

                last_value = metric_data.last_value
                metric_diff = metrics_data.get(metric_name, 0) - last_value
                metric_per_sec = round(metric_diff / self.worker_job_time)

                if format:
                    return format_number(metric_per_sec)
                else:
                    return metric_per_sec

    def get_metric_checkpoint_age(self, format):
        checkpoint_age_bytes = round(self.global_status.get("Innodb_checkpoint_age", 0))
        max_checkpoint_age_bytes = self.redo_log_size

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

        metric_data = self.metrics.adaptive_hash_index

        # Get per second value
        previous_hits = metric_data.adaptive_hash_searches.last_value
        previous_misses = metric_data.adaptive_hash_searches_btree.last_value
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
        for metric_instance in self.metrics.__dict__.values():
            if metric_instance.metric_source == MetricSource.global_status:
                metrics_data = self.global_status
            elif metric_instance.metric_source == MetricSource.innodb_metrics:
                metrics_data = self.innodb_metrics

            for metric_name, metric_data in metric_instance.__dict__.items():
                if isinstance(metric_data, MetricData):
                    metric_data.last_value = metrics_data.get(metric_name, 0)
