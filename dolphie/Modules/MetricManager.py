from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Union

import plotext as plt
from dolphie.Modules.Functions import format_bytes, format_number, format_time
from rich.text import Text
from textual.widgets import Static


class Graph(Static):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.marker = None
        self.metric_instance = None

    def on_show(self) -> None:
        self.render_graph(self.metric_instance)

    def on_resize(self) -> None:
        self.render_graph(self.metric_instance)

    def render_graph(self, metric_instance) -> None:
        self.metric_instance = metric_instance

        if self.metric_instance is None:
            return

        plt.clf()
        plt.date_form("d/m/y H:M:S")
        plt.canvas_color((10, 14, 27))
        plt.axes_color((10, 14, 27))
        plt.ticks_color((133, 159, 213))

        plt.plotsize(self.size.width, self.size.height)

        max_y_value = 0
        if isinstance(self.metric_instance, CheckpointMetrics):
            x = self.metric_instance.datetimes
            y = self.metric_instance.Innodb_checkpoint_age.values

            if y:
                plt.hline(0, (10, 14, 27))
                plt.hline(self.metric_instance.checkpoint_age_sync_flush, (241, 251, 130))
                plt.hline(self.metric_instance.checkpoint_age_max, (252, 121, 121))
                plt.text(
                    "Critical",
                    y=self.metric_instance.checkpoint_age_max,
                    x=max(x),
                    alignment="right",
                    color=(233, 233, 233),
                    style="bold",
                )
                plt.text(
                    "Warning",
                    y=self.metric_instance.checkpoint_age_sync_flush,
                    x=max(x),
                    alignment="right",
                    color=(233, 233, 233),
                    style="bold",
                )

                plt.plot(
                    x,
                    y,
                    marker=self.marker,
                    label=self.metric_instance.Innodb_checkpoint_age.label,
                    color=self.metric_instance.Innodb_checkpoint_age.color,
                )
                max_y_value = self.metric_instance.checkpoint_age_max
        elif isinstance(self.metric_instance, RedoLogMetrics) and "graph_redo_log_bar" in self.id:
            if self.metric_instance.Innodb_lsn_current.values:
                x = [0]
                y = [
                    round(
                        sum(self.metric_instance.Innodb_lsn_current.values)
                        * (3600 / len(self.metric_instance.Innodb_lsn_current.values))
                    )
                ]

                plt.hline(self.metric_instance.redo_log_size, (252, 121, 121))
                plt.text(
                    "Log Size",
                    y=self.metric_instance.redo_log_size,
                    x=0,
                    alignment="center",
                    color=(233, 233, 233),
                    style="bold",
                )

                bar_color = (46, 124, 175)
                if y[0] >= self.metric_instance.redo_log_size:
                    bar_color = (252, 121, 121)

                plt.text(
                    format_bytes(y[0], color=False) + "/hr",
                    y=y[0],
                    x=0,
                    alignment="center",
                    color=(233, 233, 233),
                    style="bold",
                    background=bar_color,
                )

                plt.bar(
                    x,
                    y,
                    marker="hd",
                    color=bar_color,
                )
                max_y_value = max(self.metric_instance.redo_log_size, max(y))
        elif isinstance(self.metric_instance, RedoLogActiveCountMetrics):
            x = self.metric_instance.datetimes
            y = self.metric_instance.Active_redo_log_count.values

            if y:
                plt.hline(1, (10, 14, 27))
                plt.hline(34, (252, 121, 121))
                plt.text(
                    "Max Count",
                    y=34,
                    x=max(x),
                    alignment="right",
                    color=(233, 233, 233),
                    style="bold",
                )

                plt.plot(
                    x,
                    y,
                    marker=self.marker,
                    label=self.metric_instance.Active_redo_log_count.label,
                    color=self.metric_instance.Active_redo_log_count.color,
                )
                max_y_value = 32

        else:
            for metric_data in self.metric_instance.__dict__.values():
                if isinstance(metric_data, MetricData) and metric_data.visible:
                    x = self.metric_instance.datetimes
                    y = metric_data.values

                    if y:
                        plt.plot(x, y, marker=self.marker, label=metric_data.label, color=metric_data.color)
                        max_y_value = max(max_y_value, max(y))

        max_y_ticks = 5
        y_tick_interval = max_y_value / max_y_ticks

        if y_tick_interval >= 1:
            y_ticks = [i * y_tick_interval for i in range(int(max_y_ticks) + 1)]
        else:
            y_ticks = [i for i in range(int(max_y_value) + 1)]

        format_function = get_number_format_function(self.metric_instance)
        y_labels = [format_function(val) for val in y_ticks]

        plt.yticks(y_ticks, y_labels)

        # Fix for Windows when graphs initially load
        try:
            self.update(Text.from_ansi(plt.build()))
        except OSError:
            pass


def get_number_format_function(data, color=False):
    data_formatters = {
        ReplicationLagMetrics: lambda val: format_time(val),
        CheckpointMetrics: lambda val: format_bytes(val, color=color),
        RedoLogMetrics: lambda val: format_bytes(val, color=color),
        AdaptiveHashIndexHitRatioMetrics: lambda val: f"{round(val)}%",
        DiskIOMetrics: lambda val: format_bytes(val, color=color),
    }

    return data_formatters.get(type(data), lambda val: format_number(val, color=color))


@dataclass
class MetricSource:
    global_status: str = "global_status"
    innodb_metrics: str = "innodb_metrics"
    disk_io_metrics: str = "disk_io_metrics"
    none: str = "none"


@dataclass
class MetricColor:
    gray: tuple = (172, 207, 231)
    blue: tuple = (68, 180, 255)
    green: tuple = (84, 239, 174)
    red: tuple = (255, 73, 112)
    yellow: tuple = (252, 213, 121)


@dataclass
class MetricData:
    label: str
    color: tuple
    visible: bool = True
    save_history: bool = True
    per_second_calculation: bool = True
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
    Com_commit: MetricData
    Com_rollback: MetricData
    graphs: List[str]
    tab_name: str = "dml"
    metric_source: MetricSource = MetricSource.global_status
    datetimes: List[str] = field(default_factory=list)


@dataclass
class ReplicationLagMetrics:
    lag: MetricData
    graphs: List[str]
    tab_name: str = "replication_lag"
    metric_source: MetricSource = MetricSource.none

    datetimes: List[str] = field(default_factory=list)


@dataclass
class CheckpointMetrics:
    Innodb_checkpoint_age: MetricData
    graphs: List[str]
    tab_name: str = "checkpoint"
    metric_source: MetricSource = MetricSource.global_status
    datetimes: List[str] = field(default_factory=list)
    checkpoint_age_max: int = 0
    checkpoint_age_sync_flush: int = 0


@dataclass
class BufferPoolRequestsMetrics:
    Innodb_buffer_pool_read_requests: MetricData
    Innodb_buffer_pool_write_requests: MetricData
    Innodb_buffer_pool_reads: MetricData
    graphs: List[str]
    tab_name: str = "buffer_pool_requests"
    metric_source: MetricSource = MetricSource.global_status
    datetimes: List[str] = field(default_factory=list)


@dataclass
class AdaptiveHashIndexMetrics:
    adaptive_hash_searches: MetricData
    adaptive_hash_searches_btree: MetricData
    graphs: List[str]
    tab_name: str = "adaptive_hash_index"
    metric_source: MetricSource = MetricSource.innodb_metrics
    datetimes: List[str] = field(default_factory=list)


@dataclass
class AdaptiveHashIndexHitRatioMetrics:
    hit_ratio: MetricData
    graphs: List[str]
    smoothed_hit_ratio: float = None
    tab_name: str = "adaptive_hash_index"
    metric_source: MetricSource = MetricSource.none
    datetimes: List[str] = field(default_factory=list)


@dataclass
class RedoLogMetrics:
    Innodb_lsn_current: MetricData
    graphs: List[str]
    tab_name: str = "redo_log"
    redo_log_size: int = 0
    metric_source: MetricSource = MetricSource.global_status
    datetimes: List[str] = field(default_factory=list)


@dataclass
class RedoLogActiveCountMetrics:
    Active_redo_log_count: MetricData
    graphs: List[str]
    tab_name: str = "redo_log"
    metric_source: MetricSource = MetricSource.global_status
    datetimes: List[str] = field(default_factory=list)


@dataclass
class TableCacheMetrics:
    Table_open_cache_hits: MetricData
    Table_open_cache_misses: MetricData
    Table_open_cache_overflows: MetricData
    graphs: List[str]
    tab_name: str = "table_cache"
    metric_source: MetricSource = MetricSource.global_status
    datetimes: List[str] = field(default_factory=list)


@dataclass
class ThreadMetrics:
    Threads_connected: MetricData
    Threads_running: MetricData
    graphs: List[str]
    tab_name: str = "threads"
    metric_source: MetricSource = MetricSource.global_status
    datetimes: List[str] = field(default_factory=list)


@dataclass
class TemporaryObjectMetrics:
    Created_tmp_tables: MetricData
    Created_tmp_disk_tables: MetricData
    Created_tmp_files: MetricData
    graphs: List[str]
    tab_name: str = "temporary_objects"
    metric_source: MetricSource = MetricSource.global_status
    datetimes: List[str] = field(default_factory=list)


@dataclass
class AbortedConnectionsMetrics:
    Aborted_clients: MetricData
    Aborted_connects: MetricData
    graphs: List[str]
    tab_name: str = "aborted_connections"
    metric_source: MetricSource = MetricSource.global_status
    datetimes: List[str] = field(default_factory=list)


@dataclass
class DiskIOMetrics:
    io_read: MetricData
    io_write: MetricData
    graphs: List[str]
    tab_name: str = "disk_io"
    metric_source: MetricSource = MetricSource.disk_io_metrics
    datetimes: List[str] = field(default_factory=list)


@dataclass
class LocksMetrics:
    # innodb_trx_lock_count: MetricData
    metadata_lock_count: MetricData
    graphs: List[str]
    tab_name: str = "locks"
    metric_source: MetricSource = MetricSource.none
    datetimes: List[str] = field(default_factory=list)


@dataclass
class MetricInstances:
    dml: DMLMetrics
    replication_lag: ReplicationLagMetrics
    checkpoint: CheckpointMetrics
    buffer_pool_requests: BufferPoolRequestsMetrics
    adaptive_hash_index: AdaptiveHashIndexMetrics
    adaptive_hash_index_hit_ratio: AdaptiveHashIndexHitRatioMetrics
    redo_log: RedoLogMetrics
    redo_log_active_count: RedoLogActiveCountMetrics
    table_cache: TableCacheMetrics
    threads: ThreadMetrics
    temporary_objects: TemporaryObjectMetrics
    aborted_connections: AbortedConnectionsMetrics
    disk_io: DiskIOMetrics
    locks: LocksMetrics


class MetricManager:
    def __init__(self):
        self.reset()

    def reset(self):
        self.worker_start_time: datetime = None
        self.polling_latency: float = None
        self.global_variables: Dict[str, Union[int, str]] = None
        self.global_status: Dict[str, int] = None
        self.replication_lag: int = None
        self.redo_log_size: int = 0

        self.metrics = MetricInstances(
            dml=DMLMetrics(
                graphs=["graph_dml"],
                Queries=MetricData(label="Queries", color=MetricColor.gray, visible=False),
                Com_select=MetricData(label="SELECT", color=MetricColor.blue),
                Com_insert=MetricData(label="INSERT", color=MetricColor.green),
                Com_update=MetricData(label="UPDATE", color=MetricColor.yellow),
                Com_delete=MetricData(label="DELETE", color=MetricColor.red),
                Com_replace=MetricData(
                    label="REPLACE", color=MetricColor.red, visible=False, save_history=False, graphable=False
                ),
                Com_commit=MetricData(
                    label="COMMIT", color=MetricColor.green, visible=False, save_history=True, graphable=False
                ),
                Com_rollback=MetricData(
                    label="ROLLBACK", color=MetricColor.red, visible=False, save_history=False, graphable=False
                ),
            ),
            replication_lag=ReplicationLagMetrics(
                graphs=["graph_replication_lag"],
                lag=MetricData(label="Lag", color=MetricColor.blue, per_second_calculation=False),
            ),
            checkpoint=CheckpointMetrics(
                graphs=["graph_checkpoint"],
                Innodb_checkpoint_age=MetricData(
                    label="Uncheckpointed", color=MetricColor.blue, per_second_calculation=False
                ),
            ),
            buffer_pool_requests=BufferPoolRequestsMetrics(
                graphs=["graph_buffer_pool_requests"],
                Innodb_buffer_pool_read_requests=MetricData(label="Read Requests", color=MetricColor.blue),
                Innodb_buffer_pool_write_requests=MetricData(label="Write Requests", color=MetricColor.green),
                Innodb_buffer_pool_reads=MetricData(label="Disk Reads", color=MetricColor.red),
            ),
            adaptive_hash_index=AdaptiveHashIndexMetrics(
                graphs=["graph_adaptive_hash_index"],
                adaptive_hash_searches=MetricData(label="Hit", color=MetricColor.green),
                adaptive_hash_searches_btree=MetricData(label="Miss", color=MetricColor.red),
            ),
            adaptive_hash_index_hit_ratio=AdaptiveHashIndexHitRatioMetrics(
                graphs=["graph_adaptive_hash_index_hit_ratio"],
                hit_ratio=MetricData(label="Hit Ratio", color=MetricColor.green, per_second_calculation=False),
            ),
            redo_log=RedoLogMetrics(
                graphs=["graph_redo_log", "graph_redo_log_bar"],
                Innodb_lsn_current=MetricData(label="Data Written", color=MetricColor.blue),
            ),
            redo_log_active_count=RedoLogActiveCountMetrics(
                graphs=["graph_redo_log_active_count"],
                Active_redo_log_count=MetricData(
                    label="Active Count", color=MetricColor.blue, per_second_calculation=False, visible=False
                ),
            ),
            table_cache=TableCacheMetrics(
                graphs=["graph_table_cache"],
                Table_open_cache_hits=MetricData(label="Hit", color=MetricColor.green),
                Table_open_cache_misses=MetricData(label="Miss", color=MetricColor.red),
                Table_open_cache_overflows=MetricData(label="Overflow", color=MetricColor.yellow),
            ),
            threads=ThreadMetrics(
                graphs=["graph_threads"],
                Threads_connected=MetricData(label="Connected", color=MetricColor.green, per_second_calculation=False),
                Threads_running=MetricData(label="Running", color=MetricColor.blue, per_second_calculation=False),
            ),
            temporary_objects=TemporaryObjectMetrics(
                graphs=["graph_temporary_objects"],
                Created_tmp_tables=MetricData(label="Tables", color=MetricColor.blue),
                Created_tmp_disk_tables=MetricData(label="Disk", color=MetricColor.red),
                Created_tmp_files=MetricData(label="Files", color=MetricColor.yellow),
            ),
            aborted_connections=AbortedConnectionsMetrics(
                graphs=["graph_aborted_connections"],
                Aborted_clients=MetricData(label="Client (timeout)", color=MetricColor.blue),
                Aborted_connects=MetricData(label="Connects (attempt)", color=MetricColor.red),
            ),
            disk_io=DiskIOMetrics(
                graphs=["graph_disk_io"],
                io_read=MetricData(label="Read", color=MetricColor.blue),
                io_write=MetricData(label="Write", color=MetricColor.green),
            ),
            locks=LocksMetrics(
                graphs=["graph_locks"],
                # innodb_trx_lock_count=MetricData(
                #     label="InnoDB TRX", color=MetricColor.blue, per_second_calculation=False
                # ),
                metadata_lock_count=MetricData(label="Metadata", color=MetricColor.red, per_second_calculation=False),
            ),
        )

    def refresh_data(
        self,
        worker_start_time: datetime,
        polling_latency: float,
        global_variables: Dict[str, Union[int, str]],
        global_status: Dict[str, int],
        innodb_metrics: Dict[str, int],
        disk_io_metrics: Dict[str, int],
        # innodb_trx_lock_metrics: Dict[str, int],
        metadata_lock_metrics: Dict[str, int],
        replication_status: Dict[str, Union[int, str]],
        replication_lag: int,  # this can be from SHOW SLAVE Status/heartbeat table
    ):
        self.worker_start_time = worker_start_time
        self.polling_latency = polling_latency
        self.global_variables = global_variables
        self.global_status = global_status
        self.innodb_metrics = innodb_metrics
        self.disk_io_metrics = disk_io_metrics
        # self.innodb_trx_lock_metrics = innodb_trx_lock_metrics
        self.metadata_lock_metrics = metadata_lock_metrics
        self.replication_status = replication_status
        self.replication_lag = replication_lag

        # Support MySQL 8.0.30+ redo log size variable
        innodb_redo_log_capacity = self.global_variables.get("innodb_redo_log_capacity", 0)
        innodb_log_file_size = round(
            self.global_variables.get("innodb_log_file_size", 0)
            * self.global_variables.get("innodb_log_files_in_group", 1)
        )
        self.redo_log_size = max(innodb_redo_log_capacity, innodb_log_file_size)

        self.update_metrics_per_second_values()

        self.update_metrics_replication_lag()
        self.update_metrics_checkpoint()
        self.update_metrics_adaptive_hash_index_hit_ratio()
        self.update_metrics_locks()

        self.update_metrics_last_value()

        self.metrics.redo_log.redo_log_size = self.redo_log_size

    def add_metric(self, metric_data: MetricData, value: int):
        if metric_data.save_history:
            metric_data.values.append(value)
        else:
            metric_data.values = [value]

    def update_metrics_per_second_values(self):
        for metric_instance in self.metrics.__dict__.values():
            added = False

            metric_source = None  # Initialize as None

            if metric_instance.metric_source == MetricSource.global_status:
                metric_source = self.global_status
            elif metric_instance.metric_source == MetricSource.innodb_metrics:
                metric_source = self.innodb_metrics
            elif metric_instance.metric_source == MetricSource.disk_io_metrics:
                metric_source = self.disk_io_metrics

            if metric_source is None:
                continue  # Skip if there's no metric source

            for metric_name, metric_data in metric_instance.__dict__.items():
                if isinstance(metric_data, MetricData):
                    if metric_data.last_value is None:
                        metric_data.last_value = metric_source.get(metric_name, 0)
                    else:
                        if metric_data.per_second_calculation:
                            metric_status_per_sec = self.get_metric_calculate_per_sec(
                                metric_name, metric_source, format=False
                            )
                        else:
                            metric_status_per_sec = metric_source.get(metric_name, 0)

                        self.add_metric(metric_data, metric_status_per_sec)
                        added = True

            if added:
                metric_instance.datetimes.append(self.worker_start_time.strftime("%d/%m/%y %H:%M:%S"))

    def update_metrics_replication_lag(self):
        if self.replication_status:
            metric_instance = self.metrics.replication_lag
            self.add_metric(metric_instance.lag, self.replication_lag)
            metric_instance.datetimes.append(self.worker_start_time.strftime("%d/%m/%y %H:%M:%S"))

    def update_metrics_adaptive_hash_index_hit_ratio(self):
        hit_ratio = self.get_metric_adaptive_hash_index(format=False)

        if hit_ratio:
            metric_instance = self.metrics.adaptive_hash_index_hit_ratio
            self.add_metric(metric_instance.hit_ratio, hit_ratio)
            metric_instance.datetimes.append(self.worker_start_time.strftime("%d/%m/%y %H:%M:%S"))

    def update_metrics_checkpoint(self):
        (max_checkpoint_age_bytes, checkpoint_age_sync_flush_bytes, _) = self.get_metric_checkpoint_age(format=False)

        metric_instance = self.metrics.checkpoint
        metric_instance.checkpoint_age_max = max_checkpoint_age_bytes
        metric_instance.checkpoint_age_sync_flush = checkpoint_age_sync_flush_bytes

    def update_metrics_locks(self):
        metric_instance = self.metrics.locks
        # self.add_metric(metric_instance.innodb_trx_lock_count, len(self.innodb_trx_lock_metrics))
        self.add_metric(metric_instance.metadata_lock_count, len(self.metadata_lock_metrics))
        metric_instance.datetimes.append(self.worker_start_time.strftime("%d/%m/%y %H:%M:%S"))

    def get_metric_calculate_per_sec(self, metric_name, metric_source=None, format=True):
        if not metric_source:
            metric_source = self.global_status

        for metric_instance in self.metrics.__dict__.values():
            if hasattr(metric_instance, metric_name):
                metric_data: MetricData = getattr(metric_instance, metric_name)

                metric_diff = metric_source.get(metric_name, 0) - metric_data.last_value
                metric_per_sec = round(metric_diff / self.polling_latency)

                if format:
                    return format_number(metric_per_sec)
                else:
                    return metric_per_sec

    def get_metric_checkpoint_age(self, format):
        checkpoint_age_bytes = round(self.global_status.get("Innodb_checkpoint_age", 0))
        max_checkpoint_age_bytes = self.redo_log_size

        if checkpoint_age_bytes + max_checkpoint_age_bytes == 0:
            return "N/A"

        checkpoint_age_sync_flush_bytes = round(max_checkpoint_age_bytes * 0.825)
        checkpoint_age_ratio = round(checkpoint_age_bytes / max_checkpoint_age_bytes * 100, 2)

        if format:
            if checkpoint_age_ratio >= 80:
                color_code = "red"
            elif checkpoint_age_ratio >= 60:
                color_code = "yellow"
            else:
                color_code = "green"

            return f"[{color_code}]{checkpoint_age_ratio}%"
        else:
            return max_checkpoint_age_bytes, checkpoint_age_sync_flush_bytes, checkpoint_age_bytes

    def get_metric_adaptive_hash_index(self, format=True):
        if self.global_variables.get("innodb_adaptive_hash_index") == "OFF":
            return "OFF" if format else None
        elif format:
            smoothed_hit_ratio = self.metrics.adaptive_hash_index_hit_ratio.smoothed_hit_ratio
            if smoothed_hit_ratio is None:
                return "N/A"
            else:
                if smoothed_hit_ratio <= 0.01:
                    return "Inactive"

                color_code = "green" if smoothed_hit_ratio > 70 else "yellow" if smoothed_hit_ratio > 50 else "red"

                return f"[{color_code}]{smoothed_hit_ratio:.2f}%[/{color_code}]"

        current_hits = self.innodb_metrics.get("adaptive_hash_searches", 0)
        current_misses = self.innodb_metrics.get("adaptive_hash_searches_btree", 0)

        hits = current_hits - self.metrics.adaptive_hash_index.adaptive_hash_searches.last_value
        misses = current_misses - self.metrics.adaptive_hash_index.adaptive_hash_searches_btree.last_value
        total_hits_misses = hits + misses
        if total_hits_misses <= 0:
            return None

        hit_ratio = (hits / total_hits_misses) * 100

        smoothing_factor = 0.5
        smoothed_hit_ratio = self.metrics.adaptive_hash_index_hit_ratio.smoothed_hit_ratio

        if smoothed_hit_ratio is None:
            smoothed_hit_ratio = hit_ratio
        else:
            smoothed_hit_ratio = (1 - smoothing_factor) * smoothed_hit_ratio + smoothing_factor * hit_ratio
        self.metrics.adaptive_hash_index_hit_ratio.smoothed_hit_ratio = smoothed_hit_ratio

        return smoothed_hit_ratio

    def update_metrics_last_value(self):
        # We set the last value for specific metrics that need it so they can get per second values
        for metric_instance in self.metrics.__dict__.values():
            if metric_instance.metric_source == MetricSource.global_status:
                metrics_data = self.global_status
            elif metric_instance.metric_source == MetricSource.innodb_metrics:
                metrics_data = self.innodb_metrics
            elif metric_instance.metric_source == MetricSource.disk_io_metrics:
                metrics_data = self.disk_io_metrics

            for metric_name, metric_data in metric_instance.__dict__.items():
                if isinstance(metric_data, MetricData) and metric_data.per_second_calculation:
                    metric_data.last_value = metrics_data.get(metric_name, 0)
