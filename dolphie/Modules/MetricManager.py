# Use __future__ to allow 'deque[int]' type hint in MetricData
from __future__ import annotations

import contextlib
import dataclasses
from collections import defaultdict, deque
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Union

import plotext as plt
from rich.text import Text
from textual.widgets import Static

from dolphie.DataTypes import ConnectionSource
from dolphie.Modules.Functions import format_bytes, format_number, format_time


class MetricSource(Enum):
    """Enumeration of sources for metric data."""

    SYSTEM_UTILIZATION = "system_utilization"
    GLOBAL_STATUS = "global_status"
    INNODB_METRICS = "innodb_metrics"
    DISK_IO_METRICS = "disk_io_metrics"
    PROXYSQL_SELECT_COMMAND_STATS = "proxysql_select_command_stats"
    PROXYSQL_TOTAL_COMMAND_STATS = "proxysql_total_command_stats"
    NONE = "none"


@dataclass
class MetricColor:
    """Namespace for standard metric graph colors."""

    gray: tuple = (172, 207, 231)
    blue: tuple = (68, 180, 255)
    green: tuple = (84, 239, 174)
    red: tuple = (255, 73, 112)
    yellow: tuple = (252, 213, 121)
    purple: tuple = (191, 121, 252)
    orange: tuple = (252, 121, 121)


class Graph(Static):
    """A Textual widget for rendering time-series graphs using plotext."""

    def __init__(self, *args, **kwargs) -> None:
        """Initialize the Graph widget."""
        super().__init__(*args, **kwargs)
        self.marker: str | None = None
        self.metric_instance: MetricInstance | None = None
        # This will be a deque, but we treat it as an iterable
        self.datetimes: deque[str] | None = None

    def on_show(self) -> None:
        """Render the graph when the widget is shown."""
        self.render_graph(self.metric_instance, self.datetimes)

    def on_resize(self) -> None:
        """Re-render the graph when the widget is resized."""
        self.render_graph(self.metric_instance, self.datetimes)

    def _setup_plot(self) -> None:
        """Clears and configures the plotext canvas."""
        plt.clf()
        plt.date_form("d/m/y H:M:S")
        plt.canvas_color((10, 14, 27))
        plt.axes_color((10, 14, 27))
        plt.ticks_color((133, 159, 213))
        plt.plotsize(self.size.width, self.size.height)

    def _finalize_plot(self, max_y_value: float) -> None:
        """Calculates Y-ticks, formats labels, and updates the widget."""
        max_y_ticks = 5
        y_tick_interval = (max_y_value / max_y_ticks) if max_y_ticks > 0 else 0

        if y_tick_interval >= 1:
            y_ticks = [i * y_tick_interval for i in range(int(max_y_ticks) + 1)]
        else:
            y_ticks = [float(i) for i in range(int(max_y_value) + 2)]

        format_function = get_number_format_function(self.metric_instance)
        y_labels = [format_function(val) for val in y_ticks]

        plt.yticks(y_ticks, y_labels)

        with contextlib.suppress(OSError):
            self.update(Text.from_ansi(plt.build()))

    def _render_checkpoint_metrics(self, x: list[str], y: list[float]) -> float:
        """Renders the graph for CheckpointMetrics."""
        plt.hline(0, (10, 14, 27))
        plt.hline(self.metric_instance.checkpoint_age_sync_flush, (241, 251, 130))
        plt.hline(self.metric_instance.checkpoint_age_max, (252, 121, 121))

        max_x = max(x)
        plt.text(
            "Critical",
            y=self.metric_instance.checkpoint_age_max,
            x=max_x,
            alignment="right",
            color=(233, 233, 233),
            style="bold",
        )
        plt.text(
            "Warning",
            y=self.metric_instance.checkpoint_age_sync_flush,
            x=max_x,
            alignment="right",
            color=(233, 233, 233),
            style="bold",
        )

        metric = self.metric_instance.Innodb_checkpoint_age
        plt.plot(x, y, marker=self.marker, label=metric.label, color=metric.color)
        return self.metric_instance.checkpoint_age_max

    def _render_redo_log_bar_metrics(self, y_values: list[int]) -> float:
        """Renders the bar graph for RedoLogMetrics."""
        x = [0]
        # Calculate y from the snapshot of values
        y = [round(sum(y_values) * (3600 / len(y_values)))]

        log_size = self.metric_instance.redo_log_size
        plt.hline(log_size, (252, 121, 121))
        plt.text(
            "Log Size",
            y=log_size,
            x=0,
            alignment="center",
            color=(233, 233, 233),
            style="bold",
        )

        bar_color = (46, 124, 175)
        if y[0] >= log_size:
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
        plt.bar(x, y, marker="hd", color=bar_color)
        return max(log_size, max(y))

    def _render_redo_log_line_metrics(self, x: list[str], y: list[float]) -> float:
        """Renders the line graph for RedoLogMetrics."""
        metric = self.metric_instance.Innodb_lsn_current
        plt.plot(x, y, marker=self.marker, label=metric.label, color=metric.color)
        return max(y) if y else 0

    def _render_active_redo_log_metrics(self, x: list[str], y: list[float]) -> float:
        """Renders the graph for RedoLogActiveCountMetrics."""
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

        metric = self.metric_instance.Active_redo_log_count
        plt.plot(x, y, marker=self.marker, label=metric.label, color=metric.color)
        return 34.0  # Fixed max Y for this graph

    def _render_system_memory_metrics(self, x: list[str], y: list[float]) -> float:
        """Renders the graph for SystemMemoryMetrics."""
        total_mem = self.metric_instance.Memory_Total.last_value or 0
        plt.hline(0, (10, 14, 27))
        plt.hline(total_mem, (252, 121, 121))
        plt.text(
            "Total",
            y=total_mem,
            x=max(x),
            alignment="right",
            color=(233, 233, 233),
            style="bold",
        )

        metric = self.metric_instance.Memory_Used
        plt.plot(x, y, marker=self.marker, label=metric.label, color=metric.color)
        return total_mem

    def _render_default_metrics(self, x: list[str]) -> float:
        """Renders a graph for any standard metric instance."""
        max_y = 0.0
        for metric_data in self.metric_instance.__dict__.values():
            if isinstance(metric_data, MetricData) and metric_data.visible:
                # **THREAD-SAFETY**: Snapshot deque to list
                y = list(metric_data.values)
                if y and x:
                    plt.plot(
                        x,
                        y,
                        marker=self.marker,
                        label=metric_data.label,
                        color=metric_data.color,
                    )
                    try:
                        max_y = max(max_y, max(y))
                    except ValueError:
                        pass  # Handle empty list
        return max_y

    def render_graph(self, metric_instance: MetricInstance | None, datetimes: deque[str] | None) -> None:
        """Renders a graph for the given metric instance and datetimes.

        Args:
            metric_instance: The metric dataclass instance to plot.
            datetimes: A deque of datetime strings for the X-axis.
        """
        self.metric_instance = metric_instance
        self.datetimes = datetimes

        if self.metric_instance is None or self.datetimes is None:
            self.update("")  # Clear the graph if no data
            return

        self._setup_plot()

        max_y_value = 0.0

        # Create a snapshot of the datetimes and all
        # relevant metric values at the beginning of the render for thread-safety
        try:
            x = list(self.datetimes)
            if not x:  # Check if list is empty after copy
                self.update("")
                return
        except RuntimeError:  # deque changed size during iteration
            self.update("")
            return

        try:
            if isinstance(self.metric_instance, CheckpointMetrics):
                y = list(self.metric_instance.Innodb_checkpoint_age.values)
                if x and y:
                    max_y_value = self._render_checkpoint_metrics(x, y)

            elif isinstance(self.metric_instance, RedoLogMetrics):
                if "graph_redo_log_bar" in self.id:
                    # Snapshot the values needed for the bar chart
                    innodb_lsn_values = list(self.metric_instance.Innodb_lsn_current.values)
                    if innodb_lsn_values:
                        max_y_value = self._render_redo_log_bar_metrics(innodb_lsn_values)
                else:
                    y = list(self.metric_instance.Innodb_lsn_current.values)
                    if x and y:
                        max_y_value = max(max_y_value, self._render_redo_log_line_metrics(x, y))

            elif isinstance(self.metric_instance, RedoLogActiveCountMetrics):
                y = list(self.metric_instance.Active_redo_log_count.values)
                if x and y:
                    max_y_value = self._render_active_redo_log_metrics(x, y)

            elif isinstance(self.metric_instance, SystemMemoryMetrics):
                y = list(self.metric_instance.Memory_Used.values)
                if x and y:
                    max_y_value = self._render_system_memory_metrics(x, y)

            else:
                # Default renderer snapshots its own 'y' values inside
                max_y_value = self._render_default_metrics(x)

        except (ValueError, TypeError, IndexError):
            pass  # Catch errors during plotting

        self._finalize_plot(max_y_value)


def get_number_format_function(data: MetricInstance, color: bool = False) -> Callable[[int | float], str]:
    """Returns the correct formatting function based on the metric type."""
    data_formatters: dict[type, Callable[[int | float], str]] = {
        ReplicationLagMetrics: lambda val: format_time(val),
        CheckpointMetrics: lambda val: format_bytes(val, color=color),
        RedoLogMetrics: lambda val: format_bytes(val, color=color),
        AdaptiveHashIndexHitRatio: lambda val: f"{round(val)}%",
        ProxySQLMultiplexEfficiency: lambda val: f"{round(val)}%",
        DiskIOMetrics: lambda val: format_bytes(val, color=color),
        ProxySQLQueriesDataNetwork: lambda val: format_bytes(val, color=color),
        SystemMemoryMetrics: lambda val: format_bytes(val, color=color),
        SystemNetworkMetrics: lambda val: format_bytes(val, color=color),
    }
    return data_formatters.get(type(data), lambda val: format_number(val, color=color))


@dataclass
class MetricData:
    label: str
    color: tuple
    visible: bool = True
    save_history: bool = True
    per_second_calculation: bool = True
    last_value: int | None = None
    graphable: bool = True
    create_switch: bool = True
    # Use a deque for O(1) appends and pops
    values: deque[int] = field(default_factory=deque)


@dataclass
class SystemCPUMetrics:
    CPU_Percent: MetricData
    graphs: list[str]
    tab_name: str = "system"
    graph_tab_name = "System"
    metric_source: MetricSource = MetricSource.SYSTEM_UTILIZATION
    connection_source: list[ConnectionSource] = field(
        default_factory=lambda: [ConnectionSource.mysql, ConnectionSource.proxysql]
    )
    use_with_replay: bool = True


@dataclass
class SystemMemoryMetrics:
    Memory_Total: MetricData
    Memory_Used: MetricData
    graphs: list[str]
    tab_name: str = "system"
    graph_tab_name = "System"
    metric_source: MetricSource = MetricSource.SYSTEM_UTILIZATION
    connection_source: list[ConnectionSource] = field(
        default_factory=lambda: [ConnectionSource.mysql, ConnectionSource.proxysql]
    )
    use_with_replay: bool = True


@dataclass
class SystemNetworkMetrics:
    Network_Down: MetricData
    Network_Up: MetricData
    graphs: list[str]
    tab_name: str = "system"
    graph_tab_name = "System"
    metric_source: MetricSource = MetricSource.SYSTEM_UTILIZATION
    connection_source: list[ConnectionSource] = field(
        default_factory=lambda: [ConnectionSource.mysql, ConnectionSource.proxysql]
    )
    use_with_replay: bool = True


@dataclass
class SystemDiskIOMetrics:
    Disk_Read: MetricData
    Disk_Write: MetricData
    graphs: list[str]
    tab_name: str = "system"
    graph_tab_name = "System"
    metric_source: MetricSource = MetricSource.SYSTEM_UTILIZATION
    connection_source: list[ConnectionSource] = field(
        default_factory=lambda: [ConnectionSource.mysql, ConnectionSource.proxysql]
    )
    use_with_replay: bool = True


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
    graphs: list[str]
    tab_name: str = "dml"
    graph_tab_name = "DML"
    metric_source: MetricSource = MetricSource.GLOBAL_STATUS
    connection_source: list[ConnectionSource] = field(
        default_factory=lambda: [ConnectionSource.mysql, ConnectionSource.proxysql]
    )
    use_with_replay: bool = True


@dataclass
class ReplicationLagMetrics:
    lag: MetricData
    graphs: list[str]
    tab_name: str = "replication_lag"
    graph_tab_name = "Replication"
    metric_source: MetricSource = MetricSource.NONE
    connection_source: list[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.mysql])
    use_with_replay: bool = True


@dataclass
class CheckpointMetrics:
    Innodb_checkpoint_age: MetricData
    graphs: list[str]
    tab_name: str = "checkpoint"
    graph_tab_name = "Checkpoint"
    metric_source: MetricSource = MetricSource.GLOBAL_STATUS
    checkpoint_age_max: int = 0
    checkpoint_age_sync_flush: int = 0
    connection_source: list[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.mysql])
    use_with_replay: bool = True


@dataclass
class BufferPoolRequestsMetrics:
    Innodb_buffer_pool_read_requests: MetricData
    Innodb_buffer_pool_write_requests: MetricData
    Innodb_buffer_pool_reads: MetricData
    graphs: list[str]
    tab_name: str = "buffer_pool_requests"
    graph_tab_name = "BP Requests"
    metric_source: MetricSource = MetricSource.GLOBAL_STATUS
    connection_source: list[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.mysql])
    use_with_replay: bool = True


@dataclass
class AdaptiveHashIndexMetrics:
    adaptive_hash_searches: MetricData
    adaptive_hash_searches_btree: MetricData
    graphs: list[str]
    tab_name: str = "adaptive_hash_index"
    graph_tab_name = "AHI"
    metric_source: MetricSource = MetricSource.INNODB_METRICS
    connection_source: list[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.mysql])
    use_with_replay: bool = True


@dataclass
class AdaptiveHashIndexHitRatio:
    hit_ratio: MetricData
    graphs: list[str]
    smoothed_hit_ratio: float | None = None
    tab_name: str = "adaptive_hash_index"
    graph_tab_name = "AHI"
    metric_source: MetricSource = MetricSource.NONE
    connection_source: list[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.mysql])
    use_with_replay: bool = True


@dataclass
class RedoLogMetrics:
    Innodb_lsn_current: MetricData
    graphs: list[str]
    tab_name: str = "redo_log"
    graph_tab_name = "Redo Log"
    redo_log_size: int = 0
    metric_source: MetricSource = MetricSource.GLOBAL_STATUS
    connection_source: list[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.mysql])
    use_with_replay: bool = True


@dataclass
class RedoLogActiveCountMetrics:
    Active_redo_log_count: MetricData
    graphs: list[str]
    tab_name: str = "redo_log"
    graph_tab_name = "Redo Log"
    metric_source: MetricSource = MetricSource.GLOBAL_STATUS
    connection_source: list[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.mysql])
    use_with_replay: bool = True


@dataclass
class TableCacheMetrics:
    Table_open_cache_hits: MetricData
    Table_open_cache_misses: MetricData
    Table_open_cache_overflows: MetricData
    graphs: list[str]
    tab_name: str = "table_cache"
    graph_tab_name = "Table Cache"
    metric_source: MetricSource = MetricSource.GLOBAL_STATUS
    connection_source: list[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.mysql])
    use_with_replay: bool = True


@dataclass
class ThreadMetrics:
    Threads_connected: MetricData
    Threads_running: MetricData
    graphs: list[str]
    tab_name: str = "threads"
    graph_tab_name = "Threads"
    metric_source: MetricSource = MetricSource.GLOBAL_STATUS
    connection_source: list[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.mysql])
    use_with_replay: bool = True


@dataclass
class TemporaryObjectMetrics:
    Created_tmp_tables: MetricData
    Created_tmp_disk_tables: MetricData
    Created_tmp_files: MetricData
    graphs: list[str]
    tab_name: str = "temporary_objects"
    graph_tab_name = "Temp Objects"
    metric_source: MetricSource = MetricSource.GLOBAL_STATUS
    connection_source: list[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.mysql])
    use_with_replay: bool = True


@dataclass
class AbortedConnectionsMetrics:
    Aborted_clients: MetricData
    Aborted_connects: MetricData
    graphs: list[str]
    tab_name: str = "aborted_connections"
    graph_tab_name = "Aborted Connections"
    metric_source: MetricSource = MetricSource.GLOBAL_STATUS
    connection_source: list[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.mysql])
    use_with_replay: bool = True


@dataclass
class DiskIOMetrics:
    io_read: MetricData
    io_write: MetricData
    graphs: list[str]
    tab_name: str = "disk_io"
    graph_tab_name = "Disk I/O"
    metric_source: MetricSource = MetricSource.DISK_IO_METRICS
    connection_source: list[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.mysql])
    use_with_replay: bool = True


@dataclass
class LocksMetrics:
    metadata_lock_count: MetricData
    graphs: list[str]
    tab_name: str = "locks"
    graph_tab_name = "Locks"
    metric_source: MetricSource = MetricSource.NONE
    connection_source: list[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.mysql])
    use_with_replay: bool = True


@dataclass
class HistoryListLength:
    trx_rseg_history_len: MetricData
    graphs: list[str]
    tab_name: str = "history_list_length"
    graph_tab_name = "History List"
    metric_source: MetricSource = MetricSource.INNODB_METRICS
    connection_source: list[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.mysql])
    use_with_replay: bool = True


@dataclass
class ProxySQLConnectionsMetrics:
    Client_Connections_non_idle: MetricData
    Client_Connections_aborted: MetricData
    Client_Connections_connected: MetricData
    Client_Connections_created: MetricData
    Server_Connections_aborted: MetricData
    Server_Connections_connected: MetricData
    Server_Connections_created: MetricData
    Access_Denied_Wrong_Password: MetricData
    graphs: list[str]
    tab_name: str = "proxysql_connections"
    graph_tab_name = "Connections"
    metric_source: MetricSource = MetricSource.GLOBAL_STATUS
    connection_source: list[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.proxysql])
    use_with_replay: bool = True


@dataclass
class ProxySQLQueriesDataNetwork:
    Queries_backends_bytes_recv: MetricData
    Queries_backends_bytes_sent: MetricData
    Queries_frontends_bytes_recv: MetricData
    Queries_frontends_bytes_sent: MetricData
    graphs: list[str]
    tab_name: str = "proxysql_queries_data_network"
    graph_tab_name = "Query Data Rates"
    metric_source: MetricSource = MetricSource.GLOBAL_STATUS
    connection_source: list[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.proxysql])
    use_with_replay: bool = True


@dataclass
class ProxySQLActiveTRX:
    Active_Transactions: MetricData
    graphs: list[str]
    tab_name: str = "proxysql_active_trx"
    graph_tab_name = "Active TRX"
    metric_source: MetricSource = MetricSource.GLOBAL_STATUS
    connection_source: list[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.proxysql])
    use_with_replay: bool = True


@dataclass
class ProxySQLMultiplexEfficiency:
    proxysql_multiplex_efficiency_ratio: MetricData
    graphs: list[str]
    tab_name: str = "proxysql_multiplex_efficiency"
    graph_tab_name = "Multiplex Efficiency"
    metric_source: MetricSource = MetricSource.GLOBAL_STATUS
    connection_source: list[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.proxysql])
    use_with_replay: bool = True


@dataclass
class ProxySQLSELECTCommandStats:
    cnt_100us: MetricData
    cnt_500us: MetricData
    cnt_1ms: MetricData
    cnt_5ms: MetricData
    cnt_10ms: MetricData
    cnt_50ms: MetricData
    cnt_100ms: MetricData
    cnt_500ms: MetricData
    cnt_1s: MetricData
    cnt_5s: MetricData
    cnt_10s: MetricData
    cnt_INFs: MetricData
    graphs: list[str]
    tab_name: str = "proxysql_select_command_stats"
    graph_tab_name = "SELECT Command Stats"
    metric_source: MetricSource = MetricSource.PROXYSQL_SELECT_COMMAND_STATS
    connection_source: list[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.proxysql])
    use_with_replay: bool = True


@dataclass
class ProxySQLTotalCommandStats:
    cnt_100us: MetricData
    cnt_500us: MetricData
    cnt_1ms: MetricData
    cnt_5ms: MetricData
    cnt_10ms: MetricData
    cnt_50ms: MetricData
    cnt_100ms: MetricData
    cnt_500ms: MetricData
    cnt_1s: MetricData
    cnt_5s: MetricData
    cnt_10s: MetricData
    cnt_INFs: MetricData
    graphs: list[str]
    tab_name: str = "proxysql_total_command_stats"
    graph_tab_name = "Total Command Stats"
    metric_source: MetricSource = MetricSource.PROXYSQL_TOTAL_COMMAND_STATS
    connection_source: list[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.proxysql])
    use_with_replay: bool = True


# Type alias for all metric types
MetricInstance = Union[
    SystemCPUMetrics,
    SystemMemoryMetrics,
    SystemNetworkMetrics,
    SystemDiskIOMetrics,
    DMLMetrics,
    ReplicationLagMetrics,
    CheckpointMetrics,
    BufferPoolRequestsMetrics,
    AdaptiveHashIndexMetrics,
    AdaptiveHashIndexHitRatio,
    RedoLogMetrics,
    RedoLogActiveCountMetrics,
    TableCacheMetrics,
    ThreadMetrics,
    TemporaryObjectMetrics,
    AbortedConnectionsMetrics,
    DiskIOMetrics,
    LocksMetrics,
    HistoryListLength,
    ProxySQLConnectionsMetrics,
    ProxySQLQueriesDataNetwork,
    ProxySQLActiveTRX,
    ProxySQLMultiplexEfficiency,
    ProxySQLSELECTCommandStats,
    ProxySQLTotalCommandStats,
]


@dataclass
class MetricInstances:
    """Container for all specific metric instances."""

    system_cpu: SystemCPUMetrics
    system_memory: SystemMemoryMetrics
    system_disk_io: SystemDiskIOMetrics
    system_network: SystemNetworkMetrics
    dml: DMLMetrics
    buffer_pool_requests: BufferPoolRequestsMetrics
    history_list_length: HistoryListLength
    adaptive_hash_index: AdaptiveHashIndexMetrics
    adaptive_hash_index_hit_ratio: AdaptiveHashIndexHitRatio
    checkpoint: CheckpointMetrics
    redo_log_active_count: RedoLogActiveCountMetrics
    redo_log: RedoLogMetrics
    table_cache: TableCacheMetrics
    threads: ThreadMetrics
    temporary_objects: TemporaryObjectMetrics
    aborted_connections: AbortedConnectionsMetrics
    disk_io: DiskIOMetrics
    locks: LocksMetrics
    replication_lag: ReplicationLagMetrics
    proxysql_active_trx: ProxySQLActiveTRX
    proxysql_multiplex_efficiency: ProxySQLMultiplexEfficiency
    proxysql_connections: ProxySQLConnectionsMetrics
    proxysql_queries_data_network: ProxySQLQueriesDataNetwork
    proxysql_select_command_stats: ProxySQLSELECTCommandStats
    proxysql_total_command_stats: ProxySQLTotalCommandStats


class MetricManager:
    """Manages the state, collection, and processing of all metrics."""

    def __init__(self, replay_file: str, daemon_mode: bool = False):
        """Initialize the MetricManager.

        Args:
            replay_file: Path to a replay file, if one is being used.
            daemon_mode: True if running in daemon mode (trims old data).
        """
        self.connection_source = ConnectionSource.mysql
        self.replay_file = replay_file
        self.daemon_mode = daemon_mode

        # Attributes populated by refresh_data
        self.worker_start_time: datetime | None = None
        self.system_utilization: dict[str, int] = {}
        self.innodb_metrics: dict[str, int] = {}
        self.disk_io_metrics: dict[str, int] = {}
        self.metadata_lock_metrics: dict[str, int] = {}
        self.replication_status: dict[str, int | str] = {}
        self.proxysql_total_command_stats: dict[str, int] = {}
        self.proxysql_select_command_stats: dict[str, int] = {}

        # State attributes
        self.initialized: bool = False
        self.polling_latency: float = 0
        self.global_variables: dict[str, int | str] = {}
        self.global_status: dict[str, int] = {}
        self.redo_log_size: int = 0
        # Use a deque for O(1) appends and pops
        self.datetimes: deque[str] = deque()

        # The authoritative structure of all metrics
        self.metrics: MetricInstances = None  # type: ignore

        # Optimized lookup tables for processing
        # For fast, source-based processing in update_..._values
        self._source_to_metrics_processing: dict[MetricSource, list[tuple[str, MetricData, list[ConnectionSource]]]] = (
            defaultdict(list)
        )
        # For fast history cleanup in daemon_cleanup_data
        self._all_metrics_data_history: list[MetricData] = []

        # Setup the dispatch map for metric sources
        self._metric_source_map: dict[MetricSource, dict[str, int] | None] = {
            MetricSource.SYSTEM_UTILIZATION: self.system_utilization,
            MetricSource.GLOBAL_STATUS: self.global_status,
            MetricSource.INNODB_METRICS: self.innodb_metrics,
            MetricSource.DISK_IO_METRICS: self.disk_io_metrics,
            MetricSource.PROXYSQL_SELECT_COMMAND_STATS: self.proxysql_select_command_stats,
            MetricSource.PROXYSQL_TOTAL_COMMAND_STATS: self.proxysql_total_command_stats,
            MetricSource.NONE: None,
        }

        self.reset()

    def reset(self):
        """Resets all metrics and state to their default values."""
        self.initialized = False
        self.polling_latency = 0
        self.global_variables.clear()
        self.global_status.clear()
        self.redo_log_size = 0
        self.datetimes.clear()

        # Clear raw data stores
        self.system_utilization.clear()
        self.innodb_metrics.clear()
        self.disk_io_metrics.clear()
        self.metadata_lock_metrics.clear()
        self.replication_status.clear()
        self.proxysql_total_command_stats.clear()
        self.proxysql_select_command_stats.clear()

        # Clear performance lookup tables
        self._source_to_metrics_processing.clear()
        self._all_metrics_data_history.clear()

        self.metrics = MetricInstances(
            system_cpu=SystemCPUMetrics(
                graphs=["graph_system_cpu"],
                CPU_Percent=MetricData(
                    label="CPU %",
                    color=MetricColor.blue,
                    per_second_calculation=False,
                    create_switch=False,
                ),
            ),
            system_memory=SystemMemoryMetrics(
                graphs=["graph_system_memory"],
                Memory_Total=MetricData(
                    label="Total",
                    color=MetricColor.blue,
                    per_second_calculation=False,
                    visible=False,
                    save_history=False,
                    create_switch=False,
                ),
                Memory_Used=MetricData(
                    label="Memory Used",
                    color=MetricColor.green,
                    per_second_calculation=False,
                    create_switch=False,
                ),
            ),
            system_disk_io=SystemDiskIOMetrics(
                graphs=["graph_system_disk_io"],
                Disk_Read=MetricData(label="IOPS Read", color=MetricColor.blue),
                Disk_Write=MetricData(label="IOPS Write", color=MetricColor.yellow),
            ),
            system_network=SystemNetworkMetrics(
                graphs=["graph_system_network"],
                Network_Down=MetricData(label="Net Dn", color=MetricColor.blue),
                Network_Up=MetricData(label="Net Up", color=MetricColor.gray),
            ),
            dml=DMLMetrics(
                graphs=["graph_dml"],
                Queries=MetricData(label="Queries", color=MetricColor.gray, visible=False),
                Com_select=MetricData(label="SELECT", color=MetricColor.blue),
                Com_insert=MetricData(label="INSERT", color=MetricColor.green),
                Com_update=MetricData(label="UPDATE", color=MetricColor.yellow),
                Com_delete=MetricData(label="DELETE", color=MetricColor.red),
                Com_replace=MetricData(
                    label="REPLACE",
                    color=MetricColor.red,
                    visible=False,
                    save_history=False,
                    graphable=False,
                ),
                Com_commit=MetricData(
                    label="COMMIT",
                    color=MetricColor.green,
                    visible=False,
                    save_history=True,
                    graphable=False,
                ),
                Com_rollback=MetricData(
                    label="ROLLBACK",
                    color=MetricColor.red,
                    visible=False,
                    save_history=False,
                    graphable=False,
                ),
            ),
            replication_lag=ReplicationLagMetrics(
                graphs=["graph_replication_lag"],
                lag=MetricData(
                    label="Lag",
                    color=MetricColor.blue,
                    per_second_calculation=False,
                    create_switch=False,
                ),
            ),
            checkpoint=CheckpointMetrics(
                graphs=["graph_checkpoint"],
                Innodb_checkpoint_age=MetricData(
                    label="Uncheckpointed",
                    color=MetricColor.green,
                    per_second_calculation=False,
                    create_switch=False,
                ),
            ),
            buffer_pool_requests=BufferPoolRequestsMetrics(
                graphs=["graph_buffer_pool_requests"],
                Innodb_buffer_pool_read_requests=MetricData(label="Read Requests", color=MetricColor.blue),
                Innodb_buffer_pool_write_requests=MetricData(label="Write Requests", color=MetricColor.green),
                Innodb_buffer_pool_reads=MetricData(label="Disk Reads", color=MetricColor.red),
            ),
            history_list_length=HistoryListLength(
                graphs=["graph_history_list_length"],
                trx_rseg_history_len=MetricData(
                    label="HLL",
                    color=MetricColor.blue,
                    per_second_calculation=False,
                    create_switch=False,
                ),
            ),
            adaptive_hash_index=AdaptiveHashIndexMetrics(
                graphs=["graph_adaptive_hash_index"],
                adaptive_hash_searches=MetricData(label="Hit", color=MetricColor.green),
                adaptive_hash_searches_btree=MetricData(label="Miss", color=MetricColor.red),
            ),
            adaptive_hash_index_hit_ratio=AdaptiveHashIndexHitRatio(
                graphs=["graph_adaptive_hash_index_hit_ratio"],
                hit_ratio=MetricData(
                    label="Hit Ratio",
                    color=MetricColor.green,
                    per_second_calculation=False,
                    create_switch=False,
                ),
            ),
            redo_log=RedoLogMetrics(
                graphs=["graph_redo_log_data_written", "graph_redo_log_bar"],
                Innodb_lsn_current=MetricData(label="Data Written", color=MetricColor.blue, create_switch=False),
            ),
            redo_log_active_count=RedoLogActiveCountMetrics(
                graphs=["graph_redo_log_active_count"],
                Active_redo_log_count=MetricData(
                    label="Active Count",
                    color=MetricColor.blue,
                    per_second_calculation=False,
                    visible=False,
                    create_switch=False,
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
                Threads_connected=MetricData(
                    label="Connected",
                    color=MetricColor.green,
                    per_second_calculation=False,
                    visible=False,
                ),
                Threads_running=MetricData(
                    label="Running",
                    color=MetricColor.blue,
                    per_second_calculation=False,
                ),
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
                io_write=MetricData(label="Write", color=MetricColor.yellow),
            ),
            locks=LocksMetrics(
                graphs=["graph_locks"],
                metadata_lock_count=MetricData(
                    label="Metadata",
                    color=MetricColor.red,
                    per_second_calculation=False,
                ),
            ),
            proxysql_connections=ProxySQLConnectionsMetrics(
                graphs=["graph_proxysql_connections"],
                Client_Connections_aborted=MetricData(label="FE (aborted)", color=MetricColor.gray),
                Client_Connections_connected=MetricData(
                    label="FE (connected)",
                    color=MetricColor.green,
                    per_second_calculation=False,
                    visible=False,
                ),
                Client_Connections_created=MetricData(label="FE (created)", color=MetricColor.yellow),
                Server_Connections_aborted=MetricData(label="BE (aborted)", color=MetricColor.red),
                Server_Connections_connected=MetricData(
                    label="BE (connected)",
                    color=MetricColor.green,
                    per_second_calculation=False,
                    visible=False,
                ),
                Server_Connections_created=MetricData(label="BE (created)", color=MetricColor.blue),
                Access_Denied_Wrong_Password=MetricData(label="Wrong Password", color=MetricColor.purple),
                Client_Connections_non_idle=MetricData(
                    label="FE (non-idle)",
                    color=MetricColor.green,
                    per_second_calculation=False,
                    visible=True,
                ),
            ),
            proxysql_queries_data_network=ProxySQLQueriesDataNetwork(
                graphs=["graph_proxysql_queries_data_network"],
                Queries_backends_bytes_recv=MetricData(label="BE Recv", color=MetricColor.blue),
                Queries_backends_bytes_sent=MetricData(label="BE Sent", color=MetricColor.green),
                Queries_frontends_bytes_recv=MetricData(label="FE Recv", color=MetricColor.purple),
                Queries_frontends_bytes_sent=MetricData(label="FE Sent", color=MetricColor.yellow),
            ),
            proxysql_active_trx=ProxySQLActiveTRX(
                graphs=["graph_proxysql_active_trx"],
                Active_Transactions=MetricData(
                    label="Active TRX",
                    color=MetricColor.blue,
                    per_second_calculation=False,
                    create_switch=False,
                ),
            ),
            proxysql_multiplex_efficiency=ProxySQLMultiplexEfficiency(
                graphs=["graph_proxysql_multiplex_efficiency"],
                proxysql_multiplex_efficiency_ratio=MetricData(
                    label="Multiplex Efficiency",
                    color=MetricColor.blue,
                    per_second_calculation=False,
                    create_switch=False,
                ),
            ),
            proxysql_select_command_stats=ProxySQLSELECTCommandStats(
                graphs=["graph_proxysql_select_command_stats"],
                cnt_100us=MetricData(label="100us", color=MetricColor.gray, visible=False),
                cnt_500us=MetricData(label="500us", color=MetricColor.blue, visible=False),
                cnt_1ms=MetricData(label="1ms", color=MetricColor.green, visible=False),
                cnt_5ms=MetricData(label="5ms", color=MetricColor.green, visible=False),
                cnt_10ms=MetricData(label="10ms", color=MetricColor.green),
                cnt_50ms=MetricData(label="50ms", color=MetricColor.yellow),
                cnt_100ms=MetricData(label="100ms", color=MetricColor.yellow),
                cnt_500ms=MetricData(label="500ms", color=MetricColor.orange),
                cnt_1s=MetricData(label="1s", color=MetricColor.orange),
                cnt_5s=MetricData(label="5s", color=MetricColor.red),
                cnt_10s=MetricData(label="10s", color=MetricColor.purple),
                cnt_INFs=MetricData(label="10s+", color=MetricColor.purple),
            ),
            proxysql_total_command_stats=ProxySQLTotalCommandStats(
                graphs=["graph_proxysql_total_command_stats"],
                cnt_100us=MetricData(label="100us", color=MetricColor.gray, visible=False),
                cnt_500us=MetricData(label="500us", color=MetricColor.blue, visible=False),
                cnt_1ms=MetricData(label="1ms", color=MetricColor.green, visible=False),
                cnt_5ms=MetricData(label="5ms", color=MetricColor.green, visible=False),
                cnt_10ms=MetricData(label="10ms", color=MetricColor.green),
                cnt_50ms=MetricData(label="50ms", color=MetricColor.yellow),
                cnt_100ms=MetricData(label="100ms", color=MetricColor.yellow),
                cnt_500ms=MetricData(label="500ms", color=MetricColor.orange),
                cnt_1s=MetricData(label="1s", color=MetricColor.orange),
                cnt_5s=MetricData(label="5s", color=MetricColor.red),
                cnt_10s=MetricData(label="10s", color=MetricColor.purple),
                cnt_INFs=MetricData(label="10s+", color=MetricColor.purple),
            ),
        )

        # Build the optimized lookup tables
        for metric_instance in self.metrics.__dict__.values():
            if not dataclasses.is_dataclass(metric_instance):
                continue

            source = getattr(metric_instance, "metric_source", MetricSource.NONE)
            conn_source = getattr(metric_instance, "connection_source", [])

            for attr_name, metric_data in metric_instance.__dict__.items():
                if isinstance(metric_data, MetricData):
                    if metric_data.save_history:
                        self._all_metrics_data_history.append(metric_data)

                    # Add to processing list if it has a valid source
                    if source != MetricSource.NONE:
                        self._source_to_metrics_processing[source].append((attr_name, metric_data, conn_source))

    def refresh_data(
        self,
        worker_start_time: datetime,
        polling_latency: float = 0,
        system_utilization: dict[str, int] = None,
        global_variables: dict[str, int | str] = None,
        global_status: dict[str, int] = None,
        innodb_metrics: dict[str, int] = None,
        proxysql_command_stats: list[dict[str, str]] = None,
        disk_io_metrics: dict[str, int] = None,
        metadata_lock_metrics: dict[str, int] = None,
        replication_status: dict[str, int | str] = None,
    ):
        """Ingests new data from a polling worker and updates all metric values."""
        if replication_status is None:
            replication_status = {}
        if metadata_lock_metrics is None:
            metadata_lock_metrics = {}
        if disk_io_metrics is None:
            disk_io_metrics = {}
        if proxysql_command_stats is None:
            proxysql_command_stats = []
        if innodb_metrics is None:
            innodb_metrics = {}
        if global_status is None:
            global_status = {}
        if global_variables is None:
            global_variables = {}
        if system_utilization is None:
            system_utilization = {}
        self.worker_start_time = worker_start_time
        self.polling_latency = polling_latency
        self.system_utilization.update(system_utilization)
        self.global_variables = global_variables
        self.global_status.update(global_status)
        self.innodb_metrics.update(innodb_metrics)
        self.disk_io_metrics.update(disk_io_metrics)
        self.metadata_lock_metrics = metadata_lock_metrics
        self.replication_status = replication_status

        self.proxysql_total_command_stats.clear()
        self.proxysql_select_command_stats.clear()

        # Calculate redo log size
        innodb_redo_log_capacity = self.global_variables.get("innodb_redo_log_capacity", 0)
        innodb_log_file_size = round(
            self.global_variables.get("innodb_log_file_size", 0)
            * self.global_variables.get("innodb_log_files_in_group", 1)
        )
        self.redo_log_size = max(int(innodb_redo_log_capacity), int(innodb_log_file_size))

        if not self.replay_file:
            self.update_proxysql_command_stats(proxysql_command_stats)
            self.update_metrics_per_second_values()
            self.update_metrics_replication_lag()
            self.update_metrics_adaptive_hash_index_hit_ratio()
            self.update_metrics_locks()
            self.update_metrics_last_value()  # Must be last

        self.update_metrics_checkpoint()
        self.metrics.redo_log.redo_log_size = self.redo_log_size

        self.add_metric_datetime()
        self.daemon_cleanup_data()

        self.initialized = True

    def add_metric(self, metric_data: MetricData, value: int):
        """Adds a new data point to a metric's value list."""
        if self.initialized:
            if metric_data.save_history:
                metric_data.values.append(value)
            else:
                # If not saving history, just keep the latest value
                if metric_data.values:
                    metric_data.values[0] = value
                else:
                    metric_data.values.append(value)

    def add_metric_datetime(self):
        """Adds the current worker timestamp to the global datetime list."""
        if self.initialized and not self.replay_file and self.worker_start_time:
            self.datetimes.append(self.worker_start_time.strftime("%d/%m/%y %H:%M:%S"))

    def get_metric_source_data(self, metric_source: MetricSource) -> dict[str, int] | None:
        """Retrieves the raw data dictionary for a given MetricSource."""
        return self._metric_source_map.get(metric_source)

    def update_metrics_per_second_values(self):
        """Iterates over all metrics and calculates their new per-second values
        using the optimized lookup table.
        """
        for source, metric_tuples in self._source_to_metrics_processing.items():
            metric_source_data = self.get_metric_source_data(source)
            if metric_source_data is None:
                continue

            for metric_name, metric_data, conn_source in metric_tuples:
                if self.connection_source not in conn_source:
                    continue

                current_metric_source_value = metric_source_data.get(metric_name, 0)

                if metric_data.last_value is None:
                    metric_data.last_value = current_metric_source_value
                    continue

                if metric_data.per_second_calculation:
                    metric_diff = current_metric_source_value - metric_data.last_value
                    metric_status_per_sec = round(metric_diff / self.polling_latency) if self.polling_latency > 0 else 0
                else:
                    metric_status_per_sec = current_metric_source_value

                # Special case for CPU_Percent smoothing
                if metric_name == "CPU_Percent":
                    if len(metric_data.values) == 1 and metric_data.values[0] == 0:
                        metric_data.values[0] = metric_status_per_sec
                    elif (
                        metric_status_per_sec in {0, 100}
                        and abs(metric_status_per_sec - (metric_data.values[-1] if metric_data.values else 0)) > 10
                    ):
                        recent_values = list(metric_data.values)[-3:]
                        if recent_values:
                            metric_status_per_sec = sum(recent_values) / len(recent_values)

                self.add_metric(metric_data, int(metric_status_per_sec))

    def update_metrics_last_value(self):
        """Updates the 'last_value' for all metrics using the optimized lookup table."""
        for source, metric_tuples in self._source_to_metrics_processing.items():
            metric_source_data = self.get_metric_source_data(source)
            if metric_source_data is None:
                continue

            for metric_name, metric_data, _ in metric_tuples:
                metric_data.last_value = metric_source_data.get(metric_name, 0)

    def update_proxysql_command_stats(self, proxysql_command_stats: list[dict[str, str]]):
        """Parses and aggregates ProxySQL command stats."""
        if self.connection_source != ConnectionSource.proxysql:
            return

        for row in proxysql_command_stats:
            if row.get("Command") == "SELECT":
                self.proxysql_select_command_stats = {key: int(value) for key, value in row.items() if value.isdigit()}

            for key, value in row.items():
                if key.startswith("cnt_") and value.isdigit():
                    int_value = int(value)
                    self.proxysql_total_command_stats[key] = self.proxysql_total_command_stats.get(key, 0) + int_value

    def update_metrics_replication_lag(self):
        """Updates the replication lag metric."""
        self.add_metric(
            self.metrics.replication_lag.lag,
            int(self.replication_status.get("Seconds_Behind", 0)),
        )

    def update_metrics_adaptive_hash_index_hit_ratio(self):
        """Updates the AHI hit ratio metric from its calculated value."""
        hit_ratio = self.calculate_ahi_ratio()
        if hit_ratio is not None:
            self.add_metric(self.metrics.adaptive_hash_index_hit_ratio.hit_ratio, int(hit_ratio))

    def update_metrics_checkpoint(self):
        """Updates the checkpoint metric instance with max/sync flush values."""
        (max_age, sync_flush, _) = self.calculate_checkpoint_age_data()
        self.metrics.checkpoint.checkpoint_age_max = max_age
        self.metrics.checkpoint.checkpoint_age_sync_flush = sync_flush

    def update_metrics_locks(self):
        """Updates the metadata lock count metric."""
        self.add_metric(self.metrics.locks.metadata_lock_count, len(self.metadata_lock_metrics))

    def calculate_checkpoint_age_data(self) -> tuple[int, int, int]:
        """Calculates raw checkpoint age data."""
        current_age = round(self.global_status.get("Innodb_checkpoint_age", 0))
        max_age = self.redo_log_size

        if current_age == 0 or max_age == 0:
            return self.redo_log_size, 0, 0

        sync_flush_age = round(max_age * 0.825)
        return max_age, sync_flush_age, current_age

    def get_formatted_checkpoint_age(self) -> str:
        """Gets a color-formatted string for the checkpoint age percentage."""
        (max_age, _, current_age) = self.calculate_checkpoint_age_data()

        if current_age == 0 or max_age == 0:
            return "N/A"

        checkpoint_age_ratio = round(current_age / max_age * 100, 2)
        color_code = "red" if checkpoint_age_ratio >= 80 else "yellow" if checkpoint_age_ratio >= 60 else "green"
        return f"[{color_code}]{checkpoint_age_ratio}%"

    def calculate_ahi_ratio(self) -> float | None:
        """Calculates the smoothed Adaptive Hash Index hit ratio."""
        if self.global_variables.get("innodb_adaptive_hash_index") == "OFF":
            return None

        current_hits = self.innodb_metrics.get("adaptive_hash_searches", 0)
        current_misses = self.innodb_metrics.get("adaptive_hash_searches_btree", 0)

        last_hits = self.metrics.adaptive_hash_index.adaptive_hash_searches.last_value
        last_misses = self.metrics.adaptive_hash_index.adaptive_hash_searches_btree.last_value

        if last_hits is None or last_misses is None:
            return None

        hits = current_hits - last_hits
        misses = current_misses - last_misses
        total_hits_misses = hits + misses

        if total_hits_misses <= 0:
            return 0.0

        hit_ratio = (hits / total_hits_misses) * 100
        smoothing_factor = 0.5
        smoothed_hit_ratio = self.metrics.adaptive_hash_index_hit_ratio.smoothed_hit_ratio

        if smoothed_hit_ratio is None:
            smoothed_hit_ratio = hit_ratio
        else:
            smoothed_hit_ratio = (1 - smoothing_factor) * smoothed_hit_ratio + smoothing_factor * hit_ratio

        self.metrics.adaptive_hash_index_hit_ratio.smoothed_hit_ratio = smoothed_hit_ratio
        return smoothed_hit_ratio

    def get_formatted_ahi_status(self) -> str:
        """Gets a color-formatted string for the AHI status."""
        if self.global_variables.get("innodb_adaptive_hash_index") == "OFF":
            return "OFF"

        smoothed_hit_ratio: float | None = None
        if self.replay_file:
            if self.metrics.adaptive_hash_index_hit_ratio.hit_ratio.values:
                smoothed_hit_ratio = self.metrics.adaptive_hash_index_hit_ratio.hit_ratio.values[-1]
        else:
            smoothed_hit_ratio = self.metrics.adaptive_hash_index_hit_ratio.smoothed_hit_ratio

        if smoothed_hit_ratio is None:
            return "N/A"
        if smoothed_hit_ratio <= 0.01:
            return "Inactive"

        color_code = "green" if smoothed_hit_ratio > 70 else "yellow" if smoothed_hit_ratio > 50 else "red"
        return f"[{color_code}]{smoothed_hit_ratio:.2f}%[/{color_code}]"

    def daemon_cleanup_data(self):
        """Cleanup data for daemon mode to keep the metrics data small."""
        if not self.daemon_mode or not self.datetimes:
            return

        time_threshold = datetime.now().astimezone() - timedelta(minutes=10)

        # Efficiently pop from the left (O(1) per item)
        while self.datetimes:
            try:
                # Peek at the leftmost datetime
                first_dt = datetime.strptime(self.datetimes[0], "%d/%m/%y %H:%M:%S").astimezone()
                if first_dt < time_threshold:
                    # If it's too old, pop it
                    self.datetimes.popleft()
                    # And pop the corresponding value from all metrics
                    for metric_data in self._all_metrics_data_history:
                        if metric_data.values:
                            metric_data.values.popleft()
                else:
                    # The first item is new enough, so all others are too
                    break
            except (ValueError, IndexError):
                # Handle malformed date or empty deque during check
                try:
                    self.datetimes.popleft()  # Discard bad data
                    for metric_data in self._all_metrics_data_history:
                        if metric_data.values:
                            metric_data.values.popleft()
                except IndexError:
                    break  # Deque is empty
