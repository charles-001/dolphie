from __future__ import annotations

from collections import deque
from collections.abc import Iterator
from dataclasses import dataclass, field, fields
from datetime import datetime, timezone
from enum import Enum
from functools import lru_cache
from threading import Lock
from typing import ClassVar, Final

from dolphie.DataTypes import ConnectionSource, ConnectionSourceType

Color = tuple[int, int, int]
MetricValue = int | float
METRIC_DATETIME_FORMAT: Final = "%d/%m/%y %H:%M:%S"


# Cached because every series stores the identical timestamp strings, so replay
# loads/seeks re-parse the same ~600-entry window once per series (~42k strptime
# calls) when rebuilding history; rolling-window trims benefit too.
@lru_cache(maxsize=4096)
def parse_metric_datetime(value: str) -> datetime | None:
    """Parse a stored metric timestamp as an aware UTC datetime."""
    try:
        return datetime.strptime(value, METRIC_DATETIME_FORMAT).replace(tzinfo=timezone.utc)
    except ValueError:
        return None


class MetricSource(Enum):
    """Enumeration of sources for metric data."""

    SYSTEM_UTILIZATION = "system_utilization"
    GLOBAL_STATUS = "global_status"
    INNODB_METRICS = "innodb_metrics"
    DISK_IO_METRICS = "disk_io_metrics"
    PROXYSQL_SELECT_COMMAND_STATS = "proxysql_select_command_stats"
    PROXYSQL_TOTAL_COMMAND_STATS = "proxysql_total_command_stats"
    NONE = "none"


class MetricColor:
    """Namespace for standard metric graph colors."""

    gray: Final[Color] = (172, 207, 231)
    blue: Final[Color] = (68, 180, 255)
    green: Final[Color] = (84, 239, 174)
    red: Final[Color] = (255, 73, 112)
    yellow: Final[Color] = (252, 213, 121)
    purple: Final[Color] = (191, 121, 252)
    orange: Final[Color] = (252, 121, 121)


class ValueFormat(Enum):
    """How a metric group's values are formatted for display."""

    NUMBER = "number"
    BYTES = "bytes"
    TIME = "time"
    PERCENT = "percent"


@dataclass
class MetricData:
    """Store one metric's values and matching UTC timestamps."""

    label: str
    color: Color
    visible: bool = True
    save_history: bool = True
    per_second_calculation: bool = True
    last_value: int | float | None = None
    graphable: bool = True
    create_switch: bool = True
    # Smooth transient extreme samples (sensor glitches) using recent history.
    smooth_extreme_values: bool = False
    _values: deque[MetricValue] = field(default_factory=deque, init=False, repr=False)
    _datetimes: deque[str] = field(default_factory=deque, init=False, repr=False)
    _polling_intervals: deque[float] = field(default_factory=deque, init=False, repr=False)

    def __post_init__(self) -> None:
        """Create the per-series lock outside the serialized dataclass fields."""
        self._lock = Lock()

    def append_sample(self, value: MetricValue, timestamp: str, polling_interval: float) -> None:
        """Append a value together with the timestamp and interval that produced it."""
        with self._lock:
            if polling_interval <= 0 and self._datetimes:
                polling_interval = self._interval_between(self._datetimes[-1], timestamp)

            if not self.save_history:
                self._values.clear()
                self._datetimes.clear()
                self._polling_intervals.clear()

            self._values.append(value)
            self._datetimes.append(timestamp)
            self._polling_intervals.append(max(polling_interval, 0))

    def replace_latest_sample(self, value: MetricValue) -> None:
        """Replace the latest value while retaining its timestamp metadata."""
        with self._lock:
            if self._values:
                self._values[-1] = value

    def replace_history(self, values: list[MetricValue], datetimes: list[str]) -> None:
        """Replace history from replay data, tail-aligning values with timestamps."""
        pair_count = min(len(values), len(datetimes))
        if pair_count == 0:
            self.clear_history()
            return

        paired_values = values[-pair_count:]
        paired_datetimes = datetimes[-pair_count:]
        polling_intervals = self._derive_polling_intervals(paired_datetimes)

        with self._lock:
            self._values = deque(paired_values)
            self._datetimes = deque(paired_datetimes)
            self._polling_intervals = deque(polling_intervals)

    def extend_history(self, values: list[MetricValue], datetimes: list[str]) -> None:
        """Append replay history, tail-aligning values with timestamps."""
        pair_count = min(len(values), len(datetimes))
        if pair_count == 0:
            return

        paired_values = values[-pair_count:]
        paired_datetimes = datetimes[-pair_count:]

        with self._lock:
            for index, value in enumerate(paired_values):
                timestamp = paired_datetimes[index]
                interval = self._interval_between(self._datetimes[-1], timestamp) if self._datetimes else 0
                self._values.append(value)
                self._datetimes.append(timestamp)
                self._polling_intervals.append(interval)

    def snapshot(self) -> tuple[list[str], list[MetricValue], list[float]]:
        """Return an atomic copy of the series for rendering or serialization."""
        with self._lock:
            return list(self._datetimes), list(self._values), list(self._polling_intervals)

    def latest_value(self) -> MetricValue | None:
        """Return the newest stored value, if present."""
        with self._lock:
            return self._values[-1] if self._values else None

    def values_snapshot(self) -> list[MetricValue]:
        """Return an atomic copy of only the stored values."""
        with self._lock:
            return list(self._values)

    def recent_values(self, count: int) -> tuple[list[MetricValue], int]:
        """Return up to the newest count values and the total stored sample count."""
        with self._lock:
            total = len(self._values)
            start = max(total - count, 0)
            return [self._values[index] for index in range(start, total)], total

    def clear_history(self) -> None:
        """Clear values and all matching sample metadata."""
        with self._lock:
            self._values.clear()
            self._datetimes.clear()
            self._polling_intervals.clear()

    def trim_before(self, threshold: datetime) -> bool:
        """Remove samples older than an aware UTC threshold."""
        trimmed = False
        with self._lock:
            while self._datetimes:
                first_dt = parse_metric_datetime(self._datetimes[0])
                if first_dt is None:
                    self._popleft_sample()
                    trimmed = True
                    continue

                if first_dt >= threshold:
                    break

                self._popleft_sample()
                trimmed = True

        return trimmed

    def _popleft_sample(self) -> None:
        """Remove one complete sample while the caller holds the series lock."""
        if self._datetimes:
            self._datetimes.popleft()
        if self._values:
            self._values.popleft()
        if self._polling_intervals:
            self._polling_intervals.popleft()

    @staticmethod
    def _derive_polling_intervals(datetimes: list[str]) -> list[float]:
        """Derive best-effort intervals for replay samples."""
        intervals: list[float] = []
        previous: datetime | None = None
        for timestamp in datetimes:
            current = parse_metric_datetime(timestamp)

            interval = max((current - previous).total_seconds(), 0) if current and previous else 0
            intervals.append(interval)
            previous = current

        return intervals

    @staticmethod
    def _interval_between(previous: str, current: str) -> float:
        """Return the non-negative interval between two stored UTC timestamps."""
        previous_dt = parse_metric_datetime(previous)
        current_dt = parse_metric_datetime(current)
        if previous_dt is None or current_dt is None:
            return 0
        return max((current_dt - previous_dt).total_seconds(), 0)


class MetricGroup:
    """Shared immutable metadata contract for a group of related metrics."""

    metric_source: ClassVar[MetricSource]
    connection_source: ClassVar[tuple[ConnectionSourceType, ...]]
    use_with_replay: ClassVar[bool] = True
    value_format: ClassVar[ValueFormat] = ValueFormat.NUMBER


@dataclass
class SystemCPUMetrics(MetricGroup):
    CPU_Percent: MetricData
    metric_source = MetricSource.SYSTEM_UTILIZATION
    connection_source = (ConnectionSource.mysql, ConnectionSource.proxysql)


@dataclass
class SystemMemoryMetrics(MetricGroup):
    Memory_Total: MetricData
    Memory_Used: MetricData
    metric_source = MetricSource.SYSTEM_UTILIZATION
    connection_source = (ConnectionSource.mysql, ConnectionSource.proxysql)
    value_format = ValueFormat.BYTES


@dataclass
class SystemNetworkMetrics(MetricGroup):
    Network_Down: MetricData
    Network_Up: MetricData
    metric_source = MetricSource.SYSTEM_UTILIZATION
    connection_source = (ConnectionSource.mysql, ConnectionSource.proxysql)
    value_format = ValueFormat.BYTES


@dataclass
class SystemDiskIOMetrics(MetricGroup):
    Disk_Read: MetricData
    Disk_Write: MetricData
    metric_source = MetricSource.SYSTEM_UTILIZATION
    connection_source = (ConnectionSource.mysql, ConnectionSource.proxysql)


@dataclass
class DMLMetrics(MetricGroup):
    Queries: MetricData
    Com_select: MetricData
    Com_insert: MetricData
    Com_update: MetricData
    Com_delete: MetricData
    Com_replace: MetricData
    Com_commit: MetricData
    Com_rollback: MetricData
    metric_source = MetricSource.GLOBAL_STATUS
    connection_source = (ConnectionSource.mysql, ConnectionSource.proxysql)


@dataclass
class ReplicationLagMetrics(MetricGroup):
    lag: MetricData
    metric_source = MetricSource.NONE
    connection_source = (ConnectionSource.mysql,)
    value_format = ValueFormat.TIME


@dataclass
class CheckpointMetrics(MetricGroup):
    Innodb_checkpoint_age: MetricData
    metric_source = MetricSource.GLOBAL_STATUS
    connection_source = (ConnectionSource.mysql,)
    value_format = ValueFormat.BYTES
    checkpoint_age_max: int = 0
    checkpoint_age_sync_flush: int = 0


@dataclass
class BufferPoolRequestsMetrics(MetricGroup):
    Innodb_buffer_pool_read_requests: MetricData
    Innodb_buffer_pool_write_requests: MetricData
    Innodb_buffer_pool_reads: MetricData
    metric_source = MetricSource.GLOBAL_STATUS
    connection_source = (ConnectionSource.mysql,)


@dataclass
class AdaptiveHashIndexMetrics(MetricGroup):
    adaptive_hash_searches: MetricData
    adaptive_hash_searches_btree: MetricData
    metric_source = MetricSource.INNODB_METRICS
    connection_source = (ConnectionSource.mysql,)


@dataclass
class AdaptiveHashIndexHitRatio(MetricGroup):
    hit_ratio: MetricData
    smoothed_hit_ratio: float | None = None
    metric_source = MetricSource.NONE
    connection_source = (ConnectionSource.mysql,)
    value_format = ValueFormat.PERCENT


@dataclass
class RedoLogMetrics(MetricGroup):
    Innodb_lsn_current: MetricData
    metric_source = MetricSource.GLOBAL_STATUS
    connection_source = (ConnectionSource.mysql,)
    value_format = ValueFormat.BYTES
    redo_log_size: int = 0


@dataclass
class RedoLogActiveCountMetrics(MetricGroup):
    Active_redo_log_count: MetricData
    metric_source = MetricSource.GLOBAL_STATUS
    connection_source = (ConnectionSource.mysql,)


@dataclass
class TableCacheMetrics(MetricGroup):
    Table_open_cache_hits: MetricData
    Table_open_cache_misses: MetricData
    Table_open_cache_overflows: MetricData
    metric_source = MetricSource.GLOBAL_STATUS
    connection_source = (ConnectionSource.mysql,)


@dataclass
class ThreadMetrics(MetricGroup):
    Threads_connected: MetricData
    Threads_running: MetricData
    metric_source = MetricSource.GLOBAL_STATUS
    connection_source = (ConnectionSource.mysql,)


@dataclass
class TemporaryObjectMetrics(MetricGroup):
    Created_tmp_tables: MetricData
    Created_tmp_disk_tables: MetricData
    Created_tmp_files: MetricData
    metric_source = MetricSource.GLOBAL_STATUS
    connection_source = (ConnectionSource.mysql,)


@dataclass
class AbortedConnectionsMetrics(MetricGroup):
    Aborted_clients: MetricData
    Aborted_connects: MetricData
    metric_source = MetricSource.GLOBAL_STATUS
    connection_source = (ConnectionSource.mysql,)


@dataclass
class DiskIOMetrics(MetricGroup):
    io_read: MetricData
    io_write: MetricData
    metric_source = MetricSource.DISK_IO_METRICS
    connection_source = (ConnectionSource.mysql,)
    value_format = ValueFormat.BYTES


@dataclass
class LocksMetrics(MetricGroup):
    metadata_lock_count: MetricData
    metric_source = MetricSource.NONE
    connection_source = (ConnectionSource.mysql,)


@dataclass
class HistoryListLength(MetricGroup):
    trx_rseg_history_len: MetricData
    metric_source = MetricSource.INNODB_METRICS
    connection_source = (ConnectionSource.mysql,)


@dataclass
class ProxySQLConnectionsMetrics(MetricGroup):
    Client_Connections_non_idle: MetricData
    Client_Connections_aborted: MetricData
    Client_Connections_connected: MetricData
    Client_Connections_created: MetricData
    Server_Connections_aborted: MetricData
    Server_Connections_connected: MetricData
    Server_Connections_created: MetricData
    Access_Denied_Wrong_Password: MetricData
    metric_source = MetricSource.GLOBAL_STATUS
    connection_source = (ConnectionSource.proxysql,)


@dataclass
class ProxySQLQueriesDataNetwork(MetricGroup):
    Queries_backends_bytes_recv: MetricData
    Queries_backends_bytes_sent: MetricData
    Queries_frontends_bytes_recv: MetricData
    Queries_frontends_bytes_sent: MetricData
    metric_source = MetricSource.GLOBAL_STATUS
    connection_source = (ConnectionSource.proxysql,)
    value_format = ValueFormat.BYTES


@dataclass
class ProxySQLActiveTRX(MetricGroup):
    Active_Transactions: MetricData
    metric_source = MetricSource.GLOBAL_STATUS
    connection_source = (ConnectionSource.proxysql,)


@dataclass
class ProxySQLMultiplexEfficiency(MetricGroup):
    proxysql_multiplex_efficiency_ratio: MetricData
    metric_source = MetricSource.GLOBAL_STATUS
    connection_source = (ConnectionSource.proxysql,)
    value_format = ValueFormat.PERCENT


# One entry per ProxySQL latency bucket: (field name, label, color, visible).
_COMMAND_STAT_BUCKET_STYLES: Final = (
    ("cnt_100us", "100us", MetricColor.gray, False),
    ("cnt_500us", "500us", MetricColor.blue, False),
    ("cnt_1ms", "1ms", MetricColor.green, False),
    ("cnt_5ms", "5ms", MetricColor.green, False),
    ("cnt_10ms", "10ms", MetricColor.green, True),
    ("cnt_50ms", "50ms", MetricColor.yellow, True),
    ("cnt_100ms", "100ms", MetricColor.yellow, True),
    ("cnt_500ms", "500ms", MetricColor.orange, True),
    ("cnt_1s", "1s", MetricColor.orange, True),
    ("cnt_5s", "5s", MetricColor.red, True),
    ("cnt_10s", "10s", MetricColor.purple, True),
    ("cnt_INFs", "10s+", MetricColor.purple, True),
)
COMMAND_STAT_BUCKETS: Final = tuple(name for name, _, _, _ in _COMMAND_STAT_BUCKET_STYLES)


def _command_stat_metric_data() -> dict[str, MetricData]:
    """Create one set of latency-bucket series for a command stats group."""
    return {
        name: MetricData(label=label, color=color, visible=visible)
        for name, label, color, visible in _COMMAND_STAT_BUCKET_STYLES
    }


@dataclass
class ProxySQLSELECTCommandStats(MetricGroup):
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
    metric_source = MetricSource.PROXYSQL_SELECT_COMMAND_STATS
    connection_source = (ConnectionSource.proxysql,)


@dataclass
class ProxySQLTotalCommandStats(ProxySQLSELECTCommandStats):
    metric_source = MetricSource.PROXYSQL_TOTAL_COMMAND_STATS


MetricInstance = (
    SystemCPUMetrics
    | SystemMemoryMetrics
    | SystemNetworkMetrics
    | SystemDiskIOMetrics
    | DMLMetrics
    | ReplicationLagMetrics
    | CheckpointMetrics
    | BufferPoolRequestsMetrics
    | AdaptiveHashIndexMetrics
    | AdaptiveHashIndexHitRatio
    | RedoLogMetrics
    | RedoLogActiveCountMetrics
    | TableCacheMetrics
    | ThreadMetrics
    | TemporaryObjectMetrics
    | AbortedConnectionsMetrics
    | DiskIOMetrics
    | LocksMetrics
    | HistoryListLength
    | ProxySQLConnectionsMetrics
    | ProxySQLQueriesDataNetwork
    | ProxySQLActiveTRX
    | ProxySQLMultiplexEfficiency
    | ProxySQLSELECTCommandStats
    | ProxySQLTotalCommandStats
)


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


def iter_metric_instances(metrics: MetricInstances) -> Iterator[tuple[str, MetricInstance]]:
    """Yield metric catalog field names and their typed instances."""
    for metric_field in fields(metrics):
        value = getattr(metrics, metric_field.name)
        assert isinstance(value, MetricInstance)
        yield metric_field.name, value


def iter_metric_data(metric_instance: MetricInstance) -> Iterator[tuple[str, MetricData]]:
    """Yield MetricData fields from a metric instance."""
    for metric_field in fields(metric_instance):
        value = getattr(metric_instance, metric_field.name)
        if isinstance(value, MetricData):
            yield metric_field.name, value


def create_metric_instances() -> MetricInstances:
    """Create the complete metric catalog with its default configuration."""
    metrics = MetricInstances(
        system_cpu=SystemCPUMetrics(
            CPU_Percent=MetricData(
                label="CPU %",
                color=MetricColor.blue,
                per_second_calculation=False,
                create_switch=False,
                smooth_extreme_values=True,
            ),
        ),
        system_memory=SystemMemoryMetrics(
            Memory_Total=MetricData(
                label="Total",
                color=MetricColor.blue,
                per_second_calculation=False,
                visible=False,
                save_history=False,
                create_switch=False,
                graphable=False,
            ),
            Memory_Used=MetricData(
                label="Memory Used",
                color=MetricColor.green,
                per_second_calculation=False,
                create_switch=False,
            ),
        ),
        system_disk_io=SystemDiskIOMetrics(
            Disk_Read=MetricData(label="IOPS Read", color=MetricColor.blue),
            Disk_Write=MetricData(label="IOPS Write", color=MetricColor.yellow),
        ),
        system_network=SystemNetworkMetrics(
            Network_Down=MetricData(label="Net Dn", color=MetricColor.blue),
            Network_Up=MetricData(label="Net Up", color=MetricColor.gray),
        ),
        dml=DMLMetrics(
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
            lag=MetricData(
                label="Lag",
                color=MetricColor.blue,
                per_second_calculation=False,
                create_switch=False,
            ),
        ),
        checkpoint=CheckpointMetrics(
            Innodb_checkpoint_age=MetricData(
                label="Uncheckpointed",
                color=MetricColor.green,
                per_second_calculation=False,
                create_switch=False,
            ),
        ),
        buffer_pool_requests=BufferPoolRequestsMetrics(
            Innodb_buffer_pool_read_requests=MetricData(label="Read Requests", color=MetricColor.blue),
            Innodb_buffer_pool_write_requests=MetricData(label="Write Requests", color=MetricColor.green),
            Innodb_buffer_pool_reads=MetricData(label="Disk Reads", color=MetricColor.red),
        ),
        history_list_length=HistoryListLength(
            trx_rseg_history_len=MetricData(
                label="HLL",
                color=MetricColor.blue,
                per_second_calculation=False,
                create_switch=False,
            ),
        ),
        adaptive_hash_index=AdaptiveHashIndexMetrics(
            adaptive_hash_searches=MetricData(label="Hit", color=MetricColor.green),
            adaptive_hash_searches_btree=MetricData(label="Miss", color=MetricColor.red),
        ),
        adaptive_hash_index_hit_ratio=AdaptiveHashIndexHitRatio(
            hit_ratio=MetricData(
                label="Hit Ratio",
                color=MetricColor.green,
                per_second_calculation=False,
                create_switch=False,
            ),
        ),
        redo_log=RedoLogMetrics(
            Innodb_lsn_current=MetricData(label="Data Written", color=MetricColor.blue, create_switch=False),
        ),
        redo_log_active_count=RedoLogActiveCountMetrics(
            Active_redo_log_count=MetricData(
                label="Active Count",
                color=MetricColor.blue,
                per_second_calculation=False,
                visible=False,
                create_switch=False,
            ),
        ),
        table_cache=TableCacheMetrics(
            Table_open_cache_hits=MetricData(label="Hit", color=MetricColor.green),
            Table_open_cache_misses=MetricData(label="Miss", color=MetricColor.red),
            Table_open_cache_overflows=MetricData(label="Overflow", color=MetricColor.yellow),
        ),
        threads=ThreadMetrics(
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
            Created_tmp_tables=MetricData(label="Tables", color=MetricColor.blue),
            Created_tmp_disk_tables=MetricData(label="Disk", color=MetricColor.red),
            Created_tmp_files=MetricData(label="Files", color=MetricColor.yellow),
        ),
        aborted_connections=AbortedConnectionsMetrics(
            Aborted_clients=MetricData(label="Client (timeout)", color=MetricColor.blue),
            Aborted_connects=MetricData(label="Connects (attempt)", color=MetricColor.red),
        ),
        disk_io=DiskIOMetrics(
            io_read=MetricData(label="Read", color=MetricColor.blue),
            io_write=MetricData(label="Write", color=MetricColor.yellow),
        ),
        locks=LocksMetrics(
            metadata_lock_count=MetricData(
                label="Metadata",
                color=MetricColor.red,
                per_second_calculation=False,
            ),
        ),
        proxysql_connections=ProxySQLConnectionsMetrics(
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
            Queries_backends_bytes_recv=MetricData(label="BE Recv", color=MetricColor.blue),
            Queries_backends_bytes_sent=MetricData(label="BE Sent", color=MetricColor.green),
            Queries_frontends_bytes_recv=MetricData(label="FE Recv", color=MetricColor.purple),
            Queries_frontends_bytes_sent=MetricData(label="FE Sent", color=MetricColor.yellow),
        ),
        proxysql_active_trx=ProxySQLActiveTRX(
            Active_Transactions=MetricData(
                label="Active TRX",
                color=MetricColor.blue,
                per_second_calculation=False,
                create_switch=False,
            ),
        ),
        proxysql_multiplex_efficiency=ProxySQLMultiplexEfficiency(
            proxysql_multiplex_efficiency_ratio=MetricData(
                label="Multiplex Efficiency",
                color=MetricColor.blue,
                per_second_calculation=False,
                create_switch=False,
            ),
        ),
        proxysql_select_command_stats=ProxySQLSELECTCommandStats(**_command_stat_metric_data()),
        proxysql_total_command_stats=ProxySQLTotalCommandStats(**_command_stat_metric_data()),
    )

    return metrics
