from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Union

import plotext as plt
from dolphie.DataTypes import ConnectionSource
from dolphie.Modules.Functions import format_bytes, format_number, format_time
from rich.text import Text
from textual.widgets import Static


class Graph(Static):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.marker = None
        self.metric_instance = None
        self.datetimes = None

    def on_show(self) -> None:
        self.render_graph(self.metric_instance, self.datetimes)

    def on_resize(self) -> None:
        self.render_graph(self.metric_instance, self.datetimes)

    def render_graph(self, metric_instance, datetimes) -> None:
        self.metric_instance = metric_instance
        self.datetimes = datetimes

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
            x = self.datetimes
            y = self.metric_instance.Innodb_checkpoint_age.values

            if y and x:
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
        elif isinstance(self.metric_instance, RedoLogMetrics):
            if "graph_redo_log_bar" in self.id:
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
            else:
                x = self.datetimes
                y = self.metric_instance.Innodb_lsn_current.values

                if y and x:
                    plt.plot(
                        x,
                        y,
                        marker=self.marker,
                        label=self.metric_instance.Innodb_lsn_current.label,
                        color=self.metric_instance.Innodb_lsn_current.color,
                    )
                    max_y_value = max(max_y_value, max(y))
        elif isinstance(self.metric_instance, RedoLogActiveCountMetrics):
            x = self.datetimes
            y = self.metric_instance.Active_redo_log_count.values

            if y and x:
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
                    x = self.datetimes
                    y = metric_data.values

                    if y and x:
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
        AdaptiveHashIndexHitRatio: lambda val: f"{round(val)}%",
        ProxySQLMultiplexEfficiency: lambda val: f"{round(val)}%",
        DiskIOMetrics: lambda val: format_bytes(val, color=color),
        ProxySQLQueriesDataNetwork: lambda val: format_bytes(val, color=color),
    }

    return data_formatters.get(type(data), lambda val: format_number(val, color=color))


@dataclass
class MetricSource:
    global_status: str = "global_status"
    innodb_metrics: str = "innodb_metrics"
    disk_io_metrics: str = "disk_io_metrics"
    proxysql_select_command_stats: str = "proxysql_select_command_stats"
    proxysql_total_command_stats: str = "proxysql_total_command_stats"
    none: str = "none"


@dataclass
class MetricColor:
    gray: tuple = (172, 207, 231)
    blue: tuple = (68, 180, 255)
    green: tuple = (84, 239, 174)
    red: tuple = (255, 73, 112)
    yellow: tuple = (252, 213, 121)
    purple: tuple = (191, 121, 252)
    orange: tuple = (252, 121, 121)


@dataclass
class MetricData:
    label: str
    color: tuple
    visible: bool = True
    save_history: bool = True
    per_second_calculation: bool = True
    last_value: int = None
    graphable: bool = True
    create_switch: bool = True
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
    graph_tab_name = "DML"
    metric_source: MetricSource = MetricSource.global_status
    connection_source: List[ConnectionSource] = field(
        default_factory=lambda: [ConnectionSource.mysql, ConnectionSource.proxysql]
    )
    use_with_replay: bool = True


@dataclass
class ReplicationLagMetrics:
    lag: MetricData
    graphs: List[str]
    tab_name: str = "replication_lag"
    graph_tab_name = "Replication"
    metric_source: MetricSource = MetricSource.none
    connection_source: List[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.mysql])
    use_with_replay: bool = True


@dataclass
class CheckpointMetrics:
    Innodb_checkpoint_age: MetricData
    graphs: List[str]
    tab_name: str = "checkpoint"
    graph_tab_name = "Checkpoint"
    metric_source: MetricSource = MetricSource.global_status
    checkpoint_age_max: int = 0
    checkpoint_age_sync_flush: int = 0
    connection_source: List[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.mysql])
    use_with_replay: bool = True


@dataclass
class BufferPoolRequestsMetrics:
    Innodb_buffer_pool_read_requests: MetricData
    Innodb_buffer_pool_write_requests: MetricData
    Innodb_buffer_pool_reads: MetricData
    graphs: List[str]
    tab_name: str = "buffer_pool_requests"
    graph_tab_name = "BP Requests"
    metric_source: MetricSource = MetricSource.global_status
    connection_source: List[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.mysql])
    use_with_replay: bool = True


@dataclass
class AdaptiveHashIndexMetrics:
    adaptive_hash_searches: MetricData
    adaptive_hash_searches_btree: MetricData
    graphs: List[str]
    tab_name: str = "adaptive_hash_index"
    graph_tab_name = "AHI"
    metric_source: MetricSource = MetricSource.innodb_metrics
    connection_source: List[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.mysql])
    use_with_replay: bool = True


@dataclass
class AdaptiveHashIndexHitRatio:
    hit_ratio: MetricData
    graphs: List[str]
    smoothed_hit_ratio: float = None
    tab_name: str = "adaptive_hash_index"
    graph_tab_name = "AHI"
    metric_source: MetricSource = MetricSource.none
    connection_source: List[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.mysql])
    use_with_replay: bool = True


@dataclass
class RedoLogMetrics:
    Innodb_lsn_current: MetricData
    graphs: List[str]
    tab_name: str = "redo_log"
    graph_tab_name = "Redo Log"
    redo_log_size: int = 0
    metric_source: MetricSource = MetricSource.global_status
    connection_source: List[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.mysql])
    use_with_replay: bool = True


@dataclass
class RedoLogActiveCountMetrics:
    Active_redo_log_count: MetricData
    graphs: List[str]
    tab_name: str = "redo_log"
    graph_tab_name = "Redo Log"
    metric_source: MetricSource = MetricSource.global_status
    connection_source: List[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.mysql])
    use_with_replay: bool = True


@dataclass
class TableCacheMetrics:
    Table_open_cache_hits: MetricData
    Table_open_cache_misses: MetricData
    Table_open_cache_overflows: MetricData
    graphs: List[str]
    tab_name: str = "table_cache"
    graph_tab_name = "Table Cache"
    metric_source: MetricSource = MetricSource.global_status
    connection_source: List[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.mysql])
    use_with_replay: bool = True


@dataclass
class ThreadMetrics:
    Threads_connected: MetricData
    Threads_running: MetricData
    graphs: List[str]
    tab_name: str = "threads"
    graph_tab_name = "Threads"
    metric_source: MetricSource = MetricSource.global_status
    connection_source: List[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.mysql])
    use_with_replay: bool = True


@dataclass
class TemporaryObjectMetrics:
    Created_tmp_tables: MetricData
    Created_tmp_disk_tables: MetricData
    Created_tmp_files: MetricData
    graphs: List[str]
    tab_name: str = "temporary_objects"
    graph_tab_name = "Temp Objects"
    metric_source: MetricSource = MetricSource.global_status
    connection_source: List[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.mysql])
    use_with_replay: bool = True


@dataclass
class AbortedConnectionsMetrics:
    Aborted_clients: MetricData
    Aborted_connects: MetricData
    graphs: List[str]
    tab_name: str = "aborted_connections"
    graph_tab_name = "Aborted Connections"
    metric_source: MetricSource = MetricSource.global_status
    connection_source: List[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.mysql])
    use_with_replay: bool = True


@dataclass
class DiskIOMetrics:
    io_read: MetricData
    io_write: MetricData
    graphs: List[str]
    tab_name: str = "disk_io"
    graph_tab_name = "Disk I/O"
    metric_source: MetricSource = MetricSource.disk_io_metrics
    connection_source: List[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.mysql])
    use_with_replay: bool = True


@dataclass
class LocksMetrics:
    metadata_lock_count: MetricData
    graphs: List[str]
    tab_name: str = "locks"
    graph_tab_name = "Locks"
    metric_source: MetricSource = MetricSource.none
    connection_source: List[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.mysql])
    use_with_replay: bool = True


@dataclass
class HistoryListLength:
    trx_rseg_history_len: MetricData
    graphs: List[str]
    tab_name: str = "history_list_length"
    graph_tab_name = "History List"
    metric_source: MetricSource = MetricSource.innodb_metrics
    connection_source: List[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.mysql])
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
    graphs: List[str]
    tab_name: str = "proxysql_connections"
    graph_tab_name = "Connections"
    metric_source: MetricSource = MetricSource.global_status
    connection_source: List[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.proxysql])
    use_with_replay: bool = True


@dataclass
class ProxySQLQueriesDataNetwork:
    Queries_backends_bytes_recv: MetricData
    Queries_backends_bytes_sent: MetricData
    Queries_frontends_bytes_recv: MetricData
    Queries_frontends_bytes_sent: MetricData
    graphs: List[str]
    tab_name: str = "proxysql_queries_data_network"
    graph_tab_name = "Query Data Rates"
    metric_source: MetricSource = MetricSource.global_status
    connection_source: List[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.proxysql])
    use_with_replay: bool = True


@dataclass
class ProxySQLActiveTRX:
    Active_Transactions: MetricData
    graphs: List[str]
    tab_name: str = "proxysql_active_trx"
    graph_tab_name = "Active TRX"
    metric_source: MetricSource = MetricSource.global_status
    connection_source: List[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.proxysql])
    use_with_replay: bool = True


@dataclass
class ProxySQLMultiplexEfficiency:
    proxysql_multiplex_efficiency_ratio: MetricData
    graphs: List[str]
    tab_name: str = "proxysql_multiplex_efficiency"
    graph_tab_name = "Multiplex Efficiency"
    metric_source: MetricSource = MetricSource.global_status
    connection_source: List[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.proxysql])
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
    graphs: List[str]
    tab_name: str = "proxysql_select_command_stats"
    graph_tab_name = "SELECT Command Stats"
    metric_source: MetricSource = MetricSource.proxysql_select_command_stats
    connection_source: List[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.proxysql])
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
    graphs: List[str]
    tab_name: str = "proxysql_total_command_stats"
    graph_tab_name = "Total Command Stats"
    metric_source: MetricSource = MetricSource.proxysql_total_command_stats
    connection_source: List[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.proxysql])
    use_with_replay: bool = True


@dataclass
class MetricInstances:
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
    def __init__(self, replay_file: str, daemon_mode: bool = False):
        self.connection_source = ConnectionSource.mysql
        self.replay_file = replay_file
        self.daemon_mode = daemon_mode

        self.reset()

    def reset(self):
        self.initialized: bool = False
        self.polling_latency: float = 0
        self.global_variables: Dict[str, Union[int, str]] = None
        self.global_status: Dict[str, int] = None
        self.redo_log_size: int = 0

        self.datetimes: List[str] = []
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
                lag=MetricData(label="Lag", color=MetricColor.blue, per_second_calculation=False, create_switch=False),
            ),
            checkpoint=CheckpointMetrics(
                graphs=["graph_checkpoint"],
                Innodb_checkpoint_age=MetricData(
                    label="Uncheckpointed", color=MetricColor.blue, per_second_calculation=False, create_switch=False
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
                    label="HLL", color=MetricColor.blue, per_second_calculation=False, create_switch=False
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
                    label="Hit Ratio", color=MetricColor.green, per_second_calculation=False, create_switch=False
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
                    label="Connected", color=MetricColor.green, per_second_calculation=False, visible=False
                ),
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
                metadata_lock_count=MetricData(label="Metadata", color=MetricColor.red, per_second_calculation=False),
            ),
            proxysql_connections=ProxySQLConnectionsMetrics(
                graphs=["graph_proxysql_connections"],
                Client_Connections_aborted=MetricData(label="FE (aborted)", color=MetricColor.gray),
                Client_Connections_connected=MetricData(
                    label="FE (connected)", color=MetricColor.green, per_second_calculation=False, visible=False
                ),
                Client_Connections_created=MetricData(label="FE (created)", color=MetricColor.yellow),
                Server_Connections_aborted=MetricData(label="BE (aborted)", color=MetricColor.red),
                Server_Connections_connected=MetricData(
                    label="BE (connected)", color=MetricColor.green, per_second_calculation=False, visible=False
                ),
                Server_Connections_created=MetricData(label="BE (created)", color=MetricColor.blue),
                Access_Denied_Wrong_Password=MetricData(label="Wrong Password", color=MetricColor.purple),
                Client_Connections_non_idle=MetricData(
                    label="FE (non-idle)", color=MetricColor.green, per_second_calculation=False, visible=True
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
                    label="Active TRX", color=MetricColor.blue, per_second_calculation=False, create_switch=False
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

    def refresh_data(
        self,
        worker_start_time: datetime = None,
        polling_latency: float = 0,
        global_variables: Dict[str, Union[int, str]] = {},
        global_status: Dict[str, int] = {},
        innodb_metrics: Dict[str, int] = {},
        proxysql_command_stats: Dict[str, int] = {},
        disk_io_metrics: Dict[str, int] = {},
        metadata_lock_metrics: Dict[str, int] = {},
        replication_status: Dict[str, Union[int, str]] = {},
    ):
        self.worker_start_time = worker_start_time
        self.polling_latency = polling_latency
        self.global_variables = global_variables
        self.global_status = global_status
        self.innodb_metrics = innodb_metrics
        self.disk_io_metrics = disk_io_metrics
        self.metadata_lock_metrics = metadata_lock_metrics
        self.replication_status = replication_status
        self.proxysql_total_command_stats = {}
        self.proxysql_select_command_stats = {}

        # Support MySQL 8.0.30+ redo log size variable
        innodb_redo_log_capacity = self.global_variables.get("innodb_redo_log_capacity", 0)
        innodb_log_file_size = round(
            self.global_variables.get("innodb_log_file_size", 0)
            * self.global_variables.get("innodb_log_files_in_group", 1)
        )
        self.redo_log_size = max(innodb_redo_log_capacity, innodb_log_file_size)

        if not self.replay_file:
            self.update_proxysql_command_stats(proxysql_command_stats)

            self.update_metrics_per_second_values()
            self.update_metrics_replication_lag()
            self.update_metrics_checkpoint()
            self.update_metrics_adaptive_hash_index_hit_ratio()
            self.update_metrics_locks()
            self.update_metrics_last_value()
        else:
            self.update_metrics_checkpoint()

        self.metrics.redo_log.redo_log_size = self.redo_log_size

        self.add_metric_datetime()
        self.daemon_cleanup_data()

        # Set the initialized flag after the first refresh since we now have last value data for differences
        # This lets us sync all metric values to a datetime
        self.initialized = True

    def add_metric(self, metric_data: MetricData, value: int):
        if self.initialized:
            if metric_data.save_history:
                metric_data.values.append(value)
            else:
                metric_data.values = [value]

    def add_metric_datetime(self):
        if self.initialized and not self.replay_file:
            self.datetimes.append(self.worker_start_time.strftime("%d/%m/%y %H:%M:%S"))

    def get_metric_source_data(self, metric_source):
        if metric_source == MetricSource.global_status:
            metric_source_data = self.global_status
        elif metric_source == MetricSource.innodb_metrics:
            metric_source_data = self.innodb_metrics
        elif metric_source == MetricSource.disk_io_metrics:
            metric_source_data = self.disk_io_metrics
        elif metric_source == MetricSource.proxysql_select_command_stats:
            metric_source_data = self.proxysql_select_command_stats
        elif metric_source == MetricSource.proxysql_total_command_stats:
            metric_source_data = self.proxysql_total_command_stats
        else:
            metric_source_data = None

        return metric_source_data

    def update_metrics_per_second_values(self):
        for metric_instance in self.metrics.__dict__.values():
            # Skip if the metric instance is not for the current connection source
            if (
                self.connection_source == ConnectionSource.mysql
                and ConnectionSource.mysql not in metric_instance.connection_source
            ):
                continue
            elif (
                self.connection_source == ConnectionSource.proxysql
                and ConnectionSource.proxysql not in metric_instance.connection_source
            ):
                continue

            metric_source_data = self.get_metric_source_data(metric_instance.metric_source)
            if metric_source_data is None:
                continue  # Skip if there's no metric source

            for metric_name, metric_data in metric_instance.__dict__.items():
                if isinstance(metric_data, MetricData):
                    current_metric_source_value = metric_source_data.get(metric_name, 0)

                    if metric_data.last_value is None:
                        metric_data.last_value = current_metric_source_value
                    else:
                        if metric_data.per_second_calculation:
                            metric_diff = current_metric_source_value - metric_data.last_value
                            metric_status_per_sec = round(metric_diff / self.polling_latency)
                        else:
                            metric_status_per_sec = current_metric_source_value

                        self.add_metric(metric_data, metric_status_per_sec)

    def update_metrics_last_value(self):
        # We set the last value for specific metrics that need it so they can get per second values
        for metric_instance in self.metrics.__dict__.values():
            metric_source_data = self.get_metric_source_data(metric_instance.metric_source)

            for metric_name, metric_data in metric_instance.__dict__.items():
                if isinstance(metric_data, MetricData) and metric_data.per_second_calculation:
                    metric_data.last_value = metric_source_data.get(metric_name, 0)

    def update_proxysql_command_stats(self, proxysql_command_stats):
        if self.connection_source != ConnectionSource.proxysql:
            return

        for row in proxysql_command_stats:
            # Convert all values to integers if they are a number for SELECT command
            if row["Command"] == "SELECT":
                self.proxysql_select_command_stats = {
                    key: int(value) if value.isdigit() else value for key, value in row.items()
                }

            for key, value in row.items():
                if key.startswith("cnt_") and value.isdigit():
                    # If the key exists, add to it; otherwise, initialize with the integer value
                    if key in self.proxysql_total_command_stats:
                        self.proxysql_total_command_stats[key] += int(value)
                    else:
                        self.proxysql_total_command_stats[key] = int(value)

    def update_metrics_replication_lag(self):
        metric_instance = self.metrics.replication_lag
        self.add_metric(metric_instance.lag, self.replication_status.get("Seconds_Behind", 0))

    def update_metrics_adaptive_hash_index_hit_ratio(self):
        hit_ratio = self.get_metric_adaptive_hash_index(format=False)

        if hit_ratio:
            metric_instance = self.metrics.adaptive_hash_index_hit_ratio
            self.add_metric(metric_instance.hit_ratio, hit_ratio)

    def update_metrics_checkpoint(self):
        (max_checkpoint_age_bytes, checkpoint_age_sync_flush_bytes, _) = self.get_metric_checkpoint_age(format=False)

        metric_instance = self.metrics.checkpoint
        metric_instance.checkpoint_age_max = max_checkpoint_age_bytes
        metric_instance.checkpoint_age_sync_flush = checkpoint_age_sync_flush_bytes

    def update_metrics_locks(self):
        metric_instance = self.metrics.locks
        self.add_metric(metric_instance.metadata_lock_count, len(self.metadata_lock_metrics))

    def get_metric_checkpoint_age(self, format):
        checkpoint_age_bytes = round(self.global_status.get("Innodb_checkpoint_age", 0))
        max_checkpoint_age_bytes = self.redo_log_size

        if checkpoint_age_bytes == 0:
            if format:
                return "N/A"
            else:
                return self.redo_log_size, 0, 0

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
            smoothed_hit_ratio = None

            # If we are replaying a file, we need to get the last value from the metric
            if self.replay_file:
                if self.metrics.adaptive_hash_index_hit_ratio.hit_ratio.values:
                    smoothed_hit_ratio = self.metrics.adaptive_hash_index_hit_ratio.hit_ratio.values[-1]
            else:
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

        if not self.metrics.adaptive_hash_index.adaptive_hash_searches.last_value:
            return None

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

    def daemon_cleanup_data(self):
        """
        Cleanup data for daemon mode to keep the metrics data small
        """
        if not self.daemon_mode:
            return

        # Define the time threshold for retaining metrics data
        time_threshold = datetime.now() - timedelta(minutes=10)

        # Filter
        filtered_datetimes = [
            dt for dt in self.datetimes if datetime.strptime(dt, "%d/%m/%y %H:%M:%S") >= time_threshold
        ]

        # Create a set for fast lookup of datetimes
        filtered_set = set(filtered_datetimes)

        # Update metrics data based on datetimes
        for metric_instance in self.metrics.__dict__.values():
            for metric_data in metric_instance.__dict__.values():
                if isinstance(metric_data, MetricData):
                    metric_data.values = [
                        value for dt, value in zip(self.datetimes, metric_data.values) if dt in filtered_set
                    ]

        # Update datetimes to only keep valid ones
        self.datetimes = filtered_datetimes
