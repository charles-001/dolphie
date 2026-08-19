from __future__ import annotations

from dataclasses import dataclass, fields
from enum import Enum
from typing import get_type_hints

from dolphie.DataTypes import ConnectionSource, ConnectionSourceType
from dolphie.Modules.MetricDefinitions import (
    COMMAND_STAT_BUCKETS,
    MetricData,
    MetricInstance,
    MetricInstances,
    create_metric_instances,
    iter_metric_data,
    iter_metric_instances,
)


class GraphRenderer(Enum):
    """Rendering strategies supported by metric graphs."""

    LINE = "line"
    CHECKPOINT = "checkpoint"
    REDO_LOG_LINE = "redo_log_line"
    REDO_LOG_BAR = "redo_log_bar"
    ACTIVE_REDO_LOG = "active_redo_log"
    SYSTEM_MEMORY = "system_memory"


class SwatchKind(Enum):
    """Legend swatches that describe a rendered series."""

    LINE = "line"
    BAR = "bar"


class GraphAvailability(Enum):
    """Runtime predicates for optional graphs."""

    ALWAYS = "always"
    ACTIVE_REDO_LOG = "active_redo_log"


class TabAvailability(Enum):
    """Runtime predicates for optional graph tabs."""

    ALWAYS = "always"
    SYSTEM_UTILIZATION = "system_utilization"
    ADAPTIVE_HASH_INDEX = "adaptive_hash_index"
    REPLICATION = "replication"
    LOCKS = "locks"


@dataclass(frozen=True, order=True)
class MetricKey:
    """Stable typed reference to one metric series."""

    group: str
    metric: str

    @property
    def dom_id(self) -> str:
        """Return the stable DOM-safe identifier for this metric."""
        return f"{self.group}-{self.metric}"


def resolve_metric_data(metric_instance: MetricInstance, metric_key: MetricKey) -> MetricData:
    """Resolve a registry metric key against its typed metric group instance."""
    metric_data = getattr(metric_instance, metric_key.metric)
    if not isinstance(metric_data, MetricData):
        raise TypeError(f"{metric_key.dom_id} does not resolve to MetricData")
    return metric_data


@dataclass(frozen=True)
class GraphSpec:
    """Declare one graph and the series it renders."""

    id: str
    metric_group: str
    series: tuple[MetricKey, ...]
    renderer: GraphRenderer = GraphRenderer.LINE
    weight: int = 1
    expanded_weight: int | None = None
    availability: GraphAvailability = GraphAvailability.ALWAYS
    control_label: str | None = None


@dataclass(frozen=True)
class GraphRowSpec:
    """Declare one horizontal row of graphs."""

    graphs: tuple[GraphSpec, ...]


@dataclass(frozen=True)
class GraphTabSpec:
    """Declare one metric graph tab and its layout."""

    id: str
    title: str
    connection_sources: frozenset[ConnectionSourceType]
    rows: tuple[GraphRowSpec, ...]
    availability: TabAvailability = TabAvailability.ALWAYS

    @property
    def graphs(self) -> tuple[GraphSpec, ...]:
        """Return all graphs in display order."""
        return tuple(graph for row in self.rows for graph in row.graphs)

    @property
    def metric_groups(self) -> tuple[str, ...]:
        """Return metric groups in first-use order."""
        return tuple(dict.fromkeys(graph.metric_group for graph in self.graphs))

    @property
    def unique_series_by_graph(self) -> tuple[tuple[GraphSpec, tuple[MetricKey, ...]], ...]:
        """Return each graph's series with first-use dedupe across the tab."""
        seen: set[MetricKey] = set()
        deduped: list[tuple[GraphSpec, tuple[MetricKey, ...]]] = []
        for graph in self.graphs:
            fresh = tuple(metric for metric in graph.series if metric not in seen)
            seen.update(fresh)
            deduped.append((graph, fresh))
        return tuple(deduped)


MYSQL = frozenset[ConnectionSourceType]((ConnectionSource.mysql,))
PROXYSQL = frozenset[ConnectionSourceType]((ConnectionSource.proxysql,))
BOTH = frozenset[ConnectionSourceType]((ConnectionSource.mysql, ConnectionSource.proxysql))


def _key(group: str, metric: str) -> MetricKey:
    return MetricKey(group, metric)


GRAPH_TABS = (
    GraphTabSpec(
        id="system",
        title="System",
        connection_sources=BOTH,
        availability=TabAvailability.SYSTEM_UTILIZATION,
        rows=(
            GraphRowSpec(
                (
                    GraphSpec(
                        "graph_system_cpu",
                        "system_cpu",
                        (_key("system_cpu", "CPU_Percent"),),
                        control_label="CPU",
                    ),
                    GraphSpec(
                        "graph_system_memory",
                        "system_memory",
                        (_key("system_memory", "Memory_Used"),),
                        renderer=GraphRenderer.SYSTEM_MEMORY,
                        control_label="Memory",
                    ),
                )
            ),
            GraphRowSpec(
                (
                    GraphSpec(
                        "graph_system_disk_io",
                        "system_disk_io",
                        (
                            _key("system_disk_io", "Disk_Read"),
                            _key("system_disk_io", "Disk_Write"),
                        ),
                        control_label="Disk",
                    ),
                    GraphSpec(
                        "graph_system_network",
                        "system_network",
                        (
                            _key("system_network", "Network_Down"),
                            _key("system_network", "Network_Up"),
                        ),
                        control_label="Network",
                    ),
                )
            ),
        ),
    ),
    GraphTabSpec(
        id="dml",
        title="DML",
        connection_sources=BOTH,
        rows=(
            GraphRowSpec(
                (
                    GraphSpec(
                        "graph_dml",
                        "dml",
                        (
                            _key("dml", "Queries"),
                            _key("dml", "Com_select"),
                            _key("dml", "Com_insert"),
                            _key("dml", "Com_update"),
                            _key("dml", "Com_delete"),
                        ),
                    ),
                )
            ),
        ),
    ),
    GraphTabSpec(
        id="buffer_pool_requests",
        title="BP Requests",
        connection_sources=MYSQL,
        rows=(
            GraphRowSpec(
                (
                    GraphSpec(
                        "graph_buffer_pool_requests",
                        "buffer_pool_requests",
                        (
                            _key("buffer_pool_requests", "Innodb_buffer_pool_read_requests"),
                            _key("buffer_pool_requests", "Innodb_buffer_pool_write_requests"),
                            _key("buffer_pool_requests", "Innodb_buffer_pool_reads"),
                        ),
                    ),
                )
            ),
        ),
    ),
    GraphTabSpec(
        id="history_list_length",
        title="History List",
        connection_sources=MYSQL,
        rows=(
            GraphRowSpec(
                (
                    GraphSpec(
                        "graph_history_list_length",
                        "history_list_length",
                        (_key("history_list_length", "trx_rseg_history_len"),),
                    ),
                )
            ),
        ),
    ),
    GraphTabSpec(
        id="adaptive_hash_index",
        title="AHI",
        connection_sources=MYSQL,
        availability=TabAvailability.ADAPTIVE_HASH_INDEX,
        rows=(
            GraphRowSpec(
                (
                    GraphSpec(
                        "graph_adaptive_hash_index",
                        "adaptive_hash_index",
                        (
                            _key("adaptive_hash_index", "adaptive_hash_searches"),
                            _key("adaptive_hash_index", "adaptive_hash_searches_btree"),
                        ),
                        control_label="Searches",
                    ),
                    GraphSpec(
                        "graph_adaptive_hash_index_hit_ratio",
                        "adaptive_hash_index_hit_ratio",
                        (_key("adaptive_hash_index_hit_ratio", "hit_ratio"),),
                        control_label="Ratio",
                    ),
                )
            ),
        ),
    ),
    GraphTabSpec(
        id="checkpoint",
        title="Checkpoint",
        connection_sources=MYSQL,
        rows=(
            GraphRowSpec(
                (
                    GraphSpec(
                        "graph_checkpoint",
                        "checkpoint",
                        (_key("checkpoint", "Innodb_checkpoint_age"),),
                        renderer=GraphRenderer.CHECKPOINT,
                    ),
                )
            ),
        ),
    ),
    GraphTabSpec(
        id="redo_log",
        title="Redo Log",
        connection_sources=MYSQL,
        rows=(
            GraphRowSpec(
                (
                    GraphSpec(
                        "graph_redo_log_active_count",
                        "redo_log_active_count",
                        (_key("redo_log_active_count", "Active_redo_log_count"),),
                        renderer=GraphRenderer.ACTIVE_REDO_LOG,
                        weight=33,
                        availability=GraphAvailability.ACTIVE_REDO_LOG,
                        control_label="Active Logs",
                    ),
                    GraphSpec(
                        "graph_redo_log_data_written",
                        "redo_log",
                        (_key("redo_log", "Innodb_lsn_current"),),
                        renderer=GraphRenderer.REDO_LOG_LINE,
                        weight=55,
                        expanded_weight=88,
                        control_label="Written",
                    ),
                    GraphSpec(
                        "graph_redo_log_bar",
                        "redo_log",
                        (_key("redo_log", "Innodb_lsn_current"),),
                        renderer=GraphRenderer.REDO_LOG_BAR,
                        weight=12,
                        control_label="Hourly",
                    ),
                )
            ),
        ),
    ),
    GraphTabSpec(
        id="table_cache",
        title="Table Cache",
        connection_sources=MYSQL,
        rows=(
            GraphRowSpec(
                (
                    GraphSpec(
                        "graph_table_cache",
                        "table_cache",
                        (
                            _key("table_cache", "Table_open_cache_hits"),
                            _key("table_cache", "Table_open_cache_misses"),
                            _key("table_cache", "Table_open_cache_overflows"),
                        ),
                    ),
                )
            ),
        ),
    ),
    GraphTabSpec(
        id="threads",
        title="Threads",
        connection_sources=MYSQL,
        rows=(
            GraphRowSpec(
                (
                    GraphSpec(
                        "graph_threads",
                        "threads",
                        (
                            _key("threads", "Threads_connected"),
                            _key("threads", "Threads_running"),
                        ),
                    ),
                )
            ),
        ),
    ),
    GraphTabSpec(
        id="temporary_objects",
        title="Temp Objects",
        connection_sources=MYSQL,
        rows=(
            GraphRowSpec(
                (
                    GraphSpec(
                        "graph_temporary_objects",
                        "temporary_objects",
                        (
                            _key("temporary_objects", "Created_tmp_tables"),
                            _key("temporary_objects", "Created_tmp_disk_tables"),
                            _key("temporary_objects", "Created_tmp_files"),
                        ),
                    ),
                )
            ),
        ),
    ),
    GraphTabSpec(
        id="aborted_connections",
        title="Aborted Connections",
        connection_sources=MYSQL,
        rows=(
            GraphRowSpec(
                (
                    GraphSpec(
                        "graph_aborted_connections",
                        "aborted_connections",
                        (
                            _key("aborted_connections", "Aborted_clients"),
                            _key("aborted_connections", "Aborted_connects"),
                        ),
                    ),
                )
            ),
        ),
    ),
    GraphTabSpec(
        id="disk_io",
        title="Disk I/O",
        connection_sources=MYSQL,
        rows=(
            GraphRowSpec(
                (
                    GraphSpec(
                        "graph_disk_io",
                        "disk_io",
                        (_key("disk_io", "io_read"), _key("disk_io", "io_write")),
                    ),
                )
            ),
        ),
    ),
    GraphTabSpec(
        id="locks",
        title="Locks",
        connection_sources=MYSQL,
        availability=TabAvailability.LOCKS,
        rows=(
            GraphRowSpec(
                (
                    GraphSpec(
                        "graph_locks",
                        "locks",
                        (_key("locks", "metadata_lock_count"),),
                    ),
                )
            ),
        ),
    ),
    GraphTabSpec(
        id="replication_lag",
        title="Replication",
        connection_sources=MYSQL,
        availability=TabAvailability.REPLICATION,
        rows=(
            GraphRowSpec(
                (
                    GraphSpec(
                        "graph_replication_lag",
                        "replication_lag",
                        (_key("replication_lag", "lag"),),
                    ),
                )
            ),
        ),
    ),
    GraphTabSpec(
        id="proxysql_active_trx",
        title="Active TRX",
        connection_sources=PROXYSQL,
        rows=(
            GraphRowSpec(
                (
                    GraphSpec(
                        "graph_proxysql_active_trx",
                        "proxysql_active_trx",
                        (_key("proxysql_active_trx", "Active_Transactions"),),
                    ),
                )
            ),
        ),
    ),
    GraphTabSpec(
        id="proxysql_multiplex_efficiency",
        title="Multiplex Efficiency",
        connection_sources=PROXYSQL,
        rows=(
            GraphRowSpec(
                (
                    GraphSpec(
                        "graph_proxysql_multiplex_efficiency",
                        "proxysql_multiplex_efficiency",
                        (_key("proxysql_multiplex_efficiency", "proxysql_multiplex_efficiency_ratio"),),
                    ),
                )
            ),
        ),
    ),
    GraphTabSpec(
        id="proxysql_connections",
        title="Connections",
        connection_sources=PROXYSQL,
        rows=(
            GraphRowSpec(
                (
                    GraphSpec(
                        "graph_proxysql_connections",
                        "proxysql_connections",
                        (
                            _key("proxysql_connections", "Client_Connections_non_idle"),
                            _key("proxysql_connections", "Client_Connections_aborted"),
                            _key("proxysql_connections", "Client_Connections_connected"),
                            _key("proxysql_connections", "Client_Connections_created"),
                            _key("proxysql_connections", "Server_Connections_aborted"),
                            _key("proxysql_connections", "Server_Connections_connected"),
                            _key("proxysql_connections", "Server_Connections_created"),
                            _key("proxysql_connections", "Access_Denied_Wrong_Password"),
                        ),
                    ),
                )
            ),
        ),
    ),
    GraphTabSpec(
        id="proxysql_queries_data_network",
        title="Query Data Rates",
        connection_sources=PROXYSQL,
        rows=(
            GraphRowSpec(
                (
                    GraphSpec(
                        "graph_proxysql_queries_data_network",
                        "proxysql_queries_data_network",
                        (
                            _key("proxysql_queries_data_network", "Queries_backends_bytes_recv"),
                            _key("proxysql_queries_data_network", "Queries_backends_bytes_sent"),
                            _key("proxysql_queries_data_network", "Queries_frontends_bytes_recv"),
                            _key("proxysql_queries_data_network", "Queries_frontends_bytes_sent"),
                        ),
                    ),
                )
            ),
        ),
    ),
    GraphTabSpec(
        id="proxysql_select_command_stats",
        title="SELECT Command Stats",
        connection_sources=PROXYSQL,
        rows=(
            GraphRowSpec(
                (
                    GraphSpec(
                        "graph_proxysql_select_command_stats",
                        "proxysql_select_command_stats",
                        tuple(_key("proxysql_select_command_stats", metric) for metric in COMMAND_STAT_BUCKETS),
                    ),
                )
            ),
        ),
    ),
    GraphTabSpec(
        id="proxysql_total_command_stats",
        title="Total Command Stats",
        connection_sources=PROXYSQL,
        rows=(
            GraphRowSpec(
                (
                    GraphSpec(
                        "graph_proxysql_total_command_stats",
                        "proxysql_total_command_stats",
                        tuple(_key("proxysql_total_command_stats", metric) for metric in COMMAND_STAT_BUCKETS),
                    ),
                )
            ),
        ),
    ),
)

GRAPH_TABS_BY_ID = {tab.id: tab for tab in GRAPH_TABS}
GRAPHS_BY_ID = {graph.id: graph for tab in GRAPH_TABS for graph in tab.graphs}
_RENDERER_METRIC_GROUPS = {
    GraphRenderer.CHECKPOINT: "checkpoint",
    GraphRenderer.REDO_LOG_LINE: "redo_log",
    GraphRenderer.REDO_LOG_BAR: "redo_log",
    GraphRenderer.ACTIVE_REDO_LOG: "redo_log_active_count",
    GraphRenderer.SYSTEM_MEMORY: "system_memory",
}


def swatch_for_metric(tab: GraphTabSpec, metric: MetricKey) -> SwatchKind:
    """Return the strongest graph encoding used by a metric."""
    renderers = {graph.renderer for graph in tab.graphs if metric in graph.series}
    if renderers == {GraphRenderer.REDO_LOG_BAR}:
        return SwatchKind.BAR
    return SwatchKind.LINE


def validate_graph_definitions() -> None:
    """Validate graph configuration against typed metric dataclasses."""
    tab_ids: set[str] = set()
    graph_ids: set[str] = set()
    assigned_metrics: set[MetricKey] = set()
    metric_types = get_type_hints(MetricInstances)

    for tab in GRAPH_TABS:
        if tab.id in tab_ids:
            raise ValueError(f"Duplicate graph tab ID: {tab.id}")
        tab_ids.add(tab.id)
        if not tab.rows:
            raise ValueError(f"Graph tab {tab.id} has no rows")

        for row in tab.rows:
            if not row.graphs:
                raise ValueError(f"Graph tab {tab.id} has an empty row")
            for graph in row.graphs:
                if graph.id in graph_ids:
                    raise ValueError(f"Duplicate graph ID: {graph.id}")
                graph_ids.add(graph.id)
                if graph.weight <= 0 or (graph.expanded_weight is not None and graph.expanded_weight <= 0):
                    raise ValueError(f"Graph {graph.id} has an invalid weight")
                if graph.metric_group not in metric_types:
                    raise ValueError(f"Unknown metric group {graph.metric_group} for graph {graph.id}")
                if not graph.series:
                    raise ValueError(f"Graph {graph.id} has no series")

                group_type = metric_types[graph.metric_group]
                expected_group = _RENDERER_METRIC_GROUPS.get(graph.renderer)
                if expected_group is not None and graph.metric_group != expected_group:
                    raise ValueError(
                        f"Renderer {graph.renderer.value} requires {expected_group}, not {graph.metric_group}"
                    )
                if not tab.connection_sources.issubset(group_type.connection_source):
                    raise ValueError(f"Graph {graph.id} is unavailable for one of tab {tab.id}'s connection sources")
                metric_fields = {field.name: field.type for field in fields(group_type)}
                for metric in graph.series:
                    if metric.group != graph.metric_group:
                        raise ValueError(f"Graph {graph.id} references series from {metric.group}")
                    if metric.metric not in metric_fields:
                        raise ValueError(f"Unknown metric {metric.dom_id} for graph {graph.id}")
                    if metric_fields[metric.metric] not in (MetricData, "MetricData"):
                        raise ValueError(f"Graph {graph.id} references non-metric field {metric.dom_id}")
                    assigned_metrics.add(metric)

    catalog = create_metric_instances()
    unassigned_metrics = {
        MetricKey(instance_name, metric_name)
        for instance_name, metric_instance in iter_metric_instances(catalog)
        for metric_name, metric_data in iter_metric_data(metric_instance)
        if metric_data.graphable and MetricKey(instance_name, metric_name) not in assigned_metrics
    }
    if unassigned_metrics:
        unassigned = ", ".join(metric.dom_id for metric in sorted(unassigned_metrics))
        raise ValueError(f"Graphable metrics missing from graph registry: {unassigned}")


validate_graph_definitions()
