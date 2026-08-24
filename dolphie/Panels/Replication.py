from __future__ import annotations

import re
import time
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor

from loguru import logger
from textual.containers import ScrollableContainer
from textual.widget import Widget
from textual.widgets import Static

from dolphie.DataTypes import ConnectionSource, DatabaseRow, DatabaseScalar, Replica
from dolphie.Modules.Functions import (
    coerce_float,
    coerce_int,
    coerce_str,
    format_bytes,
    format_number,
    format_picoseconds,
    format_time,
    host_without_port,
)
from dolphie.Modules.ManualException import ManualException
from dolphie.Modules.MySQL import Database
from dolphie.Modules.Queries import MySQLQueries
from dolphie.Modules.TabManager import Tab
from dolphie.Modules.Theme import ThemedTable as Table

# Example GTID: 3beacd96-6fe3-18ec-9d95-b4592zec4b45:1-26
_GTID_PATTERN = re.compile(r"\b(\w+(?:-\w+){4}):(.+)\b")
_ERRANT_CHECK_INTERVAL_SECONDS = 30
_MAX_REPLICA_POLL_WORKERS = 4
_MAX_REPLICA_BACKOFF_SECONDS = 60
# Bounds a stuck socket read/write (e.g. the replica accepted the TCP handshake
# but stopped responding) so it can't hold a slot in the shared poll executor
# forever and starve every other tab's replica polling.
_REPLICA_QUERY_TIMEOUT_SECONDS = 10
_CLUSTERSET_CHANNEL = "clusterset_replication"
_MANAGED_REPLICATION_CHANNELS = frozenset(
    {
        _CLUSTERSET_CHANNEL,
        "group_replication_applier",
        "group_replication_recovery",
    }
)

# Shared across poll cycles (and tabs) so each refresh doesn't pay pool setup
# and teardown; idle workers are reclaimed at interpreter exit. Constructing a
# ThreadPoolExecutor doesn't start any threads until work is submitted, so
# building it eagerly at import time (instead of lazily on first use) avoids
# a double-checked-locking dance for what would otherwise be a check-then-act
# race between concurrently starting tabs (e.g. --hostgroup).
_replica_poll_executor = ThreadPoolExecutor(
    max_workers=_MAX_REPLICA_POLL_WORKERS,
    thread_name_prefix="dolphie-replica",
)


def is_managed_replication_channel(status: Mapping[str, DatabaseScalar]) -> bool:
    """Return whether MySQL owns the channel for a clustered topology."""
    return coerce_str(status.get("Channel_Name")) in _MANAGED_REPLICATION_CHANNELS


def user_replication_channels(statuses: list[DatabaseRow]) -> list[DatabaseRow]:
    """Return channels that should appear in the generic replication panel."""
    return [status for status in statuses if not is_managed_replication_channel(status)]


def replication_channel_priority(status: Mapping[str, DatabaseScalar]) -> tuple[int, float]:
    """Rank unhealthy channels ahead of channels with ordinary numeric lag."""
    io_running = status.get("Replica_IO_Running", status.get("Slave_IO_Running"))
    sql_running = status.get("Replica_SQL_Running", status.get("Slave_SQL_Running"))
    has_error = bool(status.get("Last_IO_Error") or status.get("Last_SQL_Error"))
    if has_error or io_running != "Yes" or sql_running != "Yes":
        return (2, 0)

    lag = status.get("Seconds_Behind")
    if lag is None:
        return (1, 0)
    return (0, coerce_float(lag))


def _filter_gtid_sets(gtid_sets: str, exclude_uuids: set[str]) -> str:
    """Remove GTID lines containing any of the excluded UUIDs."""
    remaining = [
        line.strip().rstrip(",")
        for line in gtid_sets.splitlines()
        if line.strip() and not any(uuid in line for uuid in exclude_uuids)
    ]
    return ",\n".join(remaining)


def _format_clusterset_replication(
    channel: Mapping[str, DatabaseScalar] | None,
    *,
    is_cluster_primary: bool,
) -> tuple[str, str | None]:
    """Format ClusterSet async-channel health for the local cluster member."""
    if not is_cluster_primary:
        return ("[$light_blue]Managed by cluster PRIMARY[/$light_blue]", None)
    if channel is None:
        return ("[$yellow]Unavailable[/$yellow]", None)

    io_running = channel.get("Replica_IO_Running") == "Yes"
    sql_running = channel.get("Replica_SQL_Running") == "Yes"
    threads_color = "$green" if io_running and sql_running else "$red"
    lag = _replica_lag_value(channel, "Seconds_Behind_Source")
    lag_display = format_time(lag) if lag is not None else "Unknown"
    status = (
        f"[{threads_color}]IO {'ON' if io_running else 'OFF'}"
        f"  SQL {'ON' if sql_running else 'OFF'}[/{threads_color}]"
        f"  [$label]Lag[/$label] {lag_display}"
    )
    error = coerce_str(channel.get("Last_IO_Error") or channel.get("Last_SQL_Error")) or None
    return (status, error)


def _color_gtid_sets(gtid_sets: str, primary_uuid: str) -> str:
    """Apply Rich markup to GTID sets, highlighting the primary UUID."""

    def _colorize(match):
        source_id = match.group(1)
        transaction_id = match.group(2)

        if source_id == primary_uuid:
            return f"[$highlight]{source_id}[/$highlight]:{transaction_id}"
        return f"[$dark_gray]{source_id}:{transaction_id}[/$dark_gray]"

    return _GTID_PATTERN.sub(_colorize, gtid_sets.replace(",", ""))


# MariaDB GTID format: domain_id-server_id-sequence_number (e.g., 0-1-10)
def _color_mariadb_gtid_sets(gtid_sets: str, primary_server_id) -> str:
    """Apply Rich markup to MariaDB GTID sets, highlighting the primary server_id."""
    primary_server_id = str(primary_server_id)
    colored = []
    for gtid in gtid_sets.split(","):
        gtid = gtid.strip()
        if not gtid:
            continue
        parts = gtid.split("-")
        if len(parts) >= 3 and parts[1] == primary_server_id:
            colored.append(f"[$highlight]{gtid}[/$highlight]")
        else:
            colored.append(f"[$dark_gray]{gtid}[/$dark_gray]")
    return "\n".join(colored)


def _detect_mariadb_errant_trx(replica_gtid_current_pos: str, replica_server_id, primary_gtid_current_pos: str):
    """Detect errant transactions on a MariaDB replica.

    Sequence ordering belongs to the replication domain, so a failover may change
    the originating server ID without making an otherwise covered GTID errant.

    Returns a comma-separated string of errant GTIDs, or None if clean.
    """
    replica_server_id = str(replica_server_id)

    def _parse_gtids(gtid_str: str) -> list[tuple[str, str, int]]:
        result = []
        for gtid in gtid_str.split(","):
            gtid = gtid.strip()
            if not gtid:
                continue
            parts = gtid.split("-")
            if len(parts) >= 3:
                try:
                    result.append((parts[0], parts[1], int(parts[2])))
                except ValueError:
                    continue
        return result

    replica_gtids = _parse_gtids(replica_gtid_current_pos)
    primary_gtids = _parse_gtids(primary_gtid_current_pos)
    primary_sequence_by_domain: dict[str, int] = {}
    for domain_id, _, sequence in primary_gtids:
        primary_sequence_by_domain[domain_id] = max(
            primary_sequence_by_domain.get(domain_id, 0),
            sequence,
        )

    errant = []
    for domain_id, server_id, sequence in replica_gtids:
        if server_id != replica_server_id:
            continue
        if sequence > primary_sequence_by_domain.get(domain_id, 0):
            errant.append(f"{domain_id}-{server_id}-{sequence}")

    return ",".join(errant) if errant else None


def _sync_grid(
    grid,
    items: dict[str, Table],
    item_type: str,
    tab_id: str,
    app,
    tracked: dict[str, Static],
    *,
    highlighted_keys: set[str] | None = None,
):
    """Synchronize grid child widgets with the current set of items.

    Uses the caller-owned ``tracked`` dict to avoid DOM queries. The dict maps
    item keys to their mounted Static widgets and is updated in place.
    """
    current_keys = set(items.keys())
    highlighted_keys = highlighted_keys or set()

    # Update existing or mount new
    for key, table in items.items():
        if key in tracked:
            tracked[key].update(table)
            parent = tracked[key].parent
            if isinstance(parent, Widget):
                parent.set_class(key in highlighted_keys, "local_node")
        else:
            try:
                static = Static(table, id=f"{item_type}_{key}_{tab_id}")
                grid.mount(
                    ScrollableContainer(
                        static,
                        id=f"{item_type}_container_{key}_{tab_id}",
                        classes="local_node" if key in highlighted_keys else None,
                    )
                )
                tracked[key] = static
            except Exception:
                app.notify(
                    f"Failed to mount {item_type} [$highlight]{key}",
                    severity="error",
                )

    # Remove stale widgets
    for key in set(tracked.keys()) - current_keys:
        widget = tracked[key]
        if isinstance(widget.parent, Widget):
            widget.parent.remove()
        else:
            widget.remove()
        del tracked[key]


def create_panel(tab: Tab) -> None:
    dolphie = tab.dolphie

    global_variables = dolphie.global_variables
    group_replication_data = dolphie.group_replication_data
    connection_source_alt = dolphie.connection_source_alt
    panels = dolphie.panels

    # --- Replication panel ---
    replication_statuses = user_replication_channels(dolphie.replication_status)
    replication_applier_status = {
        channel: status
        for channel, status in dolphie.replication_applier_status.items()
        if channel not in _MANAGED_REPLICATION_CHANNELS
    }
    if not replication_statuses:
        tab.replication_container.display = False
        tab.replication_thread_applier_container.display = False
        tab.replication_status_grid.display = False
        _sync_grid(tab.replication_status_grid, {}, "replication_channel", tab.id, dolphie.app, tab.channel_widgets)
    else:
        tab.replication_container.display = True

        if replication_applier_status:
            is_multi_channel = len(replication_applier_status) > 1
            table_thread_applier_status = Table(box=None, header_style="label")
            if is_multi_channel:
                table_thread_applier_status.add_column("Channel")
            table_thread_applier_status.add_column("Worker", justify="center")
            table_thread_applier_status.add_column("Usage", min_width=6)
            table_thread_applier_status.add_column("Apply Time")
            table_thread_applier_status.add_column("Last Applied Transaction")
            table_thread_applier_status.add_column("Retries")
            table_thread_applier_status.add_column("Error Time")
            table_thread_applier_status.add_column("Error Message", overflow="fold")

            for channel_name, raw_channel_data in replication_applier_status.items():
                if not isinstance(raw_channel_data, dict):
                    continue

                all_workers_diff = coerce_float(raw_channel_data.get("diff_all"))
                raw_rows = raw_channel_data.get("data")
                if not isinstance(raw_rows, list):
                    continue

                for raw_row in raw_rows:
                    if not isinstance(raw_row, dict):
                        continue
                    row = raw_row
                    worker_id = row.get("worker_id")
                    thread_id = row.get("thread_id")

                    # Calculate the difference in thread events for this worker
                    worker_diff = coerce_float(raw_channel_data.get(f"diff_{thread_id}"))

                    # Format the last applied transaction
                    last_applied_transaction = coerce_str(row.get("last_applied_transaction"), "N/A")
                    transaction_parts = last_applied_transaction.split(":")
                    uuid_parts = transaction_parts[0].split("-")
                    if len(transaction_parts) > 1 and len(uuid_parts) > 4:
                        source_id_split = uuid_parts[4]
                        transaction_id = transaction_parts[1]
                        last_applied_transaction = f"…[$dark_gray]{source_id_split}[/$dark_gray]:{transaction_id}"

                    # Format the last error time
                    last_error_time = row.get("applying_transaction_last_transient_error_timestamp", "N/A")
                    last_error_time = "" if last_error_time == "0000-00-00 00:00:00.000000" else str(last_error_time)

                    # Calculate the usage percentage for each worker for the current poll
                    usage_percentage = round(100 * (worker_diff / all_workers_diff), 2) if all_workers_diff > 0 else 0.0
                    retries_count = row.get("applying_transaction_retries_count", 0)
                    retries_count = f"[$dark_gray]{retries_count}" if retries_count == 0 else f"[$red]{retries_count}"

                    # Build the row values
                    row_values = []
                    if is_multi_channel:
                        row_values.append(f"[$highlight]{channel_name}[/$highlight]")
                    row_values.extend(
                        [
                            f"[$b_highlight]{worker_id}[/$b_highlight]: {thread_id}",
                            f"{usage_percentage}%",
                            format_picoseconds(coerce_float(row.get("apply_time"))),
                            last_applied_transaction,
                            retries_count,
                            last_error_time,
                            coerce_str(row.get("applying_transaction_last_transient_error_message"), "N/A"),
                        ]
                    )
                    table_thread_applier_status.add_row(*row_values)

                if is_multi_channel:
                    table_thread_applier_status.add_section()

            tab.replication_thread_applier.update(table_thread_applier_status)
            tab.replication_thread_applier_container.display = True
        else:
            tab.replication_thread_applier_container.display = False

        if connection_source_alt == ConnectionSource.mariadb:
            available_replication_variables = {
                "slave_parallel_mode": "parallel_mode",
                "slave_parallel_workers": "parallel_workers",
                "slave_parallel_threads": "parallel_threads",
                "log_slave_updates": "log_slave_updates",
            }
        elif dolphie.is_mysql_version_at_least("8.0.22"):
            available_replication_variables = {
                "replica_parallel_type": "parallel_type",
                "replica_parallel_workers": "parallel_workers",
                "replica_preserve_commit_order": "preserve_commit_order",
                "log_replica_updates": "log_replica_updates",
            }
        else:
            available_replication_variables = {
                "slave_parallel_type": "parallel_type",
                "slave_parallel_workers": "parallel_workers",
                "slave_preserve_commit_order": "preserve_commit_order",
                "log_slave_updates": "log_slave_updates",
            }

        replication_variables = "  ".join(
            f"[$label]{display_name}[/$label] {global_variables.get(var, 'N/A')}"
            for var, display_name in available_replication_variables.items()
        )

        tab.replication_variables.update(replication_variables)

        # Multi-source replication: render a table per channel using the grid
        is_multi_source = len(replication_statuses) > 1

        if is_multi_source:
            tab.replication_status_grid.display = True
            single_parent = tab.replication_status_single.parent
            if isinstance(single_parent, ScrollableContainer):
                single_parent.display = False
            tab.replication_status_grid.set_class(True, "multi_source")
            items = {}
            for channel in replication_statuses:
                channel_name = channel.get("Channel_Name", "")
                channel_key = channel_name or "_default"
                items[channel_key] = create_replication_table(tab, channel_data=channel, show_channel_name=True)
            _sync_grid(
                tab.replication_status_grid, items, "replication_channel", tab.id, dolphie.app, tab.channel_widgets
            )
        else:
            tab.replication_status_grid.display = False
            _sync_grid(
                tab.replication_status_grid,
                {},
                "replication_channel",
                tab.id,
                dolphie.app,
                tab.channel_widgets,
            )
            single_parent = tab.replication_status_single.parent
            if isinstance(single_parent, ScrollableContainer):
                single_parent.display = True
            tab.replication_status_single.update(create_replication_table(tab, channel_data=replication_statuses[0]))

    # --- Group Replication panel ---
    if not (dolphie.group_replication or dolphie.innodb_cluster):
        tab.group_replication_container.display = False
        _sync_grid(tab.group_replication_grid, {}, "member", tab.id, dolphie.app, tab.member_widgets)
    else:
        tab.group_replication_container.display = True

        available_variables = {
            "group_replication_single_primary_mode": ("Single Primary", global_variables),
            "group_replication_consistency": ("Global Consistency", global_variables),
            "write_concurrency": ("Write Concurrency", group_replication_data),
        }

        group_replication_variables = "  ".join(
            f"[$label]{label}[/$label] {source.get(var, 'N/A')}" for var, (label, source) in available_variables.items()
        )
        member_count = len(dolphie.group_replication_members)
        online_member_count = sum(row.get("MEMBER_STATE") == "ONLINE" for row in dolphie.group_replication_members)
        online_color = "$green" if member_count and online_member_count == member_count else "$red"
        group_replication_variables += (
            f"  [$label]Online Members[/$label] [{online_color}]{online_member_count}/{member_count}[/{online_color}]"
        )

        title_prefix = panels.replication.content_key
        cluster_title = "InnoDB Cluster" if dolphie.innodb_cluster else "Group Replication"
        cluster_name = group_replication_data.get("cluster_name")
        final_cluster_name = (
            cluster_name if cluster_name else global_variables.get("group_replication_group_name", "N/A")
        )
        tab.group_replication_title.update(
            f"[b]{title_prefix}{cluster_title} ([$highlight]{final_cluster_name}[/$highlight])"
        )
        tab.group_replication_data.update(group_replication_variables)

        items = create_group_replication_member_table(tab)
        _sync_grid(
            tab.group_replication_grid,
            items,
            "member",
            tab.id,
            dolphie.app,
            tab.member_widgets,
            highlighted_keys={coerce_str(dolphie.server_uuid)},
        )

    # --- Galera Cluster panel ---
    if not dolphie.galera_cluster:
        tab.galera_container.display = False
        _sync_grid(tab.galera_grid, {}, "galera_node", tab.id, dolphie.app, tab.galera_widgets)
    else:
        tab.galera_container.display = True

        gcache_size = global_variables.get("wsrep_provider_gcache_size")
        gcache_formatted = format_bytes(int(gcache_size)) if gcache_size else "N/A"

        galera_variables = (
            f"[$label]Cluster Name[/$label] {global_variables.get('wsrep_cluster_name', 'N/A')}"
            f"  [$label]SST Method[/$label] {global_variables.get('wsrep_sst_method', 'N/A')}"
            f"  [$label]OSU Method[/$label] {global_variables.get('wsrep_osu_method', 'N/A')}"
            f"  [$label]GCache Size[/$label] {gcache_formatted}"
            f"  [$label]Apply Threads[/$label] {global_variables.get('wsrep_slave_threads', 'N/A')}"
            f"  [$label]Sync Wait[/$label] {global_variables.get('wsrep_sync_wait', 'N/A')}"
        )

        title_prefix = panels.replication.content_key
        component_status = coerce_str(dolphie.global_status.get("wsrep_cluster_status"), "N/A")
        component_color = "$green" if component_status == "Primary" else "$red"
        tab.galera_title.update(
            f"[b]{title_prefix}Galera Cluster"
            f" ([$highlight]{dolphie.global_status.get('wsrep_cluster_size', 'N/A')} nodes[/$highlight],"
            f" [{component_color}]{component_status}[/{component_color}])"
        )
        tab.galera_data.update(galera_variables)

        items = create_galera_node_table(tab)
        _sync_grid(
            tab.galera_grid,
            items,
            "galera_node",
            tab.id,
            dolphie.app,
            tab.galera_widgets,
            highlighted_keys={coerce_str(dolphie.global_status.get("wsrep_gcomm_uuid"))},
        )

    # --- ClusterSet panel ---
    clusterset_instances = dolphie.clusterset_instances
    if not clusterset_instances:
        tab.clusterset_container.display = False
        _sync_grid(tab.clusterset_grid, {}, "clusterset", tab.id, dolphie.app, tab.clusterset_widgets)
    else:
        tab.clusterset_container.display = True

        host_cluster_name = group_replication_data.get("cluster_name")
        clusterset_channel = next(
            (
                status
                for status in dolphie.replication_status
                if coerce_str(status.get("Channel_Name")) == _CLUSTERSET_CHANNEL
            ),
            None,
        )

        # Build per-cluster data from the combined query
        cluster_members: dict[str, list[str]] = {}
        cluster_meta: dict[str, dict] = {}
        for inst in clusterset_instances:
            cname = coerce_str(inst.get("cluster_name"))
            cluster_members.setdefault(cname, []).append(coerce_str(inst.get("address"), "N/A"))
            if cname not in cluster_meta:
                cluster_meta[cname] = {
                    "clusterset_name": coerce_str(inst.get("clusterset_name"), "N/A"),
                    "cluster_role": coerce_str(inst.get("cluster_role"), "N/A"),
                    "invalidated": inst.get("invalidated", 0),
                }

        tab.clusterset_title.update(
            f"[b]{panels.replication.content_key}ClusterSet ([$highlight]{len(cluster_meta)}[/$highlight] clusters)"
        )

        items = {}
        highlighted_clusters: set[str] = set()
        for cname, meta in cluster_meta.items():
            clusterset_name = meta["clusterset_name"]
            cluster_role = meta["cluster_role"]
            invalidated = meta["invalidated"]
            members = cluster_members.get(cname, [])
            is_local = cname == host_cluster_name

            cluster_role_fmt = (
                f"[b][$highlight]{cluster_role}[/$highlight]"
                if cluster_role == "PRIMARY"
                else f"[$dark_gray]{cluster_role}[/$dark_gray]"
            )

            table = Table(box=None, show_header=False)
            table.add_column()
            table.add_column()

            if is_local:
                table.add_row("[b][$light_blue]Cluster", f"[b][$highlight]{cname}[/$highlight]")
            else:
                table.add_row("[b][$light_blue]Cluster", f"[$light_blue]{cname}")

            table.add_row("[b][$label]ClusterSet", clusterset_name)
            table.add_row("[b][$label]Role", cluster_role_fmt)

            if invalidated:
                table.add_row("[b][$label]State", "[$red]INVALIDATED[/$red]")

            if is_local and cluster_role == "REPLICA":
                replication_display, channel_error = _format_clusterset_replication(
                    clusterset_channel,
                    is_cluster_primary=dolphie.is_group_replication_primary,
                )
                table.add_row("[b][$label]Replication", replication_display)
                if channel_error:
                    table.add_row("[b][$label]Error", f"[$red]{channel_error}[/$red]")

            if members:
                table.add_row("[b][$label]Members", "\n".join(members))

            item_key = f"{clusterset_name}_{cname}"
            items[item_key] = table
            if is_local:
                highlighted_clusters.add(item_key)

        tab.clusterset_grid.set_class(len(items) == 2, "two_columns")
        _sync_grid(
            tab.clusterset_grid,
            items,
            "clusterset",
            tab.id,
            dolphie.app,
            tab.clusterset_widgets,
            highlighted_keys=highlighted_clusters,
        )


# This function isn't in create_panel() because it's called as part of the replica worker instead of the main worker
def create_replica_panel(tab: Tab):
    dolphie = tab.dolphie

    # Refresh optimization: cache frequently accessed objects
    replica_manager = dolphie.replica_manager
    panels = dolphie.panels

    if not replica_manager.active_count:
        # Replay tabs never run the replicas worker, so restored discovery data
        # must not show a loading state that can never resolve.
        replica_count = 0 if dolphie.replay_file else replica_manager.discovery_count
        tab.replicas_container.display = bool(replica_count)
        tab.replicas_loading_indicator.display = bool(replica_count)
        if replica_count:
            tab.replicas_title.update(f"[$white][b]Loading [$highlight]{replica_count}[/$highlight] replicas...\n")
        _sync_grid(tab.replicas_grid, {}, "replica", tab.id, dolphie.app, tab.replica_widgets)
        return

    tab.replicas_container.display = True
    tab.replicas_loading_indicator.display = False

    # Update replicas title
    num_replicas = replica_manager.active_count
    title_prefix = panels.replication.content_key
    tab.replicas_title.update(f"[b]{title_prefix}Replicas ([$highlight]{num_replicas}[/$highlight])")

    # Sync replica grid widgets with current replica data
    sorted_replicas = replica_manager.get_sorted_replicas()
    items = {
        replica.row_key: (
            _create_replica_error_table(replica, replica.last_error)
            if replica.last_error
            else create_replication_table(tab, replica=replica)
        )
        for replica in sorted_replicas
        if replica.last_error or replica.replication_status
    }

    _sync_grid(tab.replicas_grid, items, "replica", tab.id, dolphie.app, tab.replica_widgets)


def create_replication_table(
    tab: Tab,
    dashboard_table: bool = False,
    replica: Replica | None = None,
    channel_data: Mapping[str, DatabaseScalar] | None = None,
    show_channel_name: bool = False,
) -> Table:
    dolphie = tab.dolphie

    # When replica is specified, that means we're creating a table for a replica and not replication
    if replica:
        data = replica.replication_status
        mysql_version = replica.mysql_version or ""
        connection_source_alt = replica.connection_source_alt
    else:
        data = channel_data if channel_data is not None else {}
        mysql_version = dolphie.host_version or ""
        connection_source_alt = dolphie.connection_source_alt

    # Determine replication terminology based on MySQL version
    # and connection source (MariaDB or MySQL)
    source_prefix = (
        "Source"
        if dolphie.is_mysql_version_at_least("8.0.22", mysql_version)
        and connection_source_alt != ConnectionSource.mariadb
        else "Master"
    )
    replica_prefix = "Replica" if source_prefix == "Source" else "Slave"
    uuid_key = f"{source_prefix}_UUID"

    primary_uuid = coerce_str(data.get(uuid_key))
    primary_host = dolphie.get_hostname(coerce_str(data.get(f"{source_prefix}_Host")))
    primary_user = data.get(f"{source_prefix}_User")
    primary_log_file = data.get(f"{source_prefix}_Log_File")
    primary_ssl_allowed = data.get(f"{source_prefix}_SSL_Allowed")
    relay_primary_log_file = data.get(f"Relay_{source_prefix}_Log_File")
    replica_sql_running_state = data.get(f"{replica_prefix}_SQL_Running_State") or data.get("Slave_SQL_State")
    replica_io_state = data.get(f"{replica_prefix}_IO_State")
    read_primary_log_pos = data.get(f"Read_{source_prefix}_Log_Pos")
    exec_primary_log_pos = data.get(f"Exec_{source_prefix}_Log_Pos")

    is_io_running = data.get(f"{replica_prefix}_IO_Running") == "Yes"
    is_sql_running = data.get(f"{replica_prefix}_SQL_Running") == "Yes"
    io_thread_running = "[$green]ON[/$green]" if is_io_running else "[$red]OFF[/$red]"
    sql_thread_running = "[$green]ON[/$green]" if is_sql_running else "[$red]OFF[/$red]"

    # Determine GTID status
    mariadb_using_gtid = data.get("Using_Gtid")
    mariadb_gtid_enabled = mariadb_using_gtid not in (None, "No")
    mysql_gtid_enabled = bool(data.get("Executed_Gtid_Set"))

    if mariadb_gtid_enabled:
        gtid_status = mariadb_using_gtid
    elif mysql_gtid_enabled:
        auto_position = "ON" if data.get("Auto_Position") == 1 else "OFF"
        gtid_status = f"ON [$label]Auto Position[/$label] {auto_position}"
    else:
        gtid_status = "OFF"

    # Replica lag calculation
    replica_lag_value = data.get("Seconds_Behind")
    replica_lag = coerce_float(replica_lag_value) if replica_lag_value is not None else None
    formatted_replica_lag = None
    if replica_lag is not None:
        sql_delay = data.get("SQL_Delay")
        if sql_delay:
            # Check if it's already an int or a string representing an int
            replica_lag = max(0, replica_lag - coerce_float(sql_delay))

        lag_color = "$green"
        if replica_lag >= 20:
            lag_color = "$red"
        elif replica_lag >= 10:
            lag_color = "$yellow"

        formatted_replica_lag = f"[{lag_color}]{format_time(replica_lag)}[/{lag_color}]"

    if dashboard_table:
        title = "Replication"
        if show_channel_name and data.get("Channel_Name"):
            title = f"Replication ({data.get('Channel_Name')})"
        table = Table(
            show_header=False,
            box=None,
            expand=True,
            title=title,
            title_style="b_light_blue",
            style="table_border",
        )
        table.add_column(no_wrap=True)
        table.add_column(max_width=30)
    else:
        table = Table(show_header=False, box=None)
        table.add_column()
        table.add_column(overflow="fold")

    channel_name = coerce_str(data.get("Channel_Name"))

    if replica:
        table.add_row("[b][$light_blue]Host", f"[$light_blue]{replica.host_with_port}")
        if channel_name:
            table.add_row("[b][$label]Channel", channel_name)
        table.add_row("[b][$label]Version", f"{replica.host_distro} {replica.mysql_version}")
    else:
        if channel_name and not dashboard_table:
            table.add_row("[b][$label]Channel", channel_name)
        table.add_row("[b][$label]Primary", primary_host)

    if not dashboard_table:
        table.add_row("[b][$label]User", primary_user)

    table.add_row(
        "[b][$label]Thread",
        f"[$label]IO[/$label] {io_thread_running} [$label]SQL[/$label] {sql_thread_running}",
    )

    replication_delay = ""
    if data.get("SQL_Delay"):
        if dashboard_table:
            replication_delay = "[$dark_yellow](delayed)"
        else:
            replication_delay = f"[$dark_yellow]Delay[/$dark_yellow] {format_time(coerce_float(data.get('SQL_Delay')))}"

    if not is_sql_running:
        table.add_row("[b][$label]Lag", "[$red]Stopped[/$red]")
    elif formatted_replica_lag is None:
        table.add_row("[b][$label]Lag", "[$yellow]Unknown[/$yellow]")
    else:
        table.add_row(
            "[b][$label]Lag",
            f"{formatted_replica_lag} [$label]Speed[/$label] {data.get('Replica_Speed', 0)} {replication_delay}",
        )

    if dashboard_table:
        table.add_row("[$label]Binlog IO", str(primary_log_file))
        table.add_row("[$label]Binlog SQL", str(relay_primary_log_file))
        table.add_row("[$label]Relay Log ", str(data.get("Relay_Log_File", "N/A")))
        table.add_row("[$label]GTID", gtid_status)
        table.add_row("[$label]State", str(replica_sql_running_state))
    else:
        table.add_row(
            "[b][$label]Binlog IO",
            f"{primary_log_file} ([$dark_gray]{read_primary_log_pos}[/$dark_gray])",
        )
        table.add_row(
            "[b][$label]Binlog SQL",
            f"{relay_primary_log_file} ([$dark_gray]{exec_primary_log_pos}[/$dark_gray])",
        )
        table.add_row(
            "[b][$label]Relay Log",
            f"{data.get('Relay_Log_File', 'N/A')} ([$dark_gray]{data.get('Relay_Log_Pos', 'N/A')}[/$dark_gray])",
        )

    if not dashboard_table:
        ssl_enabled = "ON" if primary_ssl_allowed == "Yes" else "OFF"
        table.add_row("[b][$label]SSL", ssl_enabled)

        replication_status_filtering = [
            "Replicate_Do_DB",
            "Replicate_Ignore_Table",
            "Replicate_Ignore_DB",
            "Replicate_Do_Table",
            "Replicate_Wild_Do_Table",
            "Replicate_Wild_Ignore_Table",
            "Replicate_Rewrite_DB",
        ]

        for status_filter in replication_status_filtering:
            value = data.get(status_filter)

            status_filter_formatted = f"Filter: {status_filter.split('Replicate_')[1]}"
            if value:
                table.add_row(f"[b][$label]{status_filter_formatted}", str(value))

        error_types = ["Last_IO_Error", "Last_SQL_Error"]
        errors = [(error_type, error) for error_type in error_types if (error := data.get(error_type))]

        if errors:
            for error_type, error_message in errors:
                table.add_row(
                    f"[b][$label]{error_type.replace('_', ' ')}",
                    f"[$red]{error_message}[/$red]",
                )
        else:
            table.add_row("[b][$label]IO State", str(replica_io_state))
            table.add_row("[b][$label]SQL State", str(replica_sql_running_state))

        if mysql_gtid_enabled:
            executed_gtid_set = coerce_str(data.get("Executed_Gtid_Set"))
            retrieved_gtid_set = coerce_str(data.get("Retrieved_Gtid_Set"))

            table.add_row("[b][$label]GTID", gtid_status)

            if replica:
                if replica.errant_check_error:
                    errant_trx = f"[$yellow]{replica.errant_check_error}[/$yellow]"
                elif replica.errant_transactions:
                    errant_trx = f"[$red]{replica.errant_transactions}[/$red]"
                else:
                    errant_trx = "[$green]None[/$green]"
                table.add_row("[b][$label]Errant TRX", errant_trx)
                # If this replica has replicas, use its primary server UUID, else use its own
                primary_uuid = primary_uuid or coerce_str(dolphie.server_uuid)

            retrieved_gtid_set = _color_gtid_sets(retrieved_gtid_set, primary_uuid)
            executed_gtid_set = _color_gtid_sets(executed_gtid_set, primary_uuid)

            table.add_row("[b][$label]Retrieved GTID", retrieved_gtid_set)
            table.add_row("[b][$label]Executed GTID", executed_gtid_set)
        elif mariadb_gtid_enabled:
            primary_id = data.get("Master_Server_Id")

            table.add_row("[b][$label]GTID", gtid_status)

            if replica:
                # Determine the primary server ID for coloring
                replica_primary_server_id = (
                    dolphie.replication_status[0].get("Master_Server_Id") if dolphie.replication_status else None
                )
                primary_id = replica_primary_server_id or dolphie.global_variables.get("server_id")

                if replica.errant_check_error:
                    errant_trx = f"[$yellow]{replica.errant_check_error}[/$yellow]"
                elif replica.errant_transactions:
                    errant_trx = f"[$red]{replica.errant_transactions}[/$red]"
                else:
                    errant_trx = "[$green]None[/$green]"
                table.add_row("[b][$label]Errant TRX", errant_trx)

                # Retrieved GTID from SHOW SLAVE STATUS
                gtid_io_pos = data.get("Gtid_IO_Pos")
                if gtid_io_pos:
                    table.add_row(
                        "[b][$label]Retrieved GTID",
                        _color_mariadb_gtid_sets(coerce_str(gtid_io_pos), primary_id),
                    )

                # Executed GTID from the replica's gtid_slave_pos
                if replica.mariadb_gtid_slave_pos:
                    table.add_row(
                        "[b][$label]Executed GTID",
                        _color_mariadb_gtid_sets(replica.mariadb_gtid_slave_pos, primary_id),
                    )
            else:
                # Self-view: this host is a replica
                gtid_io_pos = data.get("Gtid_IO_Pos")
                if gtid_io_pos:
                    table.add_row(
                        "[b][$label]Retrieved GTID",
                        _color_mariadb_gtid_sets(coerce_str(gtid_io_pos), primary_id),
                    )

                gtid_slave_pos = dolphie.global_variables.get("gtid_slave_pos")
                if gtid_slave_pos:
                    table.add_row(
                        "[b][$label]Executed GTID",
                        _color_mariadb_gtid_sets(coerce_str(gtid_slave_pos), primary_id),
                    )

    return table


def create_group_replication_member_table(tab: Tab) -> dict[str, Table]:
    dolphie = tab.dolphie

    if not dolphie.group_replication_members:
        return {}

    unsorted: list[tuple[str, str, Table]] = []  # (member_id, host, table)
    for row in dolphie.group_replication_members:
        member_id = coerce_str(row.get("MEMBER_ID"))
        member_host = coerce_str(row.get("MEMBER_HOST"))
        member_port = row.get("MEMBER_PORT")
        host = f"{member_host}:{member_port}"
        is_local = member_id == coerce_str(dolphie.server_uuid)

        member_role = row.get("MEMBER_ROLE", "N/A")
        if member_role == "PRIMARY":
            member_role = f"[b][$highlight]{member_role}[/$highlight]"

        member_state = row.get("MEMBER_STATE", "N/A")
        member_state = (
            f"[$green]{member_state}[/$green]" if member_state == "ONLINE" else f"[$red]{member_state}[/$red]"
        )

        table = Table(box=None, show_header=False)
        table.add_column()
        table.add_column()

        member_display = f"[b][$highlight]{host}[/$highlight]" if is_local else f"[$light_blue]{host}"
        table.add_row("[b][$light_blue]Member", member_display)
        table.add_row("[b][$label]UUID", str(member_id))
        table.add_row("[b][$label]Role", member_role)
        table.add_row("[b][$label]State", member_state)
        table.add_row("[b][$label]Version", row.get("MEMBER_VERSION", "N/A"))

        table.add_row(
            "[b][$label]Certifier",
            f"[$label]Queue[/$label] {format_number(coerce_int(row.get('COUNT_TRANSACTIONS_IN_QUEUE')))}"
            f" [$label]Checked[/$label] {format_number(coerce_int(row.get('COUNT_TRANSACTIONS_CHECKED')))}"
            f" [$label]Detected[/$label] {format_number(coerce_int(row.get('COUNT_CONFLICTS_DETECTED')))}",
        )
        table.add_row(
            "[b][$label]Applier",
            f"{format_number(coerce_int(row.get('COUNT_TRANSACTIONS_REMOTE_APPLIED')))}"
            f" [$label]Queue[/$label]"
            f" {format_number(coerce_int(row.get('COUNT_TRANSACTIONS_REMOTE_IN_APPLIER_QUEUE')))}",
        )
        table.add_row(
            "[b][$label]Local",
            f"[$label]Proposed[/$label] {format_number(coerce_int(row.get('COUNT_TRANSACTIONS_LOCAL_PROPOSED')))}"
            f" [$label]Rollback[/$label] {format_number(coerce_int(row.get('COUNT_TRANSACTIONS_LOCAL_ROLLBACK')))}",
        )
        table.add_row(
            "[b][$label]Cert Rows",
            format_number(coerce_int(row.get("COUNT_TRANSACTIONS_ROWS_VALIDATING"))),
        )

        unsorted.append((member_id, host, table))

    # Return sorted by host, keyed by member_id
    return {mid: tbl for mid, _, tbl in sorted(unsorted, key=lambda x: x[1])}


def create_galera_node_table(tab: Tab) -> dict[str, Table]:
    dolphie = tab.dolphie
    galera = dolphie.global_status

    if not dolphie.galera_cluster_members:
        return {}

    local_uuid = galera.get("wsrep_gcomm_uuid")

    unsorted: list[tuple[str, str, Table]] = []
    for row in dolphie.galera_cluster_members:
        node_uuid = coerce_str(row.get("node_uuid"))
        node_name = coerce_str(row.get("node_name"), "N/A")
        node_address = coerce_str(row.get("node_incoming_address"), "N/A")
        # Strip the port if it's 0 (default when wsrep-node-incoming-address isn't set)
        if node_address.endswith(":0"):
            node_address = node_address[:-2]
        is_local = node_uuid == local_uuid

        table = Table(box=None, show_header=False)
        table.add_column()
        table.add_column()

        if is_local:
            table.add_row(
                "[b][$light_blue]Member", f"[b][$highlight]{node_name}[/$highlight] [$light_blue]({node_address})"
            )
        else:
            table.add_row("[b][$light_blue]Member", f"[$light_blue]{node_name} ({node_address})")

        table.add_row("[b][$label]UUID", f"[$dark_gray]{node_uuid}[/$dark_gray]")

        if is_local:
            # Node state
            node_state = galera.get("wsrep_local_state_comment", "N/A")
            if node_state == "Synced":
                node_state_colored = f"[$green]{node_state}[/$green]"
            elif node_state in ("Donor/Desynced", "Donor"):
                node_state_colored = f"[$yellow]{node_state}[/$yellow]"
            else:
                node_state_colored = f"[$red]{node_state}[/$red]"

            table.add_row(
                "[b][$label]State",
                f"{node_state_colored}"
                f"  [$label]Connected[/$label] {galera.get('wsrep_connected', 'N/A')}"
                f"  [$label]Ready[/$label] {galera.get('wsrep_ready', 'N/A')}",
            )

            # Flow control
            flow_control_paused = coerce_float(galera.get("wsrep_flow_control_paused")) * 100

            table.add_row(
                "[b][$label]Flow Control",
                f"[$label]Paused[/$label] {flow_control_paused:.2f}%"
                f"  [$label]Sent[/$label] {format_number(galera.get('wsrep_flow_control_sent', 0))}"
                f"  [$label]Received[/$label] {format_number(galera.get('wsrep_flow_control_recv', 0))}",
            )
            table.add_row(
                "[b][$label]Queues",
                f"[$label]Recv[/$label] {format_number(galera.get('wsrep_local_recv_queue', 0))}"
                f" (avg {coerce_float(galera.get('wsrep_local_recv_queue_avg')):.4f})"
                f"  [$label]Send[/$label] {format_number(galera.get('wsrep_local_send_queue', 0))}"
                f" (avg {coerce_float(galera.get('wsrep_local_send_queue_avg')):.4f})",
            )

            # Certification
            cert_failures = int(galera.get("wsrep_local_cert_failures", 0))
            bf_aborts = int(galera.get("wsrep_local_bf_aborts", 0))

            table.add_row(
                "[b][$label]Certification",
                f"[$label]Deps[/$label] {format_number(galera.get('wsrep_cert_deps_distance', 0))}"
                f"  [$label]Failures[/$label] {format_number(cert_failures)}"
                f"  [$label]Aborts[/$label] {format_number(bf_aborts)}",
            )
            table.add_row(
                "[b][$label]Writesets",
                f"[$label]Replicated[/$label] {format_number(galera.get('wsrep_replicated', 0))}"
                f" ({format_bytes(galera.get('wsrep_replicated_bytes', 0))})"
                f"  [$label]Received[/$label] {format_number(galera.get('wsrep_received', 0))}"
                f" ({format_bytes(galera.get('wsrep_received_bytes', 0))})",
            )

        unsorted.append((node_uuid, node_name, table))

    # Return sorted by node_name, keyed by node_uuid
    return {uid: tbl for uid, _, tbl in sorted(unsorted, key=lambda x: x[1])}


def _replica_lag_value(data: Mapping[str, DatabaseScalar], lag_key: str) -> int | None:
    """Return replica lag without turning an unknown SQL NULL into zero."""
    value = data.get(lag_key)
    if value is None and lag_key not in data:
        value = data.get("Seconds_Behind_Master")
    return None if value is None else coerce_int(value)


def fetch_replication_data(tab: Tab, replica: Replica | None = None) -> DatabaseRow | list[DatabaseRow]:
    dolphie = tab.dolphie
    connection = replica.connection if replica else dolphie.main_db_connection
    mysql_version = (replica.mysql_version if replica else dolphie.host_version) or ""
    connection_source_alt = replica.connection_source_alt if replica else dolphie.connection_source_alt

    if connection is None:
        return {} if replica else []

    # Determine replication status query
    use_mariadb_status = connection_source_alt == ConnectionSource.mariadb
    use_show_replica_status = (
        dolphie.is_mysql_version_at_least("8.0.22", use_version=mysql_version) and not use_mariadb_status
    )
    if use_mariadb_status:
        replication_status_query = (
            MySQLQueries.show_all_replicas_status
            if dolphie.is_mysql_version_at_least("10.5.1", use_version=mysql_version)
            else MySQLQueries.show_all_slaves_status
        )
    elif use_show_replica_status:
        replication_status_query = MySQLQueries.show_replica_status
    else:
        replication_status_query = MySQLQueries.show_slave_status

    # Determine lag source and query
    replica_lag_source = "HB" if dolphie.heartbeat_table else None
    replica_lag_query = MySQLQueries.heartbeat_replica_lag if replica_lag_source else replication_status_query

    # Fetch replication status
    connection.execute(replication_status_query)
    all_rows = connection.fetchall()
    if use_mariadb_status:
        for row in all_rows:
            connection_name = coerce_str(row.get("Connection_name"))
            if connection_name:
                row["Channel_Name"] = connection_name

    # Track every configured source. On a downstream multi-source replica, GTIDs
    # from its other channels are legitimate and must not be reported as errant.
    uuid_field = "Source_UUID" if use_show_replica_status else "Master_UUID"
    source_uuids = {uuid for row in all_rows if (uuid := coerce_str(row.get(uuid_field)))}
    if replica:
        replica.replication_source_uuids = source_uuids
    else:
        dolphie.replication_source_uuids = source_uuids

    # Fetch replica lag using alternative method if applicable
    if replica_lag_source:
        connection.execute(replica_lag_query)
        replica_lag_data = connection.fetchone()
    else:
        replica_lag_data = None

    lag_key = "Seconds_Behind_Source" if use_show_replica_status else "Seconds_Behind_Master"

    # For replicas (downstream hosts), return a single dict as before
    if replica:
        if not all_rows:
            return {}

        # For multi-source replicas, find the channel connected to the monitored primary.
        # Fall back to the first channel when none matches (IO thread reconnecting, or
        # the replica reaches the primary via a VIP/proxy) instead of failing the poll.
        source_identity_field = "Master_Server_Id" if use_mariadb_status else uuid_field
        if len(all_rows) > 1:
            matching_status = next(
                (
                    row
                    for row in all_rows
                    if coerce_str(row.get(source_identity_field)) == coerce_str(dolphie.server_uuid)
                ),
                None,
            )
            replication_status = matching_status if matching_status is not None else all_rows[0]
        else:
            replication_status = all_rows[0]

        lag_source = replica_lag_data if replica_lag_data else replication_status
        replica_lag = _replica_lag_value(lag_source, lag_key)

        if replication_status:
            previous_lag = _replica_lag_value(replica.replication_status, "Seconds_Behind")
            replication_status["Seconds_Behind"] = replica_lag
            replication_status["Replica_Speed"] = (
                round((previous_lag - replica_lag) / dolphie.polling_latency)
                if previous_lag
                and replica_lag is not None
                and replica_lag < previous_lag
                and dolphie.polling_latency > 0
                else 0
            )

        return replication_status

    # For the main host, return a list of dicts (one per channel)
    # Build a lookup of previous lag by channel name for speed calculation
    previous_lag_by_channel = {}
    for prev_channel in dolphie.replication_status:
        ch_name = coerce_str(prev_channel.get("Channel_Name"))
        previous_lag_by_channel[ch_name] = prev_channel.get("Seconds_Behind", 0)

    is_multi_source = len(all_rows) > 1

    result = []
    for row in all_rows:
        # Heartbeat lag is not channel-aware, so only use it for single-source setups
        lag_source = replica_lag_data if (replica_lag_data and not is_multi_source) else row
        replica_lag = _replica_lag_value(lag_source, lag_key)

        channel_name = coerce_str(row.get("Channel_Name"))
        previous_lag_value = previous_lag_by_channel.get(channel_name)
        previous_lag = None if previous_lag_value is None else coerce_int(previous_lag_value)
        row["Seconds_Behind"] = replica_lag
        row["Replica_Speed"] = (
            round((previous_lag - replica_lag) / dolphie.polling_latency)
            if previous_lag and replica_lag is not None and replica_lag < previous_lag and dolphie.polling_latency > 0
            else 0
        )

        result.append(row)

    return result


def _refresh_errant_transactions(tab: Tab, replica: Replica, current_time: float) -> None:
    """Refresh expensive errant-GTID checks at a lower cadence than status polling."""
    if current_time < replica.next_errant_check_at or replica.connection is None:
        return

    replica.next_errant_check_at = current_time + _ERRANT_CHECK_INTERVAL_SECONDS
    data = replica.replication_status

    try:
        if data.get("Executed_Gtid_Set"):
            replica_gtid_set = coerce_str(data.get("Executed_Gtid_Set"))
            primary_gtid_set = coerce_str(tab.dolphie.global_variables.get("gtid_executed"))
            retrieved_gtid_set = coerce_str(data.get("Retrieved_Gtid_Set"))
            # The primary snapshot and replica status are collected independently.
            # Retrieved GTIDs prove that any apparent lead came from the source,
            # while locally injected same-source GTIDs remain detectable.
            replica.connection.execute(
                "SELECT GTID_SUBTRACT(GTID_SUBTRACT(%s, %s), %s) AS errant_trxs",
                (replica_gtid_set, primary_gtid_set, retrieved_gtid_set),
            )
            if not replica.connection.last_execute_successful:
                replica.errant_check_error = "Unavailable"
                return

            gtid_data = replica.connection.fetchone()
            monitored_source_uuid = tab.dolphie.server_uuid if isinstance(tab.dolphie.server_uuid, str) else ""
            unrelated_source_uuids = (replica.replication_source_uuids | tab.dolphie.replication_source_uuids) - {
                monitored_source_uuid
            }
            ignored_uuids = unrelated_source_uuids | {replica.group_replication_view_change_uuid}
            if not retrieved_gtid_set:
                # A replica restart clears Retrieved_Gtid_Set, so provenance of the
                # monitored source's GTIDs can't be proven and a stale gtid_executed
                # snapshot would misreport the primary's own recent GTIDs as errant.
                # The Group Replication group UUID has the same race.
                ignored_uuids.add(monitored_source_uuid)
                ignored_uuids.add(coerce_str(tab.dolphie.global_variables.get("group_replication_group_name")))
            replica.errant_transactions = (
                _filter_gtid_sets(
                    coerce_str(gtid_data.get("errant_trxs")),
                    {uuid for uuid in ignored_uuids if uuid},
                )
                or None
            )
            replica.errant_check_error = None
            return

        mariadb_using_gtid = data.get("Using_Gtid")
        if mariadb_using_gtid not in (None, "No"):
            replica.connection.execute(
                "SELECT @@server_id AS server_id, @@gtid_slave_pos AS gtid_slave_pos, "
                "@@gtid_current_pos AS gtid_current_pos"
            )
            if not replica.connection.last_execute_successful:
                replica.errant_check_error = "Unavailable"
                return

            gtid_data = replica.connection.fetchone()
            replica.mariadb_gtid_slave_pos = coerce_str(gtid_data.get("gtid_slave_pos"))
            replica_gtid_current_pos = coerce_str(gtid_data.get("gtid_current_pos"))
            primary_gtid_current_pos = coerce_str(tab.dolphie.global_variables.get("gtid_current_pos"))
            replica.errant_transactions = (
                _detect_mariadb_errant_trx(
                    replica_gtid_current_pos,
                    gtid_data.get("server_id"),
                    primary_gtid_current_pos,
                )
                if replica_gtid_current_pos and primary_gtid_current_pos
                else None
            )
            replica.errant_check_error = None
            return

        replica.errant_transactions = None
        replica.errant_check_error = None
        replica.mariadb_gtid_slave_pos = ""
    except ManualException as error:
        replica.errant_check_error = error.reason
        logger.warning(f"Unable to check errant transactions for {replica.host_with_port}: {error.reason}")


def _create_replica_error_table(replica: Replica, error: str) -> Table:
    table = Table(box=None, show_header=False)
    table.add_column()
    table.add_column(overflow="fold")
    table.add_row("[b][$light_blue]Host", f"[$light_blue]{replica.host_with_port}")
    table.add_row("[b][$label]User", replica.user or "N/A")
    table.add_row("[b][$label]Error", f"[$red]{error}")
    return table


def _record_replica_error(replica: Replica, error: str, current_time: float, refresh_interval: float) -> None:
    replica.last_error = error
    replica.consecutive_errors += 1
    delay = min(max(refresh_interval, 1) * (2 ** (replica.consecutive_errors - 1)), _MAX_REPLICA_BACKOFF_SECONDS)
    replica.next_poll_at = current_time + delay


def _poll_replica(tab: Tab, replica: Replica, current_time: float) -> None:
    if current_time < replica.next_poll_at:
        return

    dolphie = tab.dolphie
    try:
        if replica.connection is None:
            # user/password may legitimately be None (auth-socket/passwordless setups);
            # Database passes them through to pymysql, which accepts None.
            replica.connection = Database(
                app=dolphie.app,
                host=replica.host,
                user=dolphie.user,
                password=dolphie.password,
                port=replica.port or 3306,
                socket=None,
                ssl=dolphie.ssl,
                save_connection_id=False,
                read_timeout=_REPLICA_QUERY_TIMEOUT_SECONDS,
            )
            global_variables = replica.connection.fetch_status_and_variables("variables")
            if not global_variables:
                raise ManualException("Unable to read replica server variables")

            replica.mysql_version = dolphie.parse_server_version(coerce_str(global_variables.get("version")))
            replica.host_distro, replica.connection_source_alt = dolphie.determine_distro_and_connection_source_alt(
                global_variables
            )
            view_change_uuid = coerce_str(global_variables.get("group_replication_view_change_uuid"))
            replica.group_replication_view_change_uuid = (
                view_change_uuid if view_change_uuid and view_change_uuid != "AUTOMATIC" else ""
            )

            if replica.connection_source_alt == ConnectionSource.mariadb:
                server_id = coerce_int(global_variables.get("server_id"))
                reported = dolphie.replica_manager.mariadb_reported_ports.get(server_id) if server_id else None
                if reported is not None:
                    replica.reported_host, replica.reported_port = reported

        replication_status = fetch_replication_data(tab, replica)
        if not isinstance(replication_status, dict) or not replication_status:
            raise ManualException("Replication status is unavailable")

        replica.replication_status = replication_status
        _refresh_errant_transactions(tab, replica, current_time)
        replica.last_error = None
        replica.consecutive_errors = 0
        replica.next_poll_at = 0
    except Exception as error:
        if replica.connection:
            replica.connection.close()
        replica.connection = None
        replica.replication_status = {}
        if isinstance(error, ManualException):
            message = error.reason
        else:
            message = str(error) or type(error).__name__
            logger.exception(f"Unexpected replica polling error for {replica.host_with_port}")
        _record_replica_error(replica, message, current_time, dolphie.refresh_interval)


def fetch_replicas(tab: Tab) -> None:
    """Reconcile and concurrently poll the current atomic replica discovery snapshot."""
    dolphie = tab.dolphie
    replica_manager = dolphie.replica_manager
    discovered = replica_manager.available_replicas
    active_row_keys: set[str] = set()
    poll_targets: list[Replica] = []
    seen_identities: set[str] = set()

    for row in discovered:
        raw_host = coerce_str(row.get("host"))
        thread_id = row.get("id")
        port = row.get("port")
        if not raw_host or thread_id is None or port is None:
            continue

        host = dolphie.get_hostname(host_without_port(raw_host))
        identity = coerce_str(row.get("identity")) or f"endpoint:{host.lower()}:{port}"
        if identity in seen_identities:
            logger.warning(f"Skipping duplicate replica identity {identity}")
            continue
        seen_identities.add(identity)
        replica = replica_manager.upsert_replica(
            identity=identity,
            thread_id=thread_id,
            host=host,
            port=port,
            user=coerce_str(row.get("user")),
        )
        active_row_keys.add(replica.row_key)
        poll_targets.append(replica)

    replica_manager.remove_missing_replicas(active_row_keys)
    if not poll_targets:
        return

    current_time = time.monotonic()
    if len(poll_targets) == 1:
        _poll_replica(tab, poll_targets[0], current_time)
        return

    futures = [_replica_poll_executor.submit(_poll_replica, tab, replica, current_time) for replica in poll_targets]
    for future in futures:
        future.result()
