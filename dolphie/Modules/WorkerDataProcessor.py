from collections import Counter
from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, cast

from loguru import logger

from dolphie.DataTypes import ConnectionSource, ConnectionStatus, DatabaseRow, DatabaseScalar, ReplicaRow
from dolphie.Modules.Functions import coerce_float, coerce_int, coerce_str, host_without_port
from dolphie.Modules.PerformanceSchemaMetrics import PerformanceSchemaMetrics
from dolphie.Modules.Queries import MySQLQueries, ProxySQLQueries
from dolphie.Panels import MetadataLocks as MetadataLocksPanel
from dolphie.Panels import Processlist as ProcesslistPanel
from dolphie.Panels import ProxySQLProcesslist as ProxySQLProcesslistPanel
from dolphie.Panels import Replication as ReplicationPanel

if TYPE_CHECKING:
    from dolphie.App import DolphieApp
    from dolphie.Modules.TabManager import Tab


def _reported_replica_identity(row: DatabaseRow, *, mariadb: bool, uuid_key: str) -> str:
    identity_value = row.get("Server_id") if mariadb else row.get(uuid_key)
    identity = coerce_str(identity_value)
    return f"{'mariadb-id' if mariadb else 'mysql-uuid'}:{identity}" if identity else ""


def build_replica_discovery(
    processlist_rows: list[DatabaseRow],
    reported_rows: list[DatabaseRow],
    previous_rows: list[ReplicaRow],
    *,
    mariadb: bool,
    use_show_replicas: bool,
    replicaset: bool = False,
) -> list[ReplicaRow]:
    """Correlate processlist discovery with stable replica endpoints."""
    uuid_key = "Replica_UUID" if use_show_replicas else "Slave_UUID"
    previous_by_thread_id = {row.get("id"): row for row in previous_rows if row.get("id") is not None}

    reports = []
    for row in reported_rows:
        port = coerce_int(row.get("Port")) if row.get("Port") is not None else None
        report_host = coerce_str(row.get("Host"))
        identity = _reported_replica_identity(row, mariadb=mariadb, uuid_key=uuid_key)
        if port is not None:
            reports.append((identity, report_host, port))

    unused_reports = set(range(len(reports)))
    discovered: list[ReplicaRow] = []
    pending: list[tuple[DatabaseRow, int, str, str]] = []

    for row in processlist_rows:
        thread_id = coerce_int(row.get("id"))
        raw_host = coerce_str(row.get("host"))
        if not thread_id or not raw_host:
            continue

        replica_uuid = coerce_str(row.get("replica_uuid"))
        if replicaset:
            host = host_without_port(raw_host)
            raw_port = raw_host.rpartition(":")[2]
            port = coerce_int(raw_port) if raw_port.isdigit() else 3306
            identity = f"mysql-uuid:{replica_uuid}" if replica_uuid else f"replicaset-instance:{thread_id}"
            discovered.append(
                {
                    "id": thread_id,
                    "user": coerce_str(row.get("user")),
                    "host": host,
                    "replica_uuid": replica_uuid,
                    "identity": identity,
                    "port": port,
                }
            )
            continue

        if not mariadb:
            matching_report = next(
                (
                    (index, report)
                    for index, report in enumerate(reports)
                    if index in unused_reports and report[0] == f"mysql-uuid:{replica_uuid}"
                ),
                None,
            )
            if matching_report:
                report_index, (reported_identity, report_host, port) = matching_report
                unused_reports.remove(report_index)
            else:
                reported_identity, report_host, port = "", "", 3306

            # report_host/report_port exist precisely so a replica behind NAT or on a
            # container-internal network (its processlist-visible peer address) can
            # advertise the address a monitor should actually connect through. Prefer
            # it, falling back to the processlist host only when nothing was reported.
            process_host = host_without_port(raw_host)
            host = host_without_port(report_host) if report_host else process_host
            identity = reported_identity or (
                f"mysql-uuid:{replica_uuid}" if replica_uuid else f"endpoint:{host.lower()}:{port}:thread:{thread_id}"
            )
            mysql_replica: ReplicaRow = {
                "id": thread_id,
                "user": coerce_str(row.get("user")),
                "host": host,
                "replica_uuid": replica_uuid,
                "identity": identity,
                "port": port,
            }
            if report_host:
                mysql_replica["report_host"] = report_host
            discovered.append(mysql_replica)
            continue

        pending.append((row, thread_id, raw_host, replica_uuid))

    # MariaDB does not expose the replica server ID in the binlog dump thread. Preserve
    # prior same-host correlations, then require an exact report_host match. When neither
    # matches (report_host unset, or a hostname while the processlist shows an IP), pair
    # the report anyway if it's unambiguous — a single replica with a single report —
    # otherwise a replica on a non-default port silently gets 3306. Ambiguous leftovers
    # are never paired by list position because that can cross-wire replica endpoints.
    pending.sort(key=lambda item: (host_without_port(item[2]).lower(), item[1]))
    for row, thread_id, raw_host, replica_uuid in pending:
        report_host = ""
        previous = previous_by_thread_id.get(thread_id)
        process_host = host_without_port(raw_host)
        previous_matches_host = bool(
            previous and host_without_port(coerce_str(previous.get("host"))).lower() == process_host.lower()
        )
        previous_identity = previous.get("identity", "") if previous_matches_host and previous else ""
        report_index = next(
            (
                index
                for index, report in enumerate(reports)
                if index in unused_reports and report[0] == previous_identity
            ),
            None,
        )

        if report_index is None:
            report_index = next(
                (
                    index
                    for index, (_, report_host, _) in enumerate(reports)
                    if index in unused_reports
                    and report_host
                    and host_without_port(report_host).lower() == process_host.lower()
                ),
                None,
            )

        if report_index is not None:
            identity, report_host, port = reports[report_index]
            unused_reports.remove(report_index)
            host = host_without_port(report_host) if report_host else process_host
        elif previous_matches_host and previous:
            port = coerce_int(previous.get("port"), 3306)
            host = process_host
            identity = previous_identity or f"endpoint:{host.lower()}:{port}:thread:{thread_id}"
        elif len(pending) == 1 and len(unused_reports) == 1:
            report_index = min(unused_reports)
            identity, report_host, port = reports[report_index]
            unused_reports.remove(report_index)
            host = host_without_port(report_host) if report_host else process_host
        else:
            port = 3306
            host = process_host
            identity = f"endpoint:{host.lower()}:{port}:thread:{thread_id}"

        mariadb_replica: ReplicaRow = {
            "id": thread_id,
            "user": coerce_str(row.get("user")),
            "host": host,
            "replica_uuid": replica_uuid,
            "identity": identity or f"endpoint:{host.lower()}:{port}:thread:{thread_id}",
            "port": port,
        }
        if report_index is not None and report_host:
            mariadb_replica["report_host"] = report_host
        discovered.append(mariadb_replica)

    identity_counts = Counter(coerce_str(row.get("identity")) for row in discovered)
    for row in discovered:
        identity = coerce_str(row.get("identity"))
        if identity and identity_counts[identity] > 1:
            row["identity"] = (
                f"{identity}:endpoint:{coerce_str(row.get('host')).lower()}:{coerce_int(row.get('port'), 3306)}"
            )

    return sorted(discovered, key=lambda row: (row.get("identity", ""), row.get("id", 0)))


def is_group_replication_primary(members: list[DatabaseRow], server_uuid: str | int | None) -> bool:
    """Return whether the current server is the reported Group Replication primary."""
    return any(row.get("MEMBER_ID") == server_uuid and row.get("MEMBER_ROLE") == "PRIMARY" for row in members)


class WorkerDataProcessor:
    """Manages polling data processing and screen refresh operations for worker threads.

    This class encapsulates all data processing logic, screen refresh operations,
    and monitoring of various system changes.
    """

    def __init__(self, app: "DolphieApp"):
        """Initialize the WorkerDataProcessor.

        Args:
            app: Reference to the main DolphieApp instance
        """
        self.app = app

    def _refresh_replica_discovery(self, tab: "Tab") -> None:
        """Refresh replica discovery.

        Skips entirely if the processlist query fails. Still publishes (without
        report-host correlation) if the SHOW REPLICAS/SHOW SLAVE HOSTS query fails.
        """
        dolphie = tab.dolphie
        if dolphie.replicaset:
            find_replicas_query = MySQLQueries.replicaset_find_replicas
        elif dolphie.connection_source_alt == ConnectionSource.mariadb:
            find_replicas_query = (
                MySQLQueries.mariadb_find_replicas
                if dolphie.performance_schema_enabled
                else MySQLQueries.pl_find_replicas
            )
        elif dolphie.performance_schema_enabled and dolphie.is_mysql_version_at_least("5.7"):
            find_replicas_query = MySQLQueries.ps_find_replicas
        else:
            find_replicas_query = MySQLQueries.pl_find_replicas

        dolphie.main_db_connection.execute(find_replicas_query)
        if not dolphie.main_db_connection.last_execute_successful:
            return
        processlist_replicas = dolphie.main_db_connection.fetchall()

        use_show_replicas = (
            dolphie.connection_source_alt != ConnectionSource.mariadb and dolphie.is_mysql_version_at_least("8.0.22")
        )
        reported_replicas: list[DatabaseRow] = []
        if not dolphie.daemon_mode and not dolphie.replicaset:
            # A replica endpoint change always surfaces as a new binlog dump thread,
            # so reported rows only need refetching when processlist discovery changes.
            replica_manager = dolphie.replica_manager
            discovery_signature = (
                use_show_replicas,
                tuple(
                    sorted(
                        (coerce_int(row.get("id")), coerce_str(row.get("host")), coerce_str(row.get("replica_uuid")))
                        for row in processlist_replicas
                    )
                ),
            )
            if replica_manager.reported_replica_signature == discovery_signature:
                reported_replicas = replica_manager.reported_replicas
            else:
                query = MySQLQueries.show_replicas if use_show_replicas else MySQLQueries.show_slave_hosts
                dolphie.main_db_connection.execute(query)
                if dolphie.main_db_connection.last_execute_successful:
                    reported_replicas = dolphie.main_db_connection.fetchall()
                    replica_manager.reported_replica_signature = discovery_signature
                    replica_manager.reported_replicas = reported_replicas
                # On failure (e.g. missing REPLICATION SLAVE privilege), still publish
                # processlist-based discovery without port correlation, and retry the
                # reported query next cycle by not caching the signature.

        is_mariadb = dolphie.connection_source_alt == ConnectionSource.mariadb
        normalized_replicas = build_replica_discovery(
            processlist_replicas,
            reported_replicas,
            dolphie.replica_manager.available_replicas,
            mariadb=is_mariadb,
            use_show_replicas=use_show_replicas,
            replicaset=dolphie.replicaset,
        )
        dolphie.replica_manager.replace_discovery(normalized_replicas)

        if is_mariadb:
            # See Replica.reported_host: MariaDB exposes no correlating key in the
            # processlist, so record each replica's advertised (host, port) by
            # server_id here for later resolution once connected.
            dolphie.replica_manager.mariadb_reported_ports = {
                coerce_int(row.get("Server_id")): (coerce_str(row.get("Host")), coerce_int(row.get("Port"), 3306))
                for row in reported_replicas
                if row.get("Server_id") is not None
            }

    def process_mysql_data(self, tab: "Tab"):
        """Process MySQL data for a given tab."""
        dolphie = tab.dolphie

        global_variables = dolphie.main_db_connection.fetch_status_and_variables("variables")
        self.monitor_global_variable_change(tab=tab, old_data=dolphie.global_variables, new_data=global_variables)
        dolphie.global_variables = global_variables

        # At this point, we're connected so we need to do a few things
        if dolphie.connection_status == ConnectionStatus.connecting:
            # Called from worker thread, use call_from_thread
            self.app.call_from_thread(
                self.app.tab_manager.update_connection_status, tab=tab, connection_status=ConnectionStatus.connected
            )
            dolphie.host_version = dolphie.parse_server_version(coerce_str(dolphie.global_variables.get("version")))
            dolphie.get_group_replication_metadata()
            dolphie.configure_mysql_variables()
            dolphie.validate_metadata_locks_enabled()

        global_status = dolphie.main_db_connection.fetch_status_and_variables("status")
        self.monitor_uptime_change(
            tab=tab,
            old_uptime=coerce_int(dolphie.global_status.get("Uptime")),
            new_uptime=coerce_int(global_status.get("Uptime")),
        )
        dolphie.global_status = dict(global_status)
        # If the server doesn't support Innodb_lsn_current, use Innodb_os_log_written instead
        # which has less precision, but it's good enough. Used for calculating the percentage of redo log used
        if not dolphie.global_status.get("Innodb_lsn_current"):
            fallback_lsn = dolphie.global_status.get("Innodb_os_log_written")
            if fallback_lsn is not None:
                dolphie.global_status["Innodb_lsn_current"] = fallback_lsn

        dolphie.innodb_metrics = dolphie.main_db_connection.fetch_status_and_variables("innodb_metrics")

        if dolphie.galera_cluster and dolphie.panels.replication.visible:
            dolphie.main_db_connection.execute(MySQLQueries.get_galera_cluster_members)
            dolphie.galera_cluster_members = dolphie.main_db_connection.fetchall()

        replication_status = ReplicationPanel.fetch_replication_data(tab)
        dolphie.replication_status = replication_status if isinstance(replication_status, list) else []

        self._refresh_replica_discovery(tab)

        if dolphie.is_mysql_version_at_least("8.2.0") and dolphie.connection_source_alt != ConnectionSource.mariadb:
            dolphie.main_db_connection.execute(MySQLQueries.show_binary_log_status)
        else:
            dolphie.main_db_connection.execute(MySQLQueries.show_master_status)

        previous_position_value = dolphie.binlog_status.get("Position")
        dolphie.binlog_status = dolphie.main_db_connection.fetchone()
        current_position = coerce_int(dolphie.binlog_status.get("Position"))

        if previous_position_value is None:
            dolphie.binlog_status["Diff_Position"] = 0
        elif coerce_int(previous_position_value) > current_position:
            dolphie.binlog_status["Diff_Position"] = "Binlog Rotated"
        else:
            dolphie.binlog_status["Diff_Position"] = current_position - coerce_int(previous_position_value)

        if dolphie.panels.processlist.visible:
            dolphie.processlist_threads = ProcesslistPanel.fetch_data(tab)

        if dolphie.panels.replication.visible and (dolphie.innodb_cluster or dolphie.innodb_cluster_read_replica):
            dolphie.main_db_connection.execute(MySQLQueries.get_clusterset_instances)
            dolphie.clusterset_instances = dolphie.main_db_connection.fetchall()
        else:
            dolphie.clusterset_instances = []

        if dolphie.performance_schema_enabled:
            dolphie.main_db_connection.execute(MySQLQueries.ps_disk_io)
            # ps_disk_io only selects two CONVERT(..., UNSIGNED) columns, so this is always int.
            dolphie.disk_io_metrics = cast(dict[str, int | str], dolphie.main_db_connection.fetchone())

            # MariaDB uses slave_parallel_threads; MySQL uses replica_parallel_workers
            if dolphie.connection_source_alt == ConnectionSource.mariadb:
                parallel_workers = coerce_int(dolphie.global_variables.get("slave_parallel_threads"))
            else:
                parallel_workers = coerce_int(dolphie.global_variables.get("replica_parallel_workers"))

            # The MariaDB table has a different schema from MySQL's
            # replication_applier_status_by_worker, so this query is MySQL-only.
            has_applier_status = (
                dolphie.connection_source_alt != ConnectionSource.mariadb and dolphie.is_mysql_version_at_least("8.0")
            )

            if (
                has_applier_status
                and dolphie.replication_status
                and dolphie.panels.replication.visible
                and parallel_workers > 1
            ):
                dolphie.main_db_connection.execute(MySQLQueries.replication_applier_status)
                all_rows = dolphie.main_db_connection.fetchall()

                # Partition rows by channel and compute per-channel diffs
                prev = dolphie.replication_applier_status
                channels: dict[str, dict[str, Any]] = {}
                for row in all_rows:
                    channel_name_value = row.get("CHANNEL_NAME")
                    thread_id = row.get("thread_id")
                    total_thread_events = coerce_int(row.get("total_thread_events"))

                    # Grand total rollup (NULL, NULL) — skip
                    if channel_name_value is None and thread_id is None:
                        continue

                    channel_name = coerce_str(channel_name_value)

                    # Per-channel subtotal rollup (channel_name, NULL thread_id)
                    if thread_id is None:
                        ch = channels.setdefault(channel_name, {"data": []})
                        prev_ch_value = prev.get(channel_name, {})
                        prev_ch = prev_ch_value if isinstance(prev_ch_value, dict) else {}
                        ch["diff_all"] = total_thread_events - prev_ch.get("previous_all", total_thread_events)
                        ch["previous_all"] = total_thread_events
                        continue

                    # Regular worker row
                    ch = channels.setdefault(channel_name, {"data": []})
                    prev_ch_value = prev.get(channel_name, {})
                    prev_ch = prev_ch_value if isinstance(prev_ch_value, dict) else {}
                    ch["data"].append(row)
                    ch[f"diff_{thread_id}"] = total_thread_events - prev_ch.get(
                        f"previous_{thread_id}", total_thread_events
                    )
                    ch[f"previous_{thread_id}"] = total_thread_events

                dolphie.replication_applier_status = channels
            else:
                dolphie.replication_applier_status = {}

            if (
                not dolphie.daemon_mode
                and dolphie.is_mysql_version_at_least("8.0.30")
                and dolphie.connection_source_alt != ConnectionSource.mariadb
            ):
                active_redo_logs_count = dolphie.main_db_connection.fetch_value_from_field(
                    MySQLQueries.active_redo_logs, "count"
                )
                if active_redo_logs_count is not None:
                    dolphie.global_status["Active_redo_log_count"] = coerce_int(active_redo_logs_count)
                else:
                    dolphie.global_status.pop("Active_redo_log_count", None)

            if dolphie.group_replication or dolphie.innodb_cluster:
                if dolphie.is_mysql_version_at_least("8.0.13"):
                    write_concurrency = dolphie.main_db_connection.fetch_value_from_field(
                        MySQLQueries.group_replication_get_write_concurrency,
                        "write_concurrency",
                        ignore_error=True,
                    )
                    if write_concurrency is not None:
                        dolphie.group_replication_data["write_concurrency"] = coerce_int(write_concurrency)
                    else:
                        dolphie.group_replication_data.pop("write_concurrency", None)

                dolphie.main_db_connection.execute(MySQLQueries.get_group_replication_members)
                dolphie.group_replication_members = dolphie.main_db_connection.fetchall()
                dolphie.is_group_replication_primary = is_group_replication_primary(
                    dolphie.group_replication_members,
                    dolphie.server_uuid,
                )
            else:
                dolphie.group_replication_members = []
                dolphie.is_group_replication_primary = False

            if dolphie.is_mysql_version_at_least("5.7"):
                dolphie.metadata_locks = []
                if dolphie.metadata_locks_enabled and dolphie.panels.metadata_locks.visible:
                    dolphie.metadata_locks = MetadataLocksPanel.fetch_data(tab)

                if dolphie.panels.ddl.visible:
                    dolphie.main_db_connection.execute(MySQLQueries.ddls)
                    dolphie.ddl = dolphie.main_db_connection.fetchall()

                if dolphie.panels.pfs_metrics.visible:
                    # Reset the PFS metrics deltas if we're in daemon mode and it's been 10 minutes since the last reset
                    # This is to keep a realistic point-in-time view of the metrics
                    last_reset_time = dolphie.pfs_metrics_last_reset_time
                    if (
                        dolphie.daemon_mode
                        and last_reset_time is not None
                        and datetime.now().astimezone() - last_reset_time >= timedelta(minutes=10)
                    ):
                        dolphie.reset_pfs_metrics_deltas()

                    dolphie.main_db_connection.execute(MySQLQueries.file_summary_by_instance)
                    file_io_data = dolphie.main_db_connection.fetchall()
                    if not dolphie.file_io_data:
                        dolphie.file_io_data = PerformanceSchemaMetrics(file_io_data, "file_io", "FILE_NAME")
                    else:
                        dolphie.file_io_data.update_internal_data(file_io_data)

                    dolphie.main_db_connection.execute(MySQLQueries.table_io_waits_summary_by_table)
                    table_io_waits_data = dolphie.main_db_connection.fetchall()
                    if not dolphie.table_io_waits_data:
                        dolphie.table_io_waits_data = PerformanceSchemaMetrics(
                            table_io_waits_data, "table_io", "OBJECT_TABLE"
                        )
                    else:
                        dolphie.table_io_waits_data.update_internal_data(table_io_waits_data)

                if dolphie.panels.statements_summary.visible:
                    if (
                        dolphie.is_mysql_version_at_least("8.0")
                        and dolphie.connection_source_alt != ConnectionSource.mariadb
                    ):
                        dolphie.main_db_connection.execute(MySQLQueries.table_statements_summary_by_digest_80)
                    else:
                        dolphie.main_db_connection.execute(MySQLQueries.table_statements_summary_by_digest)

                    statements_summary_data = dolphie.main_db_connection.fetchall()
                    if not dolphie.statements_summary_data:
                        dolphie.statements_summary_data = PerformanceSchemaMetrics(
                            statements_summary_data, "statements_summary", "digest"
                        )
                    else:
                        dolphie.statements_summary_data.update_internal_data(statements_summary_data)

    def process_proxysql_data(self, tab: "Tab"):
        """Process ProxySQL data for a given tab."""
        dolphie = tab.dolphie

        global_variables = dolphie.main_db_connection.fetch_status_and_variables("variables")
        self.monitor_global_variable_change(tab=tab, old_data=dolphie.global_variables, new_data=global_variables)
        dolphie.global_variables = global_variables

        if dolphie.connection_status == ConnectionStatus.connecting:
            # Called from worker thread, use call_from_thread
            self.app.call_from_thread(
                self.app.tab_manager.update_connection_status, tab=tab, connection_status=ConnectionStatus.connected
            )
            dolphie.host_version = dolphie.parse_server_version(
                coerce_str(dolphie.global_variables.get("admin-version"))
            )

        global_status = dolphie.main_db_connection.fetch_status_and_variables("mysql_stats")
        self.monitor_uptime_change(
            tab=tab,
            old_uptime=coerce_int(dolphie.global_status.get("ProxySQL_Uptime")),
            new_uptime=coerce_int(global_status.get("ProxySQL_Uptime")),
        )
        dolphie.global_status = dict(global_status)

        dolphie.main_db_connection.execute(ProxySQLQueries.command_stats)
        # stats_mysql_commands_counters is Command (str) plus bigint counters, so this
        # is always int/str even though fetchall() is typed as the generic DatabaseRow.
        dolphie.proxysql_command_stats = cast(list[dict[str, int | str]], dolphie.main_db_connection.fetchall())

        # Here, we're going to format the command stats to match the global status keys of
        # MySQL and get total count of queries
        total_queries_count = 0
        query_types_for_total = ["SELECT", "INSERT", "UPDATE", "DELETE", "REPLACE", "SET", "CALL"]
        for row in dolphie.proxysql_command_stats:
            total_cnt = 0
            command = coerce_str(row.get("Command"))
            if command in query_types_for_total:
                total_cnt = coerce_int(row.get("Total_cnt"))
                total_queries_count += total_cnt

            dolphie.global_status[f"Com_{command.lower()}"] = total_cnt

        # Add the total queries to the global status
        dolphie.global_status["Queries"] = total_queries_count

        dolphie.main_db_connection.execute(ProxySQLQueries.connection_pool_data)
        data = dolphie.main_db_connection.fetchone()

        client_connections = coerce_float(dolphie.global_status.get("Client_Connections_connected"))
        if client_connections > 0:
            dolphie.global_status["proxysql_multiplex_efficiency_ratio"] = round(
                100 - ((coerce_float(data.get("connection_pool_connections")) / client_connections) * 100),
                2,
            )
        else:
            dolphie.global_status["proxysql_multiplex_efficiency_ratio"] = 100

        if dolphie.panels.proxysql_hostgroup_summary.visible:
            dolphie.main_db_connection.execute(ProxySQLQueries.hostgroup_summary)

            previous_values = {}
            columns_to_calculate_per_sec = ["Queries", "Bytes_data_sent", "Bytes_data_recv"]

            # Store previous values for each row
            for row in dolphie.proxysql_hostgroup_summary:
                row_id = (
                    f"{coerce_str(row.get('hostgroup'))}_"
                    f"{coerce_str(row.get('srv_host'))}_{coerce_str(row.get('srv_port'))}"
                )

                for column_key in columns_to_calculate_per_sec:
                    previous_values.setdefault(row_id, {})[column_key] = coerce_int(row.get(column_key))

            # Fetch the updated hostgroup summary
            hostgroup_summary = dolphie.main_db_connection.fetchall()
            dolphie.proxysql_hostgroup_summary = hostgroup_summary

            # Calculate the values per second
            for row in hostgroup_summary:
                row_id = (
                    f"{coerce_str(row.get('hostgroup'))}_"
                    f"{coerce_str(row.get('srv_host'))}_{coerce_str(row.get('srv_port'))}"
                )

                if row_id in previous_values:  # Ensure we have previous values for this row_id
                    for column_key in columns_to_calculate_per_sec:
                        previous_value = previous_values[row_id].get(column_key, 0)
                        current_value = coerce_int(row.get(column_key))

                        value_per_sec = (
                            (current_value - previous_value) / dolphie.polling_latency
                            if dolphie.polling_latency > 0
                            else 0
                        )
                        row[f"{column_key}_per_sec"] = round(value_per_sec)

        if dolphie.panels.processlist.visible:
            dolphie.processlist_threads = ProxySQLProcesslistPanel.fetch_data(tab)

        if dolphie.panels.proxysql_mysql_query_rules.visible:
            dolphie.main_db_connection.execute(ProxySQLQueries.query_rules_summary)
            dolphie.proxysql_mysql_query_rules = dolphie.main_db_connection.fetchall()

    def refresh_screen(self, tab: "Tab"):
        """Refresh the screen for a given tab, regardless of connection source."""
        dolphie = tab.dolphie

        if tab.loading_indicator.display:
            tab.loading_indicator.display = False

        # Loop each panel and refresh it
        for panel in dolphie.panels.get_all_panels():
            if panel.visible:
                # Skip the graphs panel since it's handled separately
                if panel.name == dolphie.panels.graphs.name:
                    continue

                self.app.refresh_panel(tab, panel.name)

                if panel.name == dolphie.panels.dashboard.name:
                    query_values = dolphie.metric_manager.metrics.dml.Queries.values_snapshot()
                    if query_values:
                        # Update the sparkline for queries per second
                        tab.sparkline.data = query_values
                        tab.sparkline.refresh()

        # Refresh the shared graph dashboard from this host's latest poll.
        if tab.panel_graphs.display:
            tab.graph_dashboard.bind_host(dolphie)

        tab.refresh_replay_dashboard_section()

        # We take a snapshot of the processlist to be used for commands
        # since the data can change after a key is pressed
        if not dolphie.daemon_mode:
            dolphie.processlist_threads_snapshot = dolphie.processlist_threads.copy()

    def monitor_global_variable_change(
        self,
        tab: "Tab",
        old_data: Mapping[str, DatabaseScalar],
        new_data: Mapping[str, DatabaseScalar],
    ):
        """Monitor and notify about global variable changes."""
        if not old_data:
            return

        dolphie = tab.dolphie

        # gtid is always changing so we don't want to alert on that
        # The others are ones I've found to be spammy due to monitoring tools changing them
        exclude_variables = {"gtid", "innodb_thread_sleep_delay"}

        # Add to exclude_variables with user specified variables
        if dolphie.exclude_notify_global_vars:
            exclude_variables.update(v.lower() for v in dolphie.exclude_notify_global_vars)

        for variable, new_value in new_data.items():
            if any(item in variable for item in exclude_variables):
                continue

            old_value = old_data.get(variable)
            if old_value != new_value:
                replay_manager = tab.replay_manager
                if replay_manager is not None:
                    replay_manager.capture_global_variable_change(variable, old_value, new_value)

                # read_only notification/log message is handled by monitor_read_only_change()
                if variable == "read_only":
                    continue

                logger.info(f"Global variable {variable} changed: {old_value} -> {new_value}")

                # Skip UI notifications in daemon mode since the TUI is headless
                if dolphie.daemon_mode:
                    continue

                # If the tab is not active, include the host in the notification
                include_host = ""
                active_tab = self.app.tab_manager.active_tab
                if active_tab is None or active_tab.id != tab.id:
                    include_host = f"Host:      [$light_blue]{dolphie.host_with_port}[/$light_blue]\n"
                self.app.notify(
                    f"[b][$dark_yellow]{variable}[/b][/$dark_yellow]\n"
                    f"{include_host}"
                    f"Old Value: [$highlight]{old_value}[/$highlight]\n"
                    f"New Value: [$highlight]{new_value}[/$highlight]",
                    title="Global Variable Change",
                    severity="warning",
                    timeout=15,
                )

    def monitor_uptime_change(self, tab: "Tab", old_uptime: int, new_uptime: int):
        """Monitor and handle uptime changes (e.g., server restarts)."""
        if old_uptime > new_uptime:
            formatted_old_uptime = str(timedelta(seconds=old_uptime))
            formatted_new_uptime = str(timedelta(seconds=new_uptime))

            replay_manager = tab.replay_manager
            if replay_manager is not None:
                replay_manager.capture_global_variable_change("Uptime", formatted_old_uptime, formatted_new_uptime)

            logger.info(f"Uptime changed: {formatted_old_uptime} -> {formatted_new_uptime}")

            # Reset data for Performance Schema metrics since those tables are reset on server restart
            tab.dolphie.reset_pfs_metrics_deltas(reset_fully=True)

    def monitor_read_only_change(self, tab: "Tab"):
        """Monitor and notify about read-only status changes."""
        dolphie = tab.dolphie

        if dolphie.connection_source == ConnectionSource.proxysql:
            return

        current_ro_status = dolphie.global_variables.get("read_only")
        formatted_ro_status = ConnectionStatus.read_only if current_ro_status == "ON" else ConnectionStatus.read_write

        if (
            dolphie.connection_status in (ConnectionStatus.read_write, ConnectionStatus.read_only)
            and dolphie.connection_status != formatted_ro_status
        ):
            status = "read-only" if current_ro_status == "ON" else "read/write"
            message = (
                f"Host [$light_blue]{dolphie.host_with_port}[/$light_blue] is now [$b_highlight]{status}[/$b_highlight]"
            )

            is_group_member = dolphie.group_replication or dolphie.innodb_cluster
            is_clusterset_replica_cluster = dolphie.group_replication_data.get("clusterset_role") == "REPLICA"
            unexpectedly_read_only = (not dolphie.replication_status and not is_group_member) or (
                is_group_member and dolphie.is_group_replication_primary and not is_clusterset_replica_cluster
            )

            # Standalone hosts and writable cluster primaries should not be read-only.
            if current_ro_status == "ON" and unexpectedly_read_only:
                message += " ([$dark_yellow]SHOULD BE READ/WRITE?[/$dark_yellow])"

            logger.warning(f"Read-only mode changed: {dolphie.connection_status} -> {formatted_ro_status}")
            self.app.notify(title="Read-only mode change", message=message, severity="warning", timeout=15)

            self.app.tab_manager.update_connection_status(tab=tab, connection_status=formatted_ro_status)
        elif dolphie.connection_status == ConnectionStatus.connected:
            self.app.tab_manager.update_connection_status(tab=tab, connection_status=formatted_ro_status)
