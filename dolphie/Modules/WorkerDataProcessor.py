from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from loguru import logger

from dolphie.DataTypes import ConnectionSource, ConnectionStatus
from dolphie.Modules.PerformanceSchemaMetrics import PerformanceSchemaMetrics
from dolphie.Modules.Queries import MySQLQueries, ProxySQLQueries
from dolphie.Panels import MetadataLocks as MetadataLocksPanel
from dolphie.Panels import Processlist as ProcesslistPanel
from dolphie.Panels import ProxySQLProcesslist as ProxySQLProcesslistPanel
from dolphie.Panels import Replication as ReplicationPanel

if TYPE_CHECKING:
    from dolphie.App import DolphieApp
    from dolphie.Modules.TabManager import Tab


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

    def process_mysql_data(self, tab: "Tab"):
        """Process MySQL data for a given tab."""
        dolphie = tab.dolphie

        global_variables = dolphie.main_db_connection.fetch_status_and_variables("variables")
        self.monitor_global_variable_change(tab=tab, old_data=dolphie.global_variables, new_data=global_variables)
        dolphie.global_variables = global_variables

        # At this point, we're connected so we need to do a few things
        if dolphie.connection_status == ConnectionStatus.connecting:
            self.app.tab_manager.update_connection_status(tab=tab, connection_status=ConnectionStatus.connected)
            dolphie.host_version = dolphie.parse_server_version(dolphie.global_variables.get("version"))
            dolphie.get_group_replication_metadata()
            dolphie.configure_mysql_variables()
            dolphie.validate_metadata_locks_enabled()

        global_status = dolphie.main_db_connection.fetch_status_and_variables("status")
        self.monitor_uptime_change(
            tab=tab,
            old_uptime=dolphie.global_status.get("Uptime", 0),
            new_uptime=global_status.get("Uptime", 0),
        )
        dolphie.global_status = global_status
        # If the server doesn't support Innodb_lsn_current, use Innodb_os_log_written instead
        # which has less precision, but it's good enough. Used for calculating the percentage of redo log used
        if not dolphie.global_status.get("Innodb_lsn_current"):
            dolphie.global_status["Innodb_lsn_current"] = dolphie.global_status.get("Innodb_os_log_written")

        dolphie.innodb_metrics = dolphie.main_db_connection.fetch_status_and_variables("innodb_metrics")
        dolphie.replication_status = ReplicationPanel.fetch_replication_data(tab)

        # Manage our replicas
        if dolphie.performance_schema_enabled and dolphie.is_mysql_version_at_least("5.7"):
            if dolphie.connection_source_alt == ConnectionSource.mariadb:
                find_replicas_query = MySQLQueries.mariadb_find_replicas
            else:
                find_replicas_query = MySQLQueries.ps_find_replicas
        else:
            find_replicas_query = MySQLQueries.pl_find_replicas

        dolphie.main_db_connection.execute(find_replicas_query)
        available_replicas = dolphie.main_db_connection.fetchall()
        if not dolphie.daemon_mode:
            # We update the replica ports used if the number of replicas have changed
            if len(available_replicas) != len(dolphie.replica_manager.available_replicas):
                query = (
                    MySQLQueries.show_replicas
                    if dolphie.is_mysql_version_at_least("8.0.22")
                    and dolphie.connection_source_alt != ConnectionSource.mariadb
                    else MySQLQueries.show_slave_hosts
                )
                dolphie.main_db_connection.execute(query)
                ports_replica_data = dolphie.main_db_connection.fetchall()

                # Reset the ports dictionary and start fresh
                dolphie.replica_manager.ports = {}
                for row in ports_replica_data:
                    if dolphie.connection_source_alt == ConnectionSource.mariadb:
                        key = "Server_id"
                    else:
                        key = "Replica_UUID" if dolphie.is_mysql_version_at_least("8.0.22") else "Slave_UUID"

                    dolphie.replica_manager.ports[row.get(key)] = {
                        "port": row.get("Port"),
                        "in_use": False,
                    }

                # Update the port value for each replica from an existing replica so our row_key can be properly used
                # to manage replica_manager.replicas
                # MariaDB cannot correlate thread_id to a port so it will just have to reconnect
                # each time number of replicas change
                if dolphie.connection_source_alt != ConnectionSource.mariadb:
                    existing_replicas_map = {
                        replica["id"]: replica for replica in dolphie.replica_manager.available_replicas
                    }
                    for replica in available_replicas:
                        if replica["id"] in existing_replicas_map:
                            replica["port"] = existing_replicas_map[replica["id"]].get("port")

                dolphie.replica_manager.available_replicas = available_replicas
        else:
            dolphie.replica_manager.available_replicas = available_replicas

        if dolphie.is_mysql_version_at_least("8.2.0") and dolphie.connection_source_alt != ConnectionSource.mariadb:
            dolphie.main_db_connection.execute(MySQLQueries.show_binary_log_status)
        else:
            dolphie.main_db_connection.execute(MySQLQueries.show_master_status)

        previous_position = dolphie.binlog_status.get("Position")
        dolphie.binlog_status = dolphie.main_db_connection.fetchone()

        if previous_position is None:
            dolphie.binlog_status["Diff_Position"] = 0
        elif previous_position > dolphie.binlog_status["Position"]:
            dolphie.binlog_status["Diff_Position"] = "Binlog Rotated"
        else:
            dolphie.binlog_status["Diff_Position"] = dolphie.binlog_status["Position"] - previous_position

        if dolphie.panels.processlist.visible:
            dolphie.processlist_threads = ProcesslistPanel.fetch_data(tab)

        if dolphie.panels.replication.visible and (dolphie.innodb_cluster or dolphie.innodb_cluster_read_replica):
            dolphie.main_db_connection.execute(MySQLQueries.get_clustersets)
            dolphie.innodb_cluster_clustersets = dolphie.main_db_connection.fetchall()

        if dolphie.performance_schema_enabled:
            dolphie.main_db_connection.execute(MySQLQueries.ps_disk_io)
            dolphie.disk_io_metrics = dolphie.main_db_connection.fetchone()

            if (
                dolphie.is_mysql_version_at_least("8.0")
                and dolphie.replication_status
                and dolphie.panels.replication.visible
                and dolphie.global_variables.get("replica_parallel_workers", 0) > 1
            ):
                dolphie.main_db_connection.execute(MySQLQueries.replication_applier_status)
                dolphie.replication_applier_status["data"] = dolphie.main_db_connection.fetchall()

                # Calculate the difference in total_thread_events for each worker + all workers
                for row in dolphie.replication_applier_status["data"]:
                    total_thread_events = row["total_thread_events"]
                    thread_id = row.get("thread_id")

                    # Handle the ROLLUP row, which contains the total for all threads
                    if not thread_id:
                        dolphie.replication_applier_status["diff_all"] = (
                            total_thread_events
                            - dolphie.replication_applier_status.get("previous_all", total_thread_events)
                        )
                        dolphie.replication_applier_status["previous_all"] = total_thread_events
                        continue

                    dolphie.replication_applier_status[f"diff_{thread_id}"] = (
                        total_thread_events
                        - dolphie.replication_applier_status.get(f"previous_{thread_id}", total_thread_events)
                    )
                    dolphie.replication_applier_status[f"previous_{thread_id}"] = total_thread_events

            if (
                not dolphie.daemon_mode
                and dolphie.is_mysql_version_at_least("8.0.30")
                and dolphie.connection_source_alt != ConnectionSource.mariadb
            ):
                active_redo_logs_count = dolphie.main_db_connection.fetch_value_from_field(
                    MySQLQueries.active_redo_logs, "count"
                )
                dolphie.global_status["Active_redo_log_count"] = active_redo_logs_count

            if dolphie.group_replication or dolphie.innodb_cluster:
                if dolphie.is_mysql_version_at_least("8.0.13"):
                    dolphie.group_replication_data["write_concurrency"] = (
                        dolphie.main_db_connection.fetch_value_from_field(
                            MySQLQueries.group_replication_get_write_concurrency,
                            "write_concurrency",
                        )
                    )

                dolphie.main_db_connection.execute(MySQLQueries.get_group_replication_members)
                dolphie.group_replication_members = dolphie.main_db_connection.fetchall()

            if dolphie.is_mysql_version_at_least("5.7"):
                dolphie.metadata_locks = {}
                if dolphie.metadata_locks_enabled and dolphie.panels.metadata_locks.visible:
                    dolphie.metadata_locks = MetadataLocksPanel.fetch_data(tab)

                if dolphie.panels.ddl.visible:
                    dolphie.main_db_connection.execute(MySQLQueries.ddls)
                    dolphie.ddl = dolphie.main_db_connection.fetchall()

                if dolphie.panels.pfs_metrics.visible:
                    # Reset the PFS metrics deltas if we're in daemon mode and it's been 10 minutes since the last reset
                    # This is to keep a realistic point-in-time view of the metrics
                    time_since_reset = datetime.now().astimezone() - dolphie.pfs_metrics_last_reset_time
                    if dolphie.daemon_mode and time_since_reset >= timedelta(minutes=10):
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
            self.app.tab_manager.update_connection_status(tab=tab, connection_status=ConnectionStatus.connected)
            dolphie.host_version = dolphie.parse_server_version(dolphie.global_variables.get("admin-version"))

        global_status = dolphie.main_db_connection.fetch_status_and_variables("mysql_stats")
        self.monitor_uptime_change(
            tab=tab,
            old_uptime=dolphie.global_status.get("ProxySQL_Uptime", 0),
            new_uptime=global_status.get("ProxySQL_Uptime", 0),
        )
        dolphie.global_status = global_status

        dolphie.main_db_connection.execute(ProxySQLQueries.command_stats)
        dolphie.proxysql_command_stats = dolphie.main_db_connection.fetchall()

        # Here, we're going to format the command stats to match the global status keys of
        # MySQL and get total count of queries
        total_queries_count = 0
        query_types_for_total = [
            "SELECT",
            "INSERT",
            "UPDATE",
            "DELETE",
            "REPLACE",
            "SET",
            "CALL",
        ]
        for row in dolphie.proxysql_command_stats:
            total_cnt = 0
            if row["Command"] in query_types_for_total:
                total_cnt = int(row["Total_cnt"])
                total_queries_count += total_cnt

            dolphie.global_status[f"Com_{row['Command'].lower()}"] = total_cnt

        # Add the total queries to the global status
        dolphie.global_status["Queries"] = total_queries_count

        dolphie.main_db_connection.execute(ProxySQLQueries.connection_pool_data)
        data = dolphie.main_db_connection.fetchone()

        if dolphie.global_status.get("Client_Connections_connected", 0):
            dolphie.global_status["proxysql_multiplex_efficiency_ratio"] = round(
                100
                - (
                    (
                        int(data.get("connection_pool_connections", 0))
                        / dolphie.global_status.get("Client_Connections_connected", 0)
                    )
                    * 100
                ),
                2,
            )
        else:
            dolphie.global_status["proxysql_multiplex_efficiency_ratio"] = 100

        if dolphie.panels.proxysql_hostgroup_summary.visible:
            dolphie.main_db_connection.execute(ProxySQLQueries.hostgroup_summary)

            previous_values = {}
            columns_to_calculate_per_sec = [
                "Queries",
                "Bytes_data_sent",
                "Bytes_data_recv",
            ]

            # Store previous values for each row
            for row in dolphie.proxysql_hostgroup_summary:
                row_id = f"{row['hostgroup']}_{row['srv_host']}_{row['srv_port']}"

                for column_key in columns_to_calculate_per_sec:
                    previous_values.setdefault(row_id, {})[column_key] = int(row.get(column_key, 0))

            # Fetch the updated hostgroup summary
            dolphie.proxysql_hostgroup_summary = dolphie.main_db_connection.fetchall()

            # Calculate the values per second
            for row in dolphie.proxysql_hostgroup_summary:
                row_id = f"{row['hostgroup']}_{row['srv_host']}_{row['srv_port']}"

                if row_id in previous_values:  # Ensure we have previous values for this row_id
                    for column_key in columns_to_calculate_per_sec:
                        previous_value = previous_values[row_id].get(column_key, 0)
                        current_value = int(row.get(column_key, 0))

                        value_per_sec = (current_value - previous_value) / dolphie.polling_latency
                        row[f"{column_key}_per_sec"] = round(value_per_sec)

        if dolphie.panels.processlist.visible:
            dolphie.processlist_threads = ProxySQLProcesslistPanel.fetch_data(tab)

        if dolphie.panels.proxysql_mysql_query_rules.visible:
            dolphie.main_db_connection.execute(ProxySQLQueries.query_rules_summary)
            dolphie.proxysql_mysql_query_rules = dolphie.main_db_connection.fetchall()

    def refresh_screen_proxysql(self, tab: "Tab"):
        """Refresh the ProxySQL screen for a given tab."""
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

                if panel.name == dolphie.panels.dashboard.name and dolphie.metric_manager.metrics.dml.Queries.values:
                    # Update the sparkline for queries per second
                    tab.sparkline.data = dolphie.metric_manager.metrics.dml.Queries.values
                    tab.sparkline.refresh()

        # Refresh the graph(s) for the selected tab
        self.app.update_graphs(tab.metric_graph_tabs.get_pane(tab.metric_graph_tabs.active).name)

        tab.refresh_replay_dashboard_section()

        # We take a snapshot of the processlist to be used for commands
        # since the data can change after a key is pressed
        if not dolphie.daemon_mode:
            dolphie.processlist_threads_snapshot = dolphie.processlist_threads.copy()

    def refresh_screen_mysql(self, tab: "Tab"):
        """Refresh the MySQL screen for a given tab."""
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

                if panel.name == dolphie.panels.dashboard.name and dolphie.metric_manager.metrics.dml.Queries.values:
                    # Update the sparkline for queries per second
                    tab.sparkline.data = dolphie.metric_manager.metrics.dml.Queries.values
                    tab.sparkline.refresh()

        # Refresh the graph(s) for the selected tab
        self.app.update_graphs(tab.metric_graph_tabs.get_pane(tab.metric_graph_tabs.active).name)

        tab.refresh_replay_dashboard_section()

        # We take a snapshot of the processlist to be used for commands
        # since the data can change after a key is pressed
        if not dolphie.daemon_mode:
            dolphie.processlist_threads_snapshot = dolphie.processlist_threads.copy()

    def monitor_global_variable_change(self, tab: "Tab", old_data: dict, new_data: dict):
        """Monitor and notify about global variable changes."""
        if not old_data:
            return

        dolphie = tab.dolphie

        # gtid is always changing so we don't want to alert on that
        # The others are ones I've found to be spammy due to monitoring tools changing them
        exclude_variables = {"gtid", "innodb_thread_sleep_delay"}

        # Add to exclude_variables with user specified variables
        if dolphie.exclude_notify_global_vars:
            exclude_variables.update(dolphie.exclude_notify_global_vars)

        for variable, new_value in new_data.items():
            if any(item in variable.lower() for item in exclude_variables):
                continue

            old_value = old_data.get(variable)
            if old_value != new_value:
                tab.replay_manager.capture_global_variable_change(variable, old_value, new_value)

                # read_only notification/log message is handled by monitor_read_only_change()
                if variable == "read_only":
                    continue

                logger.info(f"Global variable {variable} changed: {old_value} -> {new_value}")

                # If the tab is not active, include the host in the notification
                include_host = ""
                if self.app.tab_manager.active_tab.id != tab.id:
                    include_host = f"Host:      [$light_blue]{dolphie.host_with_port}[/$light_blue]\n"
                self.app.app.notify(
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

            tab.replay_manager.capture_global_variable_change("Uptime", formatted_old_uptime, formatted_new_uptime)

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
        status = "read-only" if current_ro_status == "ON" else "read/write"

        message = (
            f"Host [$light_blue]{dolphie.host_with_port}[/$light_blue] is now [$b_highlight]{status}[/$b_highlight]"
        )

        if (
            current_ro_status == "ON"
            and not dolphie.replication_status
            and not dolphie.group_replication
            or current_ro_status == "ON"
            and dolphie.group_replication
            and dolphie.is_group_replication_primary
        ):
            message += " ([$dark_yellow]SHOULD BE READ/WRITE?[/$dark_yellow])"

        if (
            dolphie.connection_status in [ConnectionStatus.read_write, ConnectionStatus.read_only]
            and dolphie.connection_status != formatted_ro_status
        ):
            logger.warning(f"Read-only mode changed: {dolphie.connection_status} -> {formatted_ro_status}")
            self.app.app.notify(
                title="Read-only mode change",
                message=message,
                severity="warning",
                timeout=15,
            )

            self.app.tab_manager.update_connection_status(tab=tab, connection_status=formatted_ro_status)
        elif dolphie.connection_status == ConnectionStatus.connected:
            self.app.tab_manager.update_connection_status(tab=tab, connection_status=formatted_ro_status)

        dolphie.connection_status = formatted_ro_status
