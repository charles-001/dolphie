from __future__ import annotations

import ipaddress
import os
import socket
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any

import psutil
from loguru import logger
from packaging.version import InvalidVersion
from packaging.version import parse as parse_version

import dolphie.DataTypes as DataTypes
import dolphie.Modules.MetricManager as MetricManager
from dolphie.Modules.ArgumentParser import Config
from dolphie.Modules.Functions import coerce_int, coerce_str, load_host_cache_file
from dolphie.Modules.MySQL import ConnectionSource, Database
from dolphie.Modules.PerformanceSchemaMetrics import PerformanceSchemaMetrics
from dolphie.Modules.Queries import MySQLQueries
from dolphie.Modules.Theme import themed_content

if TYPE_CHECKING:
    from dolphie.App import DolphieApp


class Dolphie:
    def __init__(self, config: Config, app: DolphieApp) -> None:
        self.config = config
        self.app = app
        self.app_version = config.app_version

        # Config options
        self.credential_profile = config.credential_profile
        self.user = config.user
        self.password = config.password
        self.host = config.host
        self.port = config.port
        self.socket = config.socket
        self.ssl = config.ssl
        self.host_cache_file = config.host_cache_file
        self.tab_setup_file = config.tab_setup_file
        self.refresh_interval: float = config.refresh_interval
        self.graph_window_minutes = config.graph_window_minutes
        self.show_trxs_only = config.show_trxs_only
        self.show_threads_with_concurrency_tickets = False
        self.show_additional_query_columns = config.show_additional_query_columns
        self.show_statements_summary_query_digest_text_sample = False
        self.heartbeat_table = config.heartbeat_table
        self.tab_setup_available_hosts = config.tab_setup_available_hosts
        self.startup_panels = config.startup_panels
        self.graph_marker = config.graph_marker
        self.hostgroup = config.hostgroup
        self.hostgroup_hosts = config.hostgroup_hosts
        self.record_for_replay = config.record_for_replay
        self.daemon_mode = config.daemon_mode
        self.daemon_mode_panels = config.daemon_mode_panels
        self.replay_file = config.replay_file  # This denotes that we're replaying a file
        self.replay_dir = config.replay_dir
        self.replay_retention_hours = config.replay_retention_hours
        self.exclude_notify_global_vars = config.exclude_notify_global_vars

        # Set the default panels based on startup_panels to be visible
        self.panels = DataTypes.Panels()

        panels_to_show = self.daemon_mode_panels if self.daemon_mode else self.startup_panels
        for panel in panels_to_show:
            if panel in self.panels.all():
                getattr(self.panels, panel).visible = True

        self.show_idle_threads: bool = False
        self.sort_by_time_descending: bool = True

        self.reset_runtime_variables()

    def reset_runtime_variables(self):
        visibility = self.metric_manager.snapshot_visibility() if hasattr(self, "metric_manager") else {}
        self.metric_manager = MetricManager.MetricManager(
            self.replay_file,
            rolling_window_minutes=self.graph_window_minutes,
        )
        self.metric_manager.restore_visibility(visibility)
        self.replica_manager = DataTypes.ReplicaManager()

        self.dolphie_start_time: datetime = datetime.now().astimezone()
        self.worker_previous_start_time: datetime = datetime.now().astimezone()
        self.worker_processing_time: float = 0
        self.polling_latency: float = 0
        self.connection_status: DataTypes.ConnectionStatusType | None = None

        self.global_variables: dict[str, int | str] = {}
        self.global_status: dict[str, int | float | str] = {}
        self.binlog_status: DataTypes.DatabaseRow = {}
        self.replication_status: list[DataTypes.DatabaseRow] = []
        self.replication_applier_status: dict[str, dict[str, Any]] = {}
        self.innodb_metrics: dict[str, int | str] = {}
        self.metadata_locks: list[DataTypes.DatabaseRow] = []
        self.ddl: list[DataTypes.DatabaseRow] = []
        self.disk_io_metrics: dict[str, int | str] = {}
        self.statements_summary_metrics: dict[str, int | str] = {}
        self.system_utilization: dict[str, int | float | tuple[float, float, float]] = {}
        self.host_cache: dict[str, str] = {}
        self.proxysql_hostgroup_summary: list[DataTypes.DatabaseRow] = []
        self.proxysql_mysql_query_rules: list[DataTypes.DatabaseRow] = []
        self.proxysql_per_second_data: dict[str, dict[str, int]] = {}
        self.proxysql_command_stats: list[dict[str, int | str]] = []
        self.processlist_threads: dict[int, DataTypes.ProcesslistThread | DataTypes.ProxySQLProcesslistThread] = {}
        self.processlist_threads_snapshot: dict[
            int, DataTypes.ProcesslistThread | DataTypes.ProxySQLProcesslistThread
        ] = {}

        # These are for group replication in replication panel
        self.is_group_replication_primary: bool = False
        self.group_replication_data: DataTypes.DatabaseRow = {}
        self.group_replication_members: list[DataTypes.DatabaseRow] = []
        self.clusterset_instances: list[DataTypes.DatabaseRow] = []

        self.galera_cluster_members: list[DataTypes.DatabaseRow] = []

        # Filters that can be applied. String filters support a leading ! to exclude matches.
        # They start as whatever the filters option is set to, if anything
        filters = self.config.filter_values
        self.user_filter: str | None = filters.get("user")
        self.db_filter: str | None = filters.get("db")
        self.host_filter: str | None = filters.get("host")
        self.query_filter: str | None = filters.get("query")
        self.hostgroup_filter: str | None = filters.get("hostgroup")
        self.query_time_filter: int | None = filters.get("time")

        # Values seen in the processlist, so the filter dropdowns can offer ones being filtered out
        self.filter_dropdown_values: dict[str, set] = {field: set() for field in ("user", "db", "host", "hostgroup")}

        # Types of hosts
        self.connection_source: DataTypes.ConnectionSourceType = ConnectionSource.mysql  # mysql, proxysql
        self.connection_source_alt: DataTypes.ConnectionSourceType = ConnectionSource.mysql  # mariadb
        self.galera_cluster: bool = False
        self.group_replication: bool = False
        self.innodb_cluster: bool = False
        self.innodb_cluster_read_replica: bool = False
        self.replicaset: bool = False

        # Main connection is used for Textual's worker thread so it can run asynchronous
        self.main_db_connection = self._create_connection()
        # Secondary connection is for ad-hoc commands that are not a part of the worker thread
        self.secondary_db_connection = self._create_connection(save_connection_id=False)

        # Misc variables
        self.host_distro: str = "MySQL"
        self.host_with_port: str = f"{self.host}:{self.port}"
        self.performance_schema_enabled: bool = False
        self.use_performance_schema_for_processlist: bool = False
        self.server_uuid: str | int | None = None
        self.replication_source_uuids: set[str] = set()
        self.host_version: str | None = None
        self.pause_refresh: bool = False
        self.active_redo_logs: int | None = None
        self.metadata_locks_enabled: bool = False

        self.host_cache_from_file = load_host_cache_file(self.host_cache_file)

        self.file_io_data: PerformanceSchemaMetrics | None = None
        self.table_io_waits_data: PerformanceSchemaMetrics | None = None
        self.statements_summary_data: PerformanceSchemaMetrics | None = None

        if self.record_for_replay or self.panels.pfs_metrics.visible:
            self.pfs_metrics_last_reset_time: datetime | None = datetime.now().astimezone()
        else:
            # This will be set when user presses key to bring up panel
            self.pfs_metrics_last_reset_time = None

        try:
            # Get the IP address of the monitored host
            monitored_ip = socket.gethostbyname(self.host)

            # Enable system metrics if using a socket file or if monitored host is localhost
            if self.socket or monitored_ip == "127.0.0.1" or monitored_ip == socket.gethostbyname(socket.gethostname()):
                self.enable_system_utilization = True
            else:
                self.enable_system_utilization = False
        except socket.gaierror:
            self.enable_system_utilization = False

    def _create_connection(self, save_connection_id: bool = True) -> Database:
        return Database(
            app=self.app,
            host=self.host,
            user=self.user,
            password=self.password,
            socket=self.socket,
            port=self.port,
            ssl=self.ssl,
            save_connection_id=save_connection_id,
            auto_connect=False,
            daemon_mode=self.daemon_mode,
        )

    def db_connect(self):
        self.main_db_connection.connect()
        if not self.daemon_mode:
            self.secondary_db_connection.connect()

        connection_source = self.main_db_connection.source
        if connection_source is None:
            raise RuntimeError("Database connection did not report a connection source")
        self.connection_source = connection_source
        self.connection_source_alt = self.connection_source
        if self.connection_source == ConnectionSource.proxysql:
            self.host_distro = ConnectionSource.proxysql
            self.host_with_port = f"{self.host}:{self.port}"

        self.metric_manager.connection_source = self.connection_source

        # Add host to tab setup file if it doesn't exist
        self.add_host_to_tab_setup_file()

    def configure_mysql_variables(self):
        global_variables = self.global_variables

        # Galera cluster check
        self.galera_cluster = global_variables.get("wsrep_on") == "ON" or bool(
            global_variables.get("wsrep_cluster_address")
        )

        self.host_distro, self.connection_source_alt = self.determine_distro_and_connection_source_alt(global_variables)

        # For RDS and Azure, we will use the host specified to connect with since hostname isn't related to the endpoint
        if ".rds.amazonaws.com" in self.host:
            self.host_with_port = f"{self.host.split('.rds.amazonaws.com')[0]}:{self.port}"
        elif ".mysql.database.azure.com" in self.host:
            self.host_with_port = f"{self.host.split('.mysql.database.azure.com')[0]}:{self.port}"
        else:
            mysql_hostname = global_variables.get("hostname")
            self.host_with_port = f"{mysql_hostname}:{self.port}" if mysql_hostname else f"{self.host}:{self.port}"

        # Server UUID configuration (mainly for replication & errant transactions)
        self.server_uuid = global_variables.get("server_uuid")
        if self.connection_source_alt == ConnectionSource.mariadb and self.is_mysql_version_at_least("10.0"):
            self.server_uuid = global_variables.get("server_id")

        # Performance schema
        self.performance_schema_enabled = global_variables.get("performance_schema") == "ON"
        self.use_performance_schema_for_processlist = self.performance_schema_enabled

        # Cluster type detection
        cluster_type = self.group_replication_data.get("cluster_type")
        if cluster_type == "ar":
            self.replicaset = True
        elif cluster_type == "gr":
            self.innodb_cluster = True
            if self.group_replication_data.get("instance_type") == "read-replica":
                self.innodb_cluster = False  # Not a part of the cluster if it's a read replica
                self.innodb_cluster_read_replica = True

        # Group replication host check
        if not self.innodb_cluster and global_variables.get("group_replication_group_name"):
            self.group_replication = True

    def determine_distro_and_connection_source_alt(
        self, global_variables: dict[str, int | str]
    ) -> tuple[str, DataTypes.ConnectionSourceType]:
        is_percona = "percona" in coerce_str(global_variables.get("version_comment")).casefold()
        is_mariadb = any(variable.startswith("aria_") for variable in global_variables)
        is_rds = "rdsdb" in coerce_str(self.global_variables.get("basedir")).casefold()
        is_aurora = self.global_variables.get("aurora_version")
        is_azure = self.global_variables.get("aad_auth_only")
        is_galera_cluster = self.galera_cluster

        # MariaDB
        if is_mariadb:
            if is_rds:
                return "Amazon RDS (MariaDB)", ConnectionSource.mariadb
            if is_azure:
                return "Azure MariaDB", ConnectionSource.mariadb
            if is_galera_cluster:
                return "MariaDB", ConnectionSource.mariadb
            return "MariaDB", ConnectionSource.mariadb

        # Percona
        if is_percona:
            if is_galera_cluster:
                return "Percona Server", ConnectionSource.mysql
            return "Percona Server", ConnectionSource.mysql

        # Standard MySQL
        if is_aurora:
            return "Amazon Aurora", ConnectionSource.mysql
        if is_rds:
            return "Amazon RDS (MySQL)", ConnectionSource.mysql
        if is_azure:
            return "Azure MySQL", ConnectionSource.mysql

        return "MySQL", ConnectionSource.mysql

    def build_kill_query(self, thread_id: int) -> str:
        is_rds = "rdsdb" in coerce_str(self.global_variables.get("basedir")).casefold()
        is_aurora = self.global_variables.get("aurora_version")
        is_azure = self.global_variables.get("aad_auth_only")

        if is_rds or is_aurora:
            return f"CALL mysql.rds_kill({thread_id})"
        if is_azure:
            return f"CALL mysql.az_kill({thread_id})"
        if self.connection_source == ConnectionSource.proxysql:
            return f"KILL CONNECTION {thread_id}"

        return f"KILL {thread_id}"

    def collect_system_utilization(self):
        if not self.enable_system_utilization:
            return

        virtual_memory = psutil.virtual_memory()
        swap_memory = psutil.swap_memory()
        network_io = psutil.net_io_counters()
        disk_io = psutil.disk_io_counters()

        self.system_utilization = {
            "Uptime": int(time.time() - psutil.boot_time()),
            "CPU_Count": psutil.cpu_count(logical=True) or 0,
            "CPU_Percent": psutil.cpu_percent(interval=0),
            "Memory_Total": virtual_memory.total,
            "Memory_Used": virtual_memory.used,
            "Swap_Total": swap_memory.total,
            "Swap_Used": swap_memory.used,
            "Network_Up": network_io.bytes_sent,
            "Network_Down": network_io.bytes_recv,
            "Disk_Read": disk_io.read_count if disk_io else 0,
            "Disk_Write": disk_io.write_count if disk_io else 0,
        }

        # Include the load average if it's available
        try:
            self.system_utilization["CPU_Load_Avg"] = psutil.getloadavg()  # 1, 5, and 15 minute load averages
        except AttributeError:
            pass

    def get_group_replication_metadata(self):
        # Check to get information on what cluster/instance type it is
        if self.is_mysql_version_at_least("8.1"):
            query = MySQLQueries.determine_cluster_type_81
        else:
            query = MySQLQueries.determine_cluster_type_8

        self.main_db_connection.execute(query, ignore_error=True)
        self.group_replication_data = self.main_db_connection.fetchone()

    def add_host_to_tab_setup_file(self):
        if self.daemon_mode:
            return

        with open(self.tab_setup_file, "a+") as file:
            file.seek(0)
            lines = file.readlines()

            host = f"{self.host}:{self.port}\n" if self.port != 3306 else f"{self.host}\n"

            if host not in lines:
                file.write(host)
                self.tab_setup_available_hosts.append(host[:-1])  # remove the \n

    def is_mysql_version_at_least(self, target: str, use_version: str | None = None):
        version = use_version or self.host_version
        if not version:
            return False
        try:
            return parse_version(version) >= parse_version(target)
        except InvalidVersion:
            # Defensive: host_version can be the "N/A" sentinel from parse_server_version
            # when the version variable hasn't been read yet (e.g. during a connection race).
            return False

    def parse_server_version(self, version: str) -> str:
        if not version:
            return "N/A"

        major, minor, patch = version.split(".", 2)
        patch = patch.split("-", 1)[0]

        return f"{major}.{minor}.{patch}"

    def get_hostname(self, host):
        if host in self.host_cache:
            return self.host_cache[host]

        if self.host_cache_from_file and host in self.host_cache_from_file:
            self.host_cache[host] = self.host_cache_from_file[host]
            return self.host_cache_from_file[host]

        try:
            ipaddress.IPv4Network(host)
            hostname = socket.gethostbyaddr(host)[0]
            self.host_cache[host] = hostname
        except (OSError, ValueError):
            self.host_cache[host] = host
            hostname = host

        return hostname

    def record_filter_dropdown_values(self):
        # Filters are applied in the query (or when rendering a replay), so a value that's being
        # filtered out isn't in the processlist anymore. Remember the values we've seen so the
        # filter dropdowns can still offer them
        for thread in self.processlist_threads.values():
            for field, values in self.filter_dropdown_values.items():
                value = getattr(thread, field, None)

                # Skip values that aren't real, such as the N/A placeholder threads get when empty
                if value and not (isinstance(value, str) and value.startswith("[")):
                    values.add(value)

    def determine_proxysql_refresh_interval(self) -> float:
        # If we have a lot of client connections, increase the refresh interval based on the
        # proxysql process execution time. René asked for this to be added to reduce load on ProxySQL
        client_connections = coerce_int(self.global_status.get("Client_Connections_connected"))
        if client_connections > 30000:
            percentage = 0.60
        elif client_connections > 20000:
            percentage = 0.50
        elif client_connections > 10000:
            percentage = 0.40
        else:
            percentage = 0

        if percentage:
            refresh_interval = self.refresh_interval + (self.worker_processing_time * percentage)
        else:
            refresh_interval = self.refresh_interval

        return refresh_interval

    def validate_metadata_locks_enabled(self):
        if not self.is_mysql_version_at_least("5.7") or not self.performance_schema_enabled:
            logger.warning(
                "Metadata Locks requires MySQL 5.7+ with Performance Schema enabled - will not capture that data"
            )
            return

        query = """
            SELECT enabled FROM performance_schema.setup_instruments WHERE name = 'wait/lock/metadata/sql/mdl'
        """
        self.main_db_connection.execute(query)
        row = self.main_db_connection.fetchone()
        if row and row.get("enabled") == "NO":
            logger.warning(
                "Metadata Locks requires Performance Schema to have"
                " wait/lock/metadata/sql/mdl enabled in setup_instruments table - will not capture that data"
            )
            return

        self.metadata_locks_enabled = True

    def get_replay_files(self):
        """Gets a list of replay files in the replay directory.

        Returns:
            list: A list of tuples in the format (full_path, formatted host name + replay name).
        """
        if not self.replay_dir or not os.path.exists(self.replay_dir):
            return []

        replay_files = []
        try:
            with os.scandir(self.replay_dir) as entries:
                for entry in entries:
                    if entry.is_dir():
                        entry_path = entry.path
                        for file in os.scandir(entry_path):
                            if file.is_file():
                                # Get first 30 characters of the host name
                                host_name = entry.name[:30]

                                # Only set port if the host name is 30 characters or more
                                port = ""
                                if len(entry.name) >= 30 and "_" in entry.name:
                                    port = "_" + entry.name.rsplit("_", 1)[-1]

                                formatted_replay_name = f"[$label]{host_name}{port}[/$label]"
                                formatted_replay_name += f": [$b_light_blue]{file.name}[/$b_light_blue]"

                                replay_files.append((file.path, themed_content(formatted_replay_name)))
        except OSError as e:
            self.app.notify(str(e), title="Error getting replay files", severity="error")

        # Sort replay_files by the file path
        replay_files.sort(key=lambda x: x[0])

        return replay_files

    def reset_pfs_metrics_deltas(self, reset_fully: bool = False):
        for instance in [self.file_io_data, self.table_io_waits_data]:
            if not instance:
                continue

            if reset_fully:
                instance.internal_data = {}
                instance.filtered_data = {}
            else:
                for file_data in instance.internal_data.values():
                    for metric_data in file_data["metrics"].values():
                        metric_data["delta"] = 0

                for file_data in instance.filtered_data.values():
                    for metric_data in file_data.values():
                        if isinstance(metric_data, dict) and "d" in metric_data:
                            metric_data["d"] = 0

        self.pfs_metrics_last_reset_time = datetime.now().astimezone()
