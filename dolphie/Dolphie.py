import ipaddress
import os
import socket
import time
from datetime import datetime
from typing import Dict, List, Tuple, Union

import psutil
from loguru import logger
from packaging.version import parse as parse_version
from rich.text import Text
from textual.app import App
from textual.widgets import Switch

import dolphie.DataTypes as DataTypes
import dolphie.Modules.MetricManager as MetricManager
from dolphie.Modules.ArgumentParser import Config
from dolphie.Modules.Functions import load_host_cache_file
from dolphie.Modules.MySQL import ConnectionSource, Database
from dolphie.Modules.PerformanceSchemaMetrics import PerformanceSchemaMetrics
from dolphie.Modules.Queries import MySQLQueries


class Dolphie:
    def __init__(self, config: Config, app: App) -> None:
        self.config = config
        self.app = app
        self.app_version = config.app_version

        self.tab_id: str = None

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
        self.refresh_interval = config.refresh_interval
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
        self.metric_manager = MetricManager.MetricManager(self.replay_file, self.daemon_mode)
        self.replica_manager = DataTypes.ReplicaManager()

        self.dolphie_start_time: datetime = datetime.now()
        self.worker_previous_start_time: datetime = datetime.now()
        self.worker_processing_time: float = 0
        self.polling_latency: float = 0
        self.connection_status: DataTypes.ConnectionStatus = None

        self.global_variables: Dict[str, Union[int, str]] = {}
        self.global_status: Dict[str, Union[int, str]] = {}
        self.binlog_status: Dict[str, Union[int, str]] = {}
        self.replication_status: Dict[str, Union[int, str]] = {}
        self.replication_applier_status: Dict[str, Union[List[Dict[str, Union[int, str]]], int]] = {}
        self.innodb_metrics: Dict[str, Union[int, str]] = {}
        self.metadata_locks: List[Dict[str, Union[int, str]]] = []
        self.ddl: List[Dict[str, Union[int, str]]] = []
        self.disk_io_metrics: Dict[str, Union[int, str]] = {}
        self.statements_summary_metrics: Dict[str, Union[int, str]] = {}
        self.system_utilization: Dict[str, Union[int, str]] = {}
        self.host_cache: Dict[str, str] = {}
        self.proxysql_hostgroup_summary: List[Dict[str, str]] = []
        self.proxysql_mysql_query_rules: List[Dict[str, str]] = []
        self.proxysql_per_second_data: Dict[str, Union[int, str]] = {}
        self.proxysql_command_stats: List[Dict[str, Union[int, str]]] = []
        self.processlist_threads: Dict[int, Union[DataTypes.ProcesslistThread, DataTypes.ProxySQLProcesslistThread]] = (
            {}
        )
        self.processlist_threads_snapshot: Dict[
            int, Union[DataTypes.ProcesslistThread, DataTypes.ProxySQLProcesslistThread]
        ] = {}

        # These are for group replication in replication panel
        self.is_group_replication_primary: bool = False
        self.group_replication_data: Dict[str, str] = {}
        self.group_replication_members: List[Dict[str, str]] = []
        self.innodb_cluster_clustersets: List[Dict[str, str]] = []

        # Filters that can be applied
        self.user_filter: str = None
        self.db_filter: str = None
        self.host_filter: str = None
        self.query_filter: str = None
        self.hostgroup_filter: int = None
        self.query_time_filter: int = None

        # Types of hosts
        self.connection_source: ConnectionSource = ConnectionSource.mysql  # mysql, proxysql
        self.connection_source_alt: ConnectionSource = ConnectionSource.mysql  # mariadb
        self.galera_cluster: bool = False
        self.group_replication: bool = False
        self.innodb_cluster: bool = False
        self.innodb_cluster_read_replica: bool = False
        self.replicaset: bool = False

        # Main connection is used for Textual's worker thread so it can run asynchronous
        db_connection_args = {
            "app": self.app,
            "host": self.host,
            "user": self.user,
            "password": self.password,
            "socket": self.socket,
            "port": self.port,
            "ssl": self.ssl,
            "auto_connect": False,
            "daemon_mode": self.daemon_mode,
        }
        self.main_db_connection = Database(**db_connection_args)
        # Secondary connection is for ad-hoc commands that are not a part of the worker thread
        self.secondary_db_connection = Database(**db_connection_args, save_connection_id=False)

        # Misc variables
        self.host_distro: str = "MySQL"
        self.host_with_port: str = f"{self.host}:{self.port}"
        self.performance_schema_enabled: bool = False
        self.use_performance_schema_for_processlist: bool = False
        self.server_uuid: str = None
        self.host_version: str = None
        self.pause_refresh: bool = False
        self.active_redo_logs: int = None
        self.metadata_locks_enabled: bool = False

        self.host_cache_from_file = load_host_cache_file(self.host_cache_file)

        self.file_io_data: PerformanceSchemaMetrics = None
        self.table_io_waits_data: PerformanceSchemaMetrics = None
        self.statements_summary_data: PerformanceSchemaMetrics = None

        if self.record_for_replay or self.panels.pfs_metrics.visible:
            self.pfs_metrics_last_reset_time: datetime = datetime.now()
        else:
            # This will be set when user presses key to bring up panel
            self.pfs_metrics_last_reset_time: datetime = None

        self.update_switches_after_reset()

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

    def db_connect(self):
        self.main_db_connection.connect()
        if not self.daemon_mode:
            self.secondary_db_connection.connect()

        self.connection_source = self.main_db_connection.source
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
        self, global_variables: Dict[str, Union[int, str]]
    ) -> Tuple[str, ConnectionSource]:
        is_percona = "percona" in global_variables.get("version_comment", "").casefold()
        is_mariadb = any(variable.startswith("aria_") for variable in global_variables)
        is_rds = "rdsdb" in self.global_variables.get("basedir", "").casefold()
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
                return "MariaDB Galera Cluster", ConnectionSource.mariadb
            return "MariaDB", ConnectionSource.mariadb

        # Percona
        if is_percona:
            if is_galera_cluster:
                return "Percona Galera Cluster", ConnectionSource.mysql
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
        is_rds = "rdsdb" in self.global_variables.get("basedir", "").casefold()
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
            "CPU_Count": psutil.cpu_count(logical=True),
            "CPU_Percent": psutil.cpu_percent(interval=0),
            "Memory_Total": virtual_memory.total,
            "Memory_Used": virtual_memory.used,
            "Swap_Total": swap_memory.total,
            "Swap_Used": swap_memory.used,
            "Network_Up": network_io.bytes_sent,
            "Network_Down": network_io.bytes_recv,
            "Disk_Read": disk_io.read_count,
            "Disk_Write": disk_io.write_count,
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

            if self.port != 3306:
                host = f"{self.host}:{self.port}\n"
            else:
                host = f"{self.host}\n"

            if host not in lines:
                file.write(host)
                self.tab_setup_available_hosts.append(host[:-1])  # remove the \n

    def is_mysql_version_at_least(self, target: str, use_version: str = None):
        version = self.host_version
        if use_version:
            version = use_version

        parsed_source = parse_version(version)
        parsed_target = parse_version(target)

        return parsed_source >= parsed_target

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
        except (ValueError, socket.error):
            self.host_cache[host] = host
            hostname = host

        return hostname

    def update_switches_after_reset(self):
        # Set the graph switches to what they're currently selected to after a reset
        switches = self.app.query(f".switch_container_{self.tab_id} Switch")
        for switch in switches:
            switch: Switch
            metric_instance_name = switch.name
            metric = switch.id

            metric_instance = getattr(self.metric_manager.metrics, metric_instance_name)
            metric_data: MetricManager.MetricData = getattr(metric_instance, metric)
            metric_data.visible = switch.value

    def determine_proxysql_refresh_interval(self) -> float:
        # If we have a lot of client connections, increase the refresh interval based on the
        # proxysql process execution time. René asked for this to be added to reduce load on ProxySQL
        client_connections = self.global_status.get("Client_Connections_connected", 0)
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
        """
        Gets a list of replay files in the replay directory.

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

                                formatted_replay_name = f"[label]{host_name}{port}[/label]"
                                formatted_replay_name += f": [b light_blue]{file.name}[/b light_blue]"

                                replay_files.append((file.path, Text.from_markup(formatted_replay_name)))
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
                for data in (instance.internal_data, instance.filtered_data):
                    for file_data in data.values():
                        for metric_data in file_data.get("metrics", file_data).values():
                            # For filtered_data so replay data is smaller in size
                            if "d" in metric_data:
                                metric_data["d"] = 0
                            elif "delta" in metric_data:
                                metric_data["delta"] = 0

        self.pfs_metrics_last_reset_time = datetime.now()
