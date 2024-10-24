import ipaddress
import os
import socket
from datetime import datetime
from typing import Dict, Union

from loguru import logger
from packaging.version import parse as parse_version
from textual.app import App
from textual.widgets import Switch

import dolphie.DataTypes as DataTypes
import dolphie.Modules.MetricManager as MetricManager
from dolphie.Modules.ArgumentParser import Config
from dolphie.Modules.Functions import load_host_cache_file
from dolphie.Modules.MySQL import ConnectionSource, Database
from dolphie.Modules.Queries import MySQLQueries


class Dolphie:
    def __init__(self, config: Config, app: App) -> None:
        self.config = config
        self.app = app
        self.app_version = config.app_version

        self.tab_id: int = None

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
        self.heartbeat_table = config.heartbeat_table
        self.tab_setup_available_hosts = config.tab_setup_available_hosts
        self.startup_panels = config.startup_panels
        self.graph_marker = config.graph_marker
        self.hostgroup = config.hostgroup
        self.hostgroup_hosts = config.hostgroup_hosts
        self.record_for_replay = config.record_for_replay
        self.daemon_mode = config.daemon_mode
        self.replay_file = config.replay_file  # This denotes that we're replaying a file
        self.replay_dir = config.replay_dir
        self.replay_retention_hours = config.replay_retention_hours
        self.exclude_notify_global_vars = config.exclude_notify_global_vars

        self.reset_runtime_variables()

        # Set the default panels based on startup_panels to be visible
        self.panels = DataTypes.Panels()
        for panel in self.panels.all():
            setattr(getattr(self.panels, panel), "visible", False)

        for panel in self.startup_panels:
            if panel in self.panels.all():
                setattr(getattr(self.panels, panel), "visible", True)

        self.show_idle_threads: bool = False
        self.sort_by_time_descending: bool = True

    def reset_runtime_variables(self):
        self.metric_manager = MetricManager.MetricManager(self.replay_file, self.daemon_mode)
        self.replica_manager = DataTypes.ReplicaManager()

        self.dolphie_start_time: datetime = datetime.now()
        self.worker_previous_start_time: datetime = datetime.now()
        self.worker_processing_time: float = 0
        self.polling_latency: float = 0
        self.connection_status: DataTypes.ConnectionStatus = None
        self.processlist_threads: Dict[int, Union[DataTypes.ProcesslistThread, DataTypes.ProxySQLProcesslistThread]] = (
            {}
        )
        self.processlist_threads_snapshot: Dict[
            int, Union[DataTypes.ProcesslistThread, DataTypes.ProxySQLProcesslistThread]
        ] = {}
        self.lock_transactions: dict = {}
        self.metadata_locks: dict = {}
        self.ddl: list = []
        self.pause_refresh: bool = False
        self.innodb_metrics: dict = {}
        self.disk_io_metrics: dict = {}
        self.global_variables: dict = {}
        self.innodb_trx_lock_metrics: dict = {}
        self.global_status: dict = {}
        self.binlog_status: dict = {}
        self.replication_status: dict = {}
        self.replication_applier_status: dict = {}
        self.active_redo_logs: int = None
        self.host_with_port: str = f"{self.host}:{self.port}"
        self.host_cache: dict = {}
        self.proxysql_hostgroup_summary: dict = {}
        self.proxysql_mysql_query_rules: dict = {}
        self.proxysql_per_second_data: dict = {}
        self.proxysql_command_stats: dict = {}

        # Filters that can be applied
        self.user_filter = None
        self.db_filter = None
        self.host_filter = None
        self.hostgroup_filter = None
        self.query_time_filter = None
        self.query_filter = None

        # Types of hosts
        self.connection_source: ConnectionSource = ConnectionSource.mysql
        self.connection_source_alt: ConnectionSource = ConnectionSource.mysql  # rds, azure, etc
        self.galera_cluster: bool = False
        self.group_replication: bool = False
        self.innodb_cluster: bool = False
        self.innodb_cluster_read_replica: bool = False
        self.replicaset: bool = False

        # These are for group replication in replication panel
        self.is_group_replication_primary: bool = False
        self.group_replication_members: dict = {}
        self.group_replication_data: dict = {}

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

        self.performance_schema_enabled: bool = False
        self.metadata_locks_enabled: bool = False
        self.use_performance_schema: bool = True
        self.server_uuid: str = None
        self.host_version: str = None
        self.host_distro: str = None

        self.host_cache_from_file = load_host_cache_file(self.host_cache_file)

        self.update_switches_after_reset()

    def db_connect(self):
        self.main_db_connection.connect()
        if not self.daemon_mode:
            self.secondary_db_connection.connect()

        self.connection_source = self.main_db_connection.source
        self.connection_source_alt = self.connection_source
        if self.connection_source == ConnectionSource.proxysql:
            self.host_distro = "ProxySQL"
            self.host_with_port = f"{self.host}:{self.port}"

        self.metric_manager.connection_source = self.connection_source

        # Add host to tab setup file if it doesn't exist
        self.add_host_to_tab_setup_file()

    def configure_mysql_variables(self):
        global_variables = self.global_variables

        version_comment = global_variables.get("version_comment").lower()

        # Get proper host version and fork
        if "percona xtradb cluster" in version_comment:
            self.host_distro = "Percona XtraDB Cluster"
        elif "percona server" in version_comment:
            self.host_distro = "Percona Server"
        elif "mariadb cluster" in version_comment:
            self.host_distro = "MariaDB Cluster"
            self.connection_source_alt = ConnectionSource.mariadb
        elif "mariadb" in version_comment:
            self.host_distro = "MariaDB"
            self.connection_source_alt = ConnectionSource.mariadb
        elif global_variables.get("aurora_version"):
            self.host_distro = "Amazon Aurora"
            self.connection_source_alt = ConnectionSource.aws_rds
        elif "rdsdb" in global_variables.get("basedir"):
            self.host_distro = "Amazon RDS"
            self.connection_source_alt = ConnectionSource.aws_rds
        elif global_variables.get("aad_auth_only"):
            self.host_distro = "Azure MySQL"
            self.connection_source_alt = ConnectionSource.azure_mysql
        else:
            self.host_distro = "MySQL"

        # For RDS and Azure, we will use the host specified to connect with since hostname isn't related to the endpoint
        if self.connection_source_alt == ConnectionSource.aws_rds:
            self.host_with_port = f"{self.host.split('.rds.amazonaws.com')[0]}:{self.port}"
        elif self.connection_source_alt == ConnectionSource.azure_mysql:
            self.host_with_port = f"{self.host.split('.mysql.database.azure.com')[0]}:{self.port}"
        else:
            self.host_with_port = f"{global_variables.get('hostname')}:{self.port}"

        self.server_uuid = global_variables.get("server_uuid")
        if self.connection_source_alt == ConnectionSource.mariadb and self.is_mysql_version_at_least("10.0"):
            self.server_uuid = global_variables.get("server_id")

        if global_variables.get("performance_schema") == "ON":
            self.performance_schema_enabled = True

        # Check to see if the host is in a Galera cluster
        if global_variables.get("wsrep_on") == "ON" or global_variables.get("wsrep_cluster_address"):
            self.galera_cluster = True

        # Check to see if the host is in a InnoDB cluster
        if self.group_replication_data.get("cluster_type") == "ar":
            self.replicaset = True
        elif self.group_replication_data.get("cluster_type") == "gr":
            self.innodb_cluster = True

            if self.group_replication_data.get("instance_type") == "read-replica":
                self.innodb_cluster = False  # It doesn't work like an actual member in a cluster so set it to False
                self.innodb_cluster_read_replica = True

        # Check to see if this is a Group Replication host
        if not self.innodb_cluster and global_variables.get("group_replication_group_name"):
            self.group_replication = True

    def get_group_replication_metadata(self):
        # Check to get information on what cluster/instance type it is
        if self.is_mysql_version_at_least("8.1"):
            query = MySQLQueries.determine_cluster_type_81
        else:
            query = MySQLQueries.determine_cluster_type_8

        self.main_db_connection.execute(query, ignore_error=True)
        data = self.main_db_connection.fetchone()
        self.group_replication_data["cluster_type"] = data.get("cluster_type")
        self.group_replication_data["instance_type"] = data.get("instance_type")

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

    def set_host_version(self, version: str):
        version_split = version.split(".")
        self.host_version = "%s.%s.%s" % (
            version_split[0],
            version_split[1],
            version_split[2].split("-")[0],
        )

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

    def detect_global_variable_change(self, old_data: dict, new_data: dict):
        if not old_data:
            return

        # gtid is always changing so we don't want to alert on that
        # read_only is managed by monitor_read_only_change in app.py
        # The others are ones I've found to be spammy due to monitoring tools changing them
        exclude_variables = {"gtid", "read_only", "innodb_thread_sleep_delay"}

        # Add to exclude_variables with user specified variables
        if self.exclude_notify_global_vars:
            exclude_variables.update(self.exclude_notify_global_vars)

        for variable, new_value in new_data.items():
            if any(item in variable.lower() for item in exclude_variables):
                continue

            old_value = old_data.get(variable)
            if old_value != new_value:
                self.app.notify(
                    f"[highlight]{variable}[/highlight] has changed from "
                    f"[highlight]{old_value}[/highlight] to "
                    f"[highlight]{new_value}[/highlight]",
                    title="Global Variable Change",
                    severity="warning",
                    timeout=10,
                )
                logger.info(f"Global variable {variable} changed: {old_value} -> {new_value}")

    def validate_metadata_locks_enabled(self):
        if not self.is_mysql_version_at_least("5.7") or not self.performance_schema_enabled:
            logger.warning(
                "Metadata Locks requires MySQL 5.7+ with Performance Schema enabled - will not capture that data."
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
                " wait/lock/metadata/sql/mdl enabled in setup_instruments table - will not capture that data."
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

                                replay_files.append((file.path, formatted_replay_name))
        except OSError as e:
            self.app.notify(str(e), title="Error getting replay files", severity="error")

        return replay_files
