import ipaddress
import socket
from datetime import datetime

import dolphie.DataTypes as DataTypes
import dolphie.Modules.MetricManager as MetricManager
from dolphie.Modules.ArgumentParser import Config
from dolphie.Modules.Functions import load_host_cache_file
from dolphie.Modules.MySQL import Database
from dolphie.Modules.Queries import MySQLQueries
from packaging.version import parse as parse_version
from textual.app import App
from textual.widgets import Switch


class Dolphie:
    def __init__(self, config: Config, app: App) -> None:
        self.config = config
        self.app = app
        self.app_version = config.app_version

        self.tab_id: int = None
        self.tab_name: str = None

        # Config options
        self.user = config.user
        self.password = config.password
        self.host = config.host
        self.port = config.port
        self.socket = config.socket
        self.ssl = config.ssl
        self.host_cache_file = config.host_cache_file
        self.host_setup_file = config.host_setup_file
        self.refresh_interval = config.refresh_interval
        self.show_trxs_only = config.show_trxs_only
        self.show_additional_query_columns = config.show_additional_query_columns
        self.heartbeat_table = config.heartbeat_table
        self.host_setup_available_hosts = config.host_setup_available_hosts
        self.startup_panels = config.startup_panels
        self.graph_marker = config.graph_marker
        # self.historical_trx_locks = config.historical_trx_locks
        self.hostgroup = config.hostgroup
        self.hostgroup_hosts = config.hostgroup_hosts

        self.reset_runtime_variables()

        # Set the default panels based on startup_panels to be visible
        self.panels = DataTypes.Panels()
        for panel in self.panels.all():
            setattr(getattr(self.panels, panel), "visible", False)
        for panel in self.startup_panels:
            setattr(getattr(self.panels, panel), "visible", True)

        self.show_idle_threads: bool = False
        self.sort_by_time_descending: bool = True

    def reset_runtime_variables(self):
        self.metric_manager = MetricManager.MetricManager()
        self.replica_manager = DataTypes.ReplicaManager()

        # Set the graph switches to what they're currently selected to since we reset metric_manager
        switches = self.app.query(f".switch_container_{self.tab_id} Switch")
        for switch in switches:
            switch: Switch
            metric_instance_name = switch.name
            metric = switch.id

            metric_instance = getattr(self.metric_manager.metrics, metric_instance_name)
            metric_data: MetricManager.MetricData = getattr(metric_instance, metric)
            metric_data.visible = switch.value

        self.dolphie_start_time: datetime = datetime.now()
        self.worker_start_time: datetime = datetime.now()
        self.worker_previous_start_time: datetime = datetime.now()
        self.completed_first_loop: bool = False
        self.polling_latency: float = 0
        self.refresh_latency: str = "0"
        self.connection_status: DataTypes.ConnectionStatus = None
        self.processlist_threads: dict = {}
        self.processlist_threads_snapshot: dict = {}
        self.lock_transactions: dict = {}
        self.metadata_locks: dict = {}
        self.ddl: list = []
        self.pause_refresh: bool = False
        self.previous_binlog_position: int = 0
        self.previous_replica_sbm: int = 0
        self.innodb_metrics: dict = {}
        self.disk_io_metrics: dict = {}
        self.global_variables: dict = {}
        self.innodb_trx_lock_metrics: dict = {}
        self.global_status: dict = {}
        self.binlog_status: dict = {}
        self.replication_status: dict = {}
        self.replication_applier_status: dict = {}
        self.replica_lag_source: str = None
        self.replica_lag: int = None
        self.active_redo_logs: int = None
        self.mysql_host: str = f"{self.host}:{self.port}"
        self.binlog_transaction_compression_percentage: int = None
        self.host_cache: dict = {}

        self.user_filter = None
        self.db_filter = None
        self.host_filter = None
        self.query_time_filter = None
        self.query_filter = None

        # Types of hosts
        self.galera_cluster: bool = False
        self.group_replication: bool = False
        self.innodb_cluster: bool = False
        self.innodb_cluster_read_replica: bool = False
        self.replicaset: bool = False
        self.aws_rds: bool = False
        self.azure: bool = False
        self.mariadb: bool = False

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
        }
        self.main_db_connection = Database(**db_connection_args)
        # Secondary connection is for ad-hoc commands that are not a part of the worker thread
        self.secondary_db_connection = Database(**db_connection_args, save_connection_id=False)

        self.performance_schema_enabled: bool = False
        self.use_performance_schema: bool = True
        self.server_uuid: str = None
        self.mysql_version: str = None
        self.host_distro: str = None

        self.host_cache_from_file = load_host_cache_file(self.host_cache_file)

    def db_connect(self):
        self.main_db_connection.connect()
        self.secondary_db_connection.connect()

        global_variables = self.main_db_connection.fetch_status_and_variables("variables")

        basedir = global_variables.get("basedir")
        aurora_version = global_variables.get("aurora_version")
        version = global_variables.get("version").lower()
        version_comment = global_variables.get("version_comment").lower()
        version_split = version.split(".")
        self.mysql_version = "%s.%s.%s" % (
            version_split[0],
            version_split[1],
            version_split[2].split("-")[0],
        )

        # Get proper host version and fork
        if "percona xtradb cluster" in version_comment:
            self.host_distro = "Percona XtraDB Cluster"
        elif "percona server" in version_comment:
            self.host_distro = "Percona Server"
        elif "mariadb cluster" in version_comment:
            self.host_distro = "MariaDB Cluster"
            self.mariadb = True
        elif "mariadb" in version_comment or "mariadb" in version:
            self.host_distro = "MariaDB"
            self.mariadb = True
        elif aurora_version:
            self.host_distro = "Amazon Aurora"
            self.aws_rds = True
        elif "rdsdb" in basedir:
            self.host_distro = "Amazon RDS"
            self.aws_rds = True
        elif global_variables.get("aad_auth_only"):
            self.host_distro = "Azure MySQL"
            self.azure = True
        else:
            self.host_distro = "MySQL"

        # For RDS and Azure, we will use the host specified to connect with since hostname isn't related to the endpoint
        if self.aws_rds:
            self.mysql_host = f"{self.host.split('.rds.amazonaws.com')[0]}:{self.port}"
        elif self.azure:
            self.mysql_host = f"{self.host.split('.mysql.database.azure.com')[0]}:{self.port}"
        else:
            self.mysql_host = f"{global_variables.get('hostname')}:{self.port}"

        major_version = int(version_split[0])
        self.server_uuid = global_variables.get("server_uuid")
        if "MariaDB" in self.host_distro and major_version >= 10:
            self.server_uuid = global_variables.get("server_id")

        if global_variables.get("performance_schema") == "ON":
            self.performance_schema_enabled = True

        # Check to see if the host is in a Galera cluster
        galera_matches = any(key.startswith("wsrep_") for key in global_variables.keys())
        if galera_matches:
            self.galera_cluster = True

        # Check to get information on what cluster type it is
        if self.is_mysql_version_at_least("8.1"):
            query = MySQLQueries.determine_cluster_type_81
        else:
            query = MySQLQueries.determine_cluster_type_8

        self.main_db_connection.execute(query, ignore_error=True)
        data = self.main_db_connection.fetchone()

        cluster_type = data.get("cluster_type")
        instance_type = data.get("instance_type")

        if cluster_type == "ar":
            self.replicaset = True
        elif cluster_type == "gr":
            self.innodb_cluster = True

            if instance_type == "read-replica":
                self.innodb_cluster = False  # It doesn't work like an actual member in a cluster so set it to False
                self.innodb_cluster_read_replica = True

        if not self.innodb_cluster and global_variables.get("group_replication_group_name"):
            self.group_replication = True

        # Add host to host setup file if it doesn't exist
        with open(self.host_setup_file, "a+") as file:
            file.seek(0)
            lines = file.readlines()

            if self.port != 3306:
                host = f"{self.host}:{self.port}\n"
            else:
                host = f"{self.host}\n"

            if host not in lines:
                file.write(host)
                self.host_setup_available_hosts.append(host[:-1])  # remove the \n

    def is_mysql_version_at_least(self, target, use_version=None):
        version = self.mysql_version
        if use_version:
            version = use_version

        parsed_source = parse_version(version)
        parsed_target = parse_version(target)

        return parsed_source >= parsed_target

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

    def massage_metrics_data(self):
        if self.is_mysql_version_at_least("8.0"):
            # If we're using MySQL 8, we need to fetch the checkpoint age from the performance schema if it's not
            # available in global status
            # On Azure MySQL there is no BACKUP_ADMIN privilege so we can't fetch the checkpoint age
            if not self.global_status.get("Innodb_checkpoint_age") and not self.azure:
                self.global_status["Innodb_checkpoint_age"] = self.main_db_connection.fetch_value_from_field(
                    MySQLQueries.checkpoint_age, "checkpoint_age"
                )

            if self.is_mysql_version_at_least("8.0.30"):
                active_redo_logs_count = self.main_db_connection.fetch_value_from_field(
                    MySQLQueries.active_redo_logs, "count"
                )
                self.global_status["Active_redo_log_count"] = active_redo_logs_count

        # If the server doesn't support Innodb_lsn_current, use Innodb_os_log_written instead
        # which has less precision, but it's good enough
        if not self.global_status.get("Innodb_lsn_current"):
            self.global_status["Innodb_lsn_current"] = self.global_status["Innodb_os_log_written"]
