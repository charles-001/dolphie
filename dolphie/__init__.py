import ipaddress
import os
import socket
from datetime import datetime
from importlib import metadata

import dolphie.DataTypes as DataTypes
import dolphie.Modules.MetricManager as MetricManager
import requests
from dolphie.Modules.ManualException import ManualException
from dolphie.Modules.MySQL import Database
from dolphie.Modules.Queries import MySQLQueries
from dolphie.Widgets.new_version_modal import NewVersionModal
from packaging.version import parse as parse_version
from textual.app import App
from textual.widgets import Switch

try:
    __package_name__ = metadata.metadata(__package__ or __name__)["Name"]
    __version__ = metadata.version(__package__ or __name__)
except Exception:
    __package_name__ = "Dolphie"
    __version__ = "N/A"


class Dolphie:
    def __init__(self):
        self.app: App = None
        self.app_version = __version__
        self.tab_name = None
        self.tab_id = None

        # Config options
        self.user: str = None
        self.password: str = None
        self.host: str = None
        self.port: int = 3306
        self.socket: str = None
        self.ssl: dict = {}
        self.config_file: str = None
        self.host_cache_file: str = None
        self.host_setup_file: str = None
        self.debug: bool = False
        self.refresh_interval: int = 1
        self.show_idle_threads: bool = False
        self.show_trxs_only: bool = False
        self.show_additional_query_columns: bool = False
        self.sort_by_time_descending: bool = True
        self.heartbeat_table: str = None
        self.user_filter: str = None
        self.db_filter: str = None
        self.host_filter: str = None
        self.query_time_filter: str = 0
        self.query_filter: str = None
        self.host_setup_available_hosts: list = []
        self.host_cache: dict = {}
        self.host_cache_from_file: dict = {}
        self.startup_panels: str = None
        self.graph_marker: str = None

        self.reset_runtime_variables()

    def reset_runtime_variables(self):
        self.metric_manager = MetricManager.MetricManager()
        self.replica_manager = DataTypes.ReplicaManager()
        self.panels = DataTypes.Panels()

        # Set the graph switches to what they're currently selected to since we reset metric_manager
        if self.app:
            # Set the default panels based on startup_panels to be visible
            for panel in self.panels.all():
                setattr(getattr(self.panels, panel), "visible", False)
            for panel in self.startup_panels:
                setattr(getattr(self.panels, panel), "visible", True)

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
        self.first_loop: bool = False
        self.polling_latency: float = 0
        self.refresh_latency: str = "0"
        self.read_only_status: str = None
        self.processlist_threads: dict = {}
        self.processlist_threads_snapshot: dict = {}
        self.lock_transactions: dict = {}
        self.ddl: list = []
        self.pause_refresh: bool = False
        self.previous_binlog_position: int = 0
        self.previous_replica_sbm: int = 0
        self.innodb_metrics: dict = {}
        self.disk_io_metrics: dict = {}
        self.global_variables: dict = {}
        self.lock_metrics: dict = {}
        self.global_status: dict = {}
        self.binlog_status: dict = {}
        self.replication_status: dict = {}
        self.replication_applier_status: dict = {}
        self.replica_lag_source: str = None
        self.replica_lag: int = None
        self.active_redo_logs: int = None
        self.mysql_host: str = None
        self.binlog_transaction_compression_percentage: int = None

        # Types of hosts
        self.galera_cluster: bool = False
        self.group_replication: bool = False
        self.innodb_cluster: bool = False
        self.innodb_cluster_read_replica: bool = False
        self.replicaset: bool = False
        self.aws_rds: bool = False
        self.mariadb: bool = False

        # These are for group replication in replication panel
        self.is_group_replication_primary: bool = False
        self.group_replication_members: dict = {}
        self.group_replication_data: dict = {}

        # Database connection global_variables
        # Main connection is used for Textual's worker thread so it can run asynchronous
        self.main_db_connection: Database = None
        # Secondary connection is for ad-hoc commands that are not a part of the worker thread
        self.secondary_db_connection: Database = None
        self.performance_schema_enabled: bool = False
        self.use_performance_schema: bool = True
        self.server_uuid: str = None
        self.mysql_version: str = None
        self.host_distro: str = None

    def check_for_update(self):
        # Query PyPI API to get the latest version
        try:
            url = f"https://pypi.org/pypi/{__package_name__}/json"
            response = requests.get(url, timeout=3)

            if response.status_code == 200:
                data = response.json()

                # Extract the latest version from the response
                latest_version = data["info"]["version"]

                # Compare the current version with the latest version
                if parse_version(latest_version) > parse_version(__version__):
                    self.app.push_screen(NewVersionModal(current_version=__version__, latest_version=latest_version))
        except Exception:
            pass

    def is_mysql_version_at_least(self, target, use_version=None):
        version = self.mysql_version
        if use_version:
            version = use_version

        parsed_source = parse_version(version)
        parsed_target = parse_version(target)

        return parsed_source >= parsed_target

    def db_connect(self):
        db_connection_args = {
            "app": self.app,
            "tab_name": self.tab_name,
            "host": self.host,
            "user": self.user,
            "password": self.password,
            "socket": self.socket,
            "port": self.port,
            "ssl": self.ssl,
        }
        self.main_db_connection = Database(**db_connection_args)
        self.secondary_db_connection = Database(**db_connection_args, save_connection_id=False)

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
        else:
            self.host_distro = "MySQL"

        # For RDS, we will use the host specified to connect with since hostname isn't related to the endpoint
        if self.aws_rds:
            self.mysql_host = f"{self.host.split('.rds.amazonaws.com')[0]}:{self.port}"
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

    def monitor_read_only_change(self):
        current_ro_status = self.global_variables.get("read_only")
        formatted_ro_status = "RO" if current_ro_status == "ON" else "R/W"
        status = "read-only" if current_ro_status == "ON" else "read/write"

        message = f"Host [light_blue]{self.host}:{self.port}[/light_blue] is now [b highlight]{status}[/b highlight]"

        if current_ro_status == "ON" and not self.replication_status and not self.group_replication:
            message += " ([yellow]SHOULD BE READ/WRITE?[/yellow])"
        elif current_ro_status == "ON" and self.group_replication and self.is_group_replication_primary:
            message += " ([yellow]SHOULD BE READ/WRITE?[/yellow])"

        if self.read_only_status != formatted_ro_status and self.first_loop:
            self.app.notify(title="Read-only mode change", message=message, severity="warning", timeout=15)

        self.read_only_status = formatted_ro_status

    def command_input_to_variable(self, return_data):
        variable = return_data[0]
        value = return_data[1]
        if value:
            setattr(self, variable, value)

    def load_host_cache_file(self):
        if os.path.exists(self.host_cache_file):
            with open(self.host_cache_file) as file:
                for line in file:
                    line = line.strip()
                    error_message = f"Host cache entry '{line}' is not properly formatted! Format: ip=hostname"

                    if "=" not in line:
                        raise ManualException(error_message)

                    host, hostname = line.split("=", maxsplit=1)
                    host = host.strip()
                    hostname = hostname.strip()

                    if not host or not hostname:
                        raise ManualException(error_message)

                    self.host_cache_from_file[host] = hostname

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
            if not self.global_status.get("Innodb_checkpoint_age"):
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

    def fetch_replication_data(self, replica: DataTypes.Replica = None):
        use_version = self.mysql_version
        if replica:
            use_version = replica.mysql_version

        if self.heartbeat_table:
            query = MySQLQueries.heartbeat_replica_lag
            replica_lag_source = "HB"
        elif (
            self.is_mysql_version_at_least("8.0", use_version) and self.performance_schema_enabled and not self.mariadb
        ):
            replica_lag_source = "PS"
            query = MySQLQueries.ps_replica_lag
        else:
            query = MySQLQueries.replication_status
            replica_lag_source = None

        replica_lag_data = None
        if replica:
            replica.connection.execute(query)
            replica_lag_data = replica.connection.fetchone()
        else:
            self.main_db_connection.execute(MySQLQueries.replication_status)
            replica_lag_data = self.main_db_connection.fetchone()
            self.replication_status = replica_lag_data

            if self.replication_status:
                # Use a better way to detect replication lag if available
                if replica_lag_source:
                    self.main_db_connection.execute(query)
                    replica_lag_data = self.main_db_connection.fetchone()

                # If we're using MySQL 8, fetch the replication applier status data
                self.replication_applier_status = None
                if (
                    self.is_mysql_version_at_least("8.0")
                    and self.panels.replication.visible
                    and self.global_variables.get("slave_parallel_workers", 0) > 1
                ):
                    self.main_db_connection.execute(MySQLQueries.replication_applier_status)
                    self.replication_applier_status = self.main_db_connection.fetchall()

        replica_lag = None
        if replica_lag_data and replica_lag_data["Seconds_Behind_Master"] is not None:
            replica_lag = int(replica_lag_data["Seconds_Behind_Master"])

        if replica:
            return replica_lag_source, replica_lag
        else:
            # Save the previous replica lag for to determine Speed data point
            self.previous_replica_sbm = self.replica_lag

            self.replica_lag_source = replica_lag_source
            self.replica_lag = replica_lag
