import ipaddress
import os
import re
import socket
from datetime import datetime, timedelta
from importlib import metadata

import pymysql
import requests
from dolphie.Modules.Functions import format_number, format_sys_table_memory
from dolphie.Modules.ManualException import ManualException
from dolphie.Modules.MetricManager import MetricManager
from dolphie.Modules.MySQL import Database
from dolphie.Modules.Queries import MySQLQueries
from dolphie.Widgets.command_screen import CommandScreen
from dolphie.Widgets.event_log_screen import EventLog
from dolphie.Widgets.modal import CommandModal
from dolphie.Widgets.new_version_modal import NewVersionModal
from dolphie.Widgets.quick_switch import QuickSwitchHostModal
from packaging.version import parse as parse_version
from rich import box
from rich.align import Align
from rich.console import Group
from rich.panel import Panel
from rich.style import Style
from rich.syntax import Syntax
from rich.table import Table
from sqlparse import format as sqlformat
from textual.app import App

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

        # Config options
        self.user: str = None
        self.password: str = None
        self.host: str = None
        self.port: int = 3306
        self.socket: str = None
        self.ssl: dict = {}
        self.config_file: str = None
        self.host_cache_file: str = None
        self.quick_switch_hosts_file: str = None
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
        self.quick_switch_hosts: list = []
        self.host_cache: dict = {}
        self.host_cache_from_file: dict = {}
        self.startup_panels: str = None
        self.first_loop: bool = False

        # Panel display states
        self.display_dashboard_panel: bool = False
        self.display_processlist_panel: bool = False
        self.display_replication_panel: bool = False
        self.display_graphs_panel: bool = False

        self.reset_runtime_variables()

    def reset_runtime_variables(self):
        self.metric_manager = MetricManager()

        self.dolphie_start_time: datetime = datetime.now()
        self.worker_start_time: datetime = datetime.now()
        self.worker_previous_start_time: datetime = datetime.now()
        self.polling_latency: float = 0
        self.read_only_data: str = None
        self.read_only: str = None
        self.processlist_threads: dict = {}
        self.processlist_threads_snapshot: dict = {}
        self.pause_refresh: bool = False
        self.previous_binlog_position: int = 0
        self.previous_replica_sbm: int = 0
        self.innodb_metrics: dict = {}
        self.disk_io_metrics: dict = {}
        self.global_variables: dict = {}
        self.global_status: dict = {}
        self.binlog_status: dict = {}
        self.replication_status: dict = {}
        self.replication_applier_status: dict = {}
        self.replication_primary_server_uuid: str = None
        self.replica_lag_source: str = None
        self.replica_lag: int = None
        self.active_redo_logs: int = None
        self.mysql_host: str = None
        self.quick_switched_connection: bool = False

        # These are for replicas in replication panel
        self.replica_data: dict = {}
        self.replica_connections: dict = {}
        self.replica_tables: dict = {}
        self.replica_ports: dict = {}

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
        self.main_db_connection_id: int = None
        self.secondary_db_connection_id: int = None
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

    def is_mysql_version_at_least(self, target):
        parsed_source = parse_version(self.mysql_version)
        parsed_target = parse_version(target)

        return parsed_source >= parsed_target

    def db_connect(self):
        self.main_db_connection = Database(self.host, self.user, self.password, self.socket, self.port, self.ssl)
        self.secondary_db_connection = Database(self.host, self.user, self.password, self.socket, self.port, self.ssl)

        # Get connection IDs so we can exclude them from processlist
        self.main_db_connection_id = self.main_db_connection.fetch_value_from_field("SELECT CONNECTION_ID()")
        self.secondary_db_connection_id = self.secondary_db_connection.fetch_value_from_field("SELECT CONNECTION_ID()")

        # Reduce any issues with the queries Dolphie runs (mostly targetting only_full_group_by)
        self.main_db_connection.execute("SET SESSION sql_mode = ''")
        self.secondary_db_connection.execute("SET SESSION sql_mode = ''")

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

        # Add host to quick switch hosts file if it doesn't exist
        with open(self.quick_switch_hosts_file, "a+") as file:
            file.seek(0)
            lines = file.readlines()

            if self.port != 3306:
                host = f"{self.host}:{self.port}\n"
            else:
                host = f"{self.host}\n"

            if host not in lines:
                file.write(host)
                self.quick_switch_hosts.append(host[:-1])  # remove the \n

    def command_input_to_variable(self, return_data):
        variable = return_data[0]
        value = return_data[1]
        if value:
            setattr(self, variable, value)

    def toggle_panel(self, panel_name):
        panel = self.app.query_one(f"#panel_{panel_name}")

        new_display = not panel.display
        panel.display = new_display
        setattr(self, f"display_{panel_name}_panel", new_display)
        if panel_name not in ["graphs"]:
            self.app.refresh_panel(panel_name, toggled=True)

    def notify(self, message, severity="information", title=None, timeout=8):
        self.app.notify(message, severity=severity, title=title, timeout=timeout)

    def capture_key(self, key):
        screen_data = None

        if not self.main_db_connection:
            self.notify("Database connection must be established before using commands")

            return

        if key == "1":
            self.toggle_panel("dashboard")
        elif key == "2":
            self.toggle_panel("processlist")
            self.app.query_one("#panel_processlist").clear()
        elif key == "3":
            self.toggle_panel("replication")
        elif key == "4":
            self.toggle_panel("graphs")
            self.app.update_graphs("dml")
        elif key == "grave_accent":

            def command_get_input(data):
                host_port = data["host"].split(":")
                self.host = host_port[0]
                self.port = int(host_port[1]) if len(host_port) > 1 else 3306

                password = data.get("password")
                if password:
                    self.password = password

                # Trigger a quick switch connection for the worker thread
                self.quick_switched_connection = True

                self.app.query_one("#main_container").display = False
                self.app.query_one("LoadingIndicator").display = True
                self.app.query_one("#panel_dashboard_queries_qps").display = False
                self.app.query_one("TopBar").host = "Connecting to MySQL"

            self.app.push_screen(QuickSwitchHostModal(quick_switch_hosts=self.quick_switch_hosts), command_get_input)

        elif key == "a":
            if self.show_additional_query_columns:
                self.show_additional_query_columns = False
            else:
                self.show_additional_query_columns = True

        elif key == "c":
            self.user_filter = ""
            self.db_filter = ""
            self.host_filter = ""
            self.query_time_filter = ""
            self.query_filter = ""

            self.notify("Cleared all filters", severity="success")

        elif key == "d":
            tables = {}
            all_tables = []

            db_count = self.secondary_db_connection.execute(MySQLQueries.databases)
            databases = self.secondary_db_connection.fetchall()

            # Determine how many tables to provide data
            max_num_tables = 1 if db_count <= 20 else 3

            # Calculate how many databases per table
            row_per_count = db_count // max_num_tables

            # Create dictionary of tables
            for table_counter in range(1, max_num_tables + 1):
                tables[table_counter] = Table(box=box.ROUNDED, show_header=False, style="table_border")
                tables[table_counter].add_column("")

            # Loop over databases
            db_counter = 1
            table_counter = 1

            # Sort the databases by name
            for database in databases:
                tables[table_counter].add_row(database["SCHEMA_NAME"])
                db_counter += 1

                if db_counter > row_per_count and table_counter < max_num_tables:
                    table_counter += 1
                    db_counter = 1

            # Collect table data into an array
            all_tables = [table_data for table_data in tables.values() if table_data]

            table_grid = Table.grid()
            table_grid.add_row(*all_tables)

            screen_data = Group(
                Align.center("[b]Databases[/b]"),
                Align.center(table_grid),
                Align.center("Total: [b highlight]%s[/b highlight]" % db_count),
            )

        elif key == "e":
            if self.is_mysql_version_at_least("8.0") and self.performance_schema_enabled:
                self.app.push_screen(
                    EventLog(
                        self.read_only,
                        self.app_version,
                        self.mysql_host,
                        self.secondary_db_connection,
                    )
                )
            else:
                self.notify("Error log command requires MySQL 8+ with Performance Schema enabled")

        elif key == "f":

            def command_get_input(filter_data):
                filter_name, filter_value = filter_data[0], filter_data[1]
                filters_mapping = {
                    "User": "user_filter",
                    "Database": "db_filter",
                    "Host": "host_filter",
                    "Query time": "query_time_filter",
                    "Query text": "query_filter",
                }

                attribute = filters_mapping.get(filter_name)
                if attribute:
                    setattr(self, attribute, int(filter_value) if attribute == "query_time_filter" else filter_value)
                    self.notify(
                        f"Filtering [b]{filter_name.capitalize()}[/b] by [b highlight]{filter_value}[/b highlight]",
                        severity="success",
                    )
                else:
                    self.notify(f"Invalid filter name {filter_name}", severity="error")

            self.app.push_screen(
                CommandModal(
                    command="thread_filter",
                    message="Select which field you'd like to filter by",
                    processlist_data=self.processlist_threads_snapshot,
                    host_cache_data=self.host_cache,
                ),
                command_get_input,
            )

        elif key == "i":
            if self.show_idle_threads:
                self.show_idle_threads = False
                self.sort_by_time_descending = True

                self.notify("Processlist will now hide idle threads")
            else:
                self.show_idle_threads = True
                self.sort_by_time_descending = False

                self.notify("Processlist will now show idle threads")

        elif key == "k":

            def command_get_input(thread_id):
                try:
                    if self.aws_rds:
                        self.secondary_db_connection.cursor.execute("CALL mysql.rds_kill(%s)" % thread_id)
                    else:
                        self.secondary_db_connection.cursor.execute("KILL %s" % thread_id)

                    self.notify("Killed Thread ID [b highlight]%s[/b highlight]" % thread_id, severity="success")
                except Exception as e:
                    self.notify(e.args[1], title="Error killing Thread ID", severity="error")

            self.app.push_screen(
                CommandModal(
                    command="thread_kill_by_id",
                    message="Specify a Thread ID to kill",
                    processlist_data=self.processlist_threads_snapshot,
                ),
                command_get_input,
            )

        elif key == "K":

            def command_get_input(data):
                def execute_kill(thread_id):
                    query = "CALL mysql.rds_kill(%s)" if self.aws_rds else "KILL %s"
                    self.secondary_db_connection.cursor.execute(query % thread_id)

                kill_type, kill_value, include_sleeping_queries, lower_limit, upper_limit = data
                db_field = {"username": "user", "host": "host", "time_range": "time"}.get(kill_type)

                commands_to_kill = ["Query", "Execute"]

                if include_sleeping_queries:
                    commands_to_kill.append("Sleep")

                threads_killed = 0
                for thread_id, thread in self.processlist_threads_snapshot.items():
                    try:
                        if thread["command"] in commands_to_kill:
                            if kill_type == "time_range":
                                if lower_limit <= thread["time"] <= upper_limit:
                                    execute_kill(thread_id)
                                    threads_killed += 1
                            elif thread[db_field] == kill_value:
                                execute_kill(thread_id)
                                threads_killed += 1
                    except Exception as e:
                        self.notify(str(e), title="Error Killing Thread ID", severity="error")
                        return

                if threads_killed:
                    self.notify(f"Killed [highlight]{threads_killed}[/highlight] threads")
                else:
                    self.notify("No threads were killed")

            self.app.push_screen(
                CommandModal(
                    command="thread_kill_by_parameter",
                    message="Kill threads based around parameters",
                    processlist_data=self.processlist_threads_snapshot,
                ),
                command_get_input,
            )

        elif key == "l":
            deadlock = ""
            output = re.search(
                r"------------------------\nLATEST\sDETECTED\sDEADLOCK\n------------------------"
                "\n(.*?)------------\nTRANSACTIONS",
                self.secondary_db_connection.fetch_value_from_field(MySQLQueries.innodb_status, "Status"),
                flags=re.S,
            )
            if output:
                deadlock = output.group(1)

                deadlock = deadlock.replace("***", "[yellow]*****[/yellow]")
                screen_data = deadlock
            else:
                screen_data = Align.center("No deadlock detected")

        elif key == "o":
            screen_data = self.secondary_db_connection.fetch_value_from_field(MySQLQueries.innodb_status, "Status")

        elif key == "m":
            if not self.is_mysql_version_at_least("5.7") or not self.performance_schema_enabled:
                self.notify("Memory usage command requires MySQL 5.7+ with Performance Schema enabled")
                return

            table_grid = Table.grid()
            table1 = Table(box=box.ROUNDED, style="table_border")

            header_style = Style(bold=True)
            table1.add_column("User", header_style=header_style)
            table1.add_column("Current", header_style=header_style)
            table1.add_column("Total", header_style=header_style)

            self.secondary_db_connection.execute(MySQLQueries.memory_by_user)
            data = self.secondary_db_connection.fetchall()
            for row in data:
                table1.add_row(
                    row["user"],
                    format_sys_table_memory(row["current_allocated"]),
                    format_sys_table_memory(row["total_allocated"]),
                )

            table2 = Table(box=box.ROUNDED, style="table_border")
            table2.add_column("Code Area", header_style=header_style)
            table2.add_column("Current", header_style=header_style)

            self.secondary_db_connection.execute(MySQLQueries.memory_by_code_area)
            data = self.secondary_db_connection.fetchall()
            for row in data:
                table2.add_row(row["code_area"], format_sys_table_memory(row["current_allocated"]))

            table3 = Table(box=box.ROUNDED, style="table_border")
            table3.add_column("Host", header_style=header_style)
            table3.add_column("Current", header_style=header_style)
            table3.add_column("Total", header_style=header_style)

            self.secondary_db_connection.execute(MySQLQueries.memory_by_host)
            data = self.secondary_db_connection.fetchall()
            for row in data:
                table3.add_row(
                    self.get_hostname(row["host"]),
                    format_sys_table_memory(row["current_allocated"]),
                    format_sys_table_memory(row["total_allocated"]),
                )

            table_grid.add_row("", Align.center("[b]Memory Allocation[/b]"), "")
            table_grid.add_row(table1, table3, table2)

            screen_data = Align.center(table_grid)

        elif key == "p":
            if not self.pause_refresh:
                self.pause_refresh = True
                self.notify(f"Refresh is paused! Press [b highlight]{key}[/b highlight] again to resume")
            else:
                self.pause_refresh = False
                self.notify("Refreshing has resumed", severity="success")

        if key == "P":
            if self.use_performance_schema:
                self.use_performance_schema = False
                self.notify("Switched to using [b highlight]Processlist")
            else:
                if self.performance_schema_enabled:
                    self.use_performance_schema = True
                    self.notify("Switched to using [b highlight]Performance Schema")
                else:
                    self.notify("You can't switch to Performance Schema because it isn't enabled")

        elif key == "q":
            self.app.exit()

        elif key == "r":

            def command_get_input(refresh_interval):
                self.refresh_interval = refresh_interval

                self.notify(
                    f"Refresh interval set to [b highlight]{refresh_interval}[/b highlight] second(s)",
                    severity="success",
                )

            self.app.push_screen(
                CommandModal(command="refresh_interval", message="Specify refresh interval (in seconds)"),
                command_get_input,
            )

        elif key == "R":
            self.metric_manager = MetricManager()
            active_graph = self.app.query_one("#tabbed_content").active
            self.app.update_graphs(active_graph.split("tab_")[1])
            self.notify("Metrics have been reset", severity="success")

        elif key == "s":
            if self.sort_by_time_descending:
                self.sort_by_time_descending = False
                self.notify("Processlist will now sort threads by time in ascending order")
            else:
                self.sort_by_time_descending = True
                self.notify("Processlist will now sort threads by time in descending order")

        elif key == "t":

            def command_get_input(thread_id):
                elements = []

                thread_data = self.processlist_threads_snapshot.get(thread_id)
                if not thread_data:
                    self.notify("Thread ID was not found in processlist")
                    return

                table = Table(box=box.ROUNDED, show_header=False, style="table_border")
                table.add_column("")
                table.add_column("")

                table.add_row("[label]Thread ID", str(thread_id))
                table.add_row("[label]User", thread_data["user"])
                table.add_row("[label]Host", thread_data["host"])
                table.add_row("[label]Database", thread_data["db"])
                table.add_row("[label]Command", thread_data["command"])
                table.add_row("[label]State", thread_data["state"])
                table.add_row("[label]Time", str(timedelta(seconds=thread_data["time"])).zfill(8))
                table.add_row("[label]Rows Locked", thread_data["trx_rows_locked"])
                table.add_row("[label]Rows Modified", thread_data["trx_rows_modified"])

                if (
                    "innodb_thread_concurrency" in self.global_variables
                    and self.global_variables["innodb_thread_concurrency"]
                ):
                    table.add_row("[label]Tickets", thread_data["trx_concurrency_tickets"])

                table.add_row("", "")
                table.add_row("[label]TRX Time", thread_data["trx_time"])
                table.add_row("[label]TRX State", thread_data["trx_state"])
                table.add_row("[label]TRX Operation", thread_data["trx_operation_state"])
                elements.append(Group(Align.center("[b white]Thread Details[/b white]"), Align.center(table)))

                query = sqlformat(thread_data["query"], reindent_aligned=True)
                query_db = thread_data["db"]

                if query:
                    explain_failure = ""
                    explain_data = ""

                    formatted_query = Syntax(
                        query,
                        "sql",
                        line_numbers=False,
                        word_wrap=True,
                        theme="monokai",
                        background_color="#030918",
                    )

                    if query_db:
                        try:
                            self.secondary_db_connection.cursor.execute("USE %s" % query_db)
                            self.secondary_db_connection.cursor.execute("EXPLAIN %s" % query)

                            explain_data = self.secondary_db_connection.fetchall()
                        except pymysql.Error as e:
                            explain_failure = "[b indian_red]EXPLAIN ERROR:[/b indian_red] [indian_red]%s" % e.args[1]

                    if explain_data:
                        explain_table = Table(box=box.HEAVY_EDGE, style="table_border")

                        columns = []
                        for row in explain_data:
                            values = []
                            for column, value in row.items():
                                # Exclude possbile_keys field since it takes up too much space
                                if column == "possible_keys":
                                    continue

                                # Don't duplicate columns
                                if column not in columns:
                                    explain_table.add_column(column)
                                    columns.append(column)

                                if column == "key" and value is None:
                                    value = "[b white on #B30000]NO INDEX[/b white on #B30000]"

                                if column == "rows":
                                    value = format_number(value)

                                values.append(str(value))

                            explain_table.add_row(*values)

                        query_panel_title = "[b white]Query & Explain[/b white]"
                        query_panel_elements = Group(Align.center(formatted_query), "", Align.center(explain_table))
                    elif explain_failure:
                        query_panel_title = "[b white]Query & Explain[/b white]"
                        query_panel_elements = Group(Align.center(formatted_query), "", Align.center(explain_failure))
                    else:
                        query_panel_title = "[b white]Query[/b white]"
                        query_panel_elements = Align.center(formatted_query)

                    elements.append(
                        Panel(
                            query_panel_elements,
                            title=query_panel_title,
                            box=box.HORIZONTALS,
                            border_style="panel_border",
                        )
                    )

                # Transaction history
                transaction_history_title = ""
                transaction_history_table = Table(box=box.ROUNDED, style="table_border")
                if (
                    self.is_mysql_version_at_least("5.7")
                    and self.performance_schema_enabled
                    and thread_data["mysql_thread_id"]
                ):
                    query = MySQLQueries.thread_transaction_history.replace("$1", str(thread_data["mysql_thread_id"]))
                    self.secondary_db_connection.cursor.execute(query)
                    transaction_history = self.secondary_db_connection.fetchall()

                    if transaction_history:
                        transaction_history_title = "[b]Transaction History[/b]"
                        transaction_history_table.add_column("Start Time")
                        transaction_history_table.add_column("Query")

                        for query in transaction_history:
                            formatted_query = ""
                            if query["sql_text"]:
                                formatted_query = Syntax(
                                    re.sub(r"\s+", " ", query["sql_text"]),
                                    "sql",
                                    line_numbers=False,
                                    word_wrap=True,
                                    theme="monokai",
                                    background_color="#030918",
                                )

                            transaction_history_table.add_row(
                                query["start_time"].strftime("%Y-%m-%d %H:%M:%S"), formatted_query
                            )

                        elements.append(
                            Group(Align.center(transaction_history_title), Align.center(transaction_history_table))
                        )

                screen_data = Group(*[element for element in elements if element])

                self.app.push_screen(CommandScreen(self.read_only, self.app_version, self.mysql_host, screen_data))

            self.app.push_screen(
                CommandModal(
                    command="show_thread",
                    message="Specify a Thread ID to display its details",
                    processlist_data=self.processlist_threads_snapshot,
                ),
                command_get_input,
            )

        elif key == "T":
            if self.show_trxs_only:
                self.show_trxs_only = False
                self.show_idle_threads = False
                self.notify("Processlist will now no longer only show threads that have an active transaction")
            else:
                self.show_trxs_only = True
                self.show_idle_threads = True
                self.notify("Processlist will now only show threads that have an active transaction")

        elif key == "u":
            user_stat_data = self.create_user_stats_table()
            if user_stat_data:
                screen_data = Align.center(user_stat_data)
            else:
                self.notify("User statistics command requires Performance Schema to be enabled")

        elif key == "v":

            def command_get_input(input_variable):
                table_grid = Table.grid()
                table_counter = 1
                variable_counter = 1
                row_counter = 1
                variable_num = 1
                all_tables = []
                tables = {}
                display_global_variables = {}

                for variable, value in self.global_variables.items():
                    if input_variable == "all":
                        display_global_variables[variable] = self.global_variables[variable]
                    else:
                        if input_variable:
                            if input_variable in variable:
                                display_global_variables[variable] = self.global_variables[variable]

                max_num_tables = 1 if len(display_global_variables) <= 50 else 2

                # Create the number of tables we want
                while table_counter <= max_num_tables:
                    tables[table_counter] = Table(box=box.ROUNDED, show_header=False, style="table_border")
                    tables[table_counter].add_column("")
                    tables[table_counter].add_column("")

                    table_counter += 1

                # Calculate how many global_variables per table
                row_per_count = len(display_global_variables) // max_num_tables

                # Loop global_variables
                for variable, value in display_global_variables.items():
                    tables[variable_num].add_row("[label]%s" % variable, str(value))

                    if variable_counter == row_per_count and row_counter != max_num_tables:
                        row_counter += 1
                        variable_counter = 0
                        variable_num += 1

                    variable_counter += 1

                # Put all the variable data from dict into an array
                all_tables = [table_data for table_data in tables.values() if table_data]

                # Add the data into a single tuple for add_row
                if display_global_variables:
                    table_grid.add_row(*all_tables)
                    screen_data = Align.center(table_grid)

                    self.app.push_screen(CommandScreen(self.read_only, self.app_version, self.mysql_host, screen_data))
                else:
                    if input_variable:
                        self.notify("No variable(s) found that match [b highlight]%s[/b highlight]" % input_variable)

            self.app.push_screen(
                CommandModal(
                    command="variable_search",
                    message="Specify a variable to wildcard search\n[dim](input all to show everything)[/dim]",
                ),
                command_get_input,
            )

        elif key == "z":
            if self.host_cache:
                table = Table(box=box.ROUNDED, style="table_border")
                table.add_column("Host/IP")
                table.add_column("Hostname (if resolved)")

                for ip, addr in self.host_cache.items():
                    if ip:
                        table.add_row(ip, addr)

                screen_data = Group(
                    Align.center("[b]Host Cache[/b]"),
                    Align.center(table),
                    Align.center("Total: [b highlight]%s" % len(self.host_cache)),
                )
            else:
                screen_data = Align.center("\nThere are currently no hosts resolved")

        elif key == "question_mark":
            keys = {
                "`": "Quickly connect to another host",
                "a": "Toggle additional processlist columns",
                "c": "Clear all filters set",
                "d": "Display all databases",
                "e": "Display error log from Performance Schema",
                "f": "Filter processlist by a supported option",
                "i": "Toggle displaying idle threads",
                "k": "Kill a thread by its ID",
                "K": "Kill a thread by a supported option",
                "l": "Display the most recent deadlock",
                "o": "Display output from SHOW ENGINE INNODB STATUS",
                "m": "Display memory usage",
                "p": "Pause refreshing",
                "P": "Switch between using Information Schema/Performance Schema for processlist panel",
                "q": "Quit",
                "r": "Set the refresh interval",
                "R": "Reset all metrics",
                "t": "Display details of a thread along with an EXPLAIN of its query",
                "T": "Transaction view - toggle displaying threads that only have an active transaction",
                "s": "Sort processlist by time in descending/ascending order",
                "u": "List active connected users and their statistics",
                "v": "Variable wildcard search sourced from SHOW GLOBAL VARIABLES",
                "z": "Display all entries in the host cache",
            }

            table_keys = Table(box=box.HORIZONTALS, style="table_border", title="Commands", title_style="bold")
            table_keys.add_column("Key", justify="center", style="b highlight")
            table_keys.add_column("Description")

            for key, description in keys.items():
                table_keys.add_row(key, description)

            panels = {
                "1": "Show/hide Dashboard",
                "2": "Show/hide Processlist",
                "3": "Show/hide Replication/Replicas",
                "4": "Show/hide Graph Metrics",
            }
            table_panels = Table(box=box.HORIZONTALS, style="table_border", title="Panels", title_style="bold")
            table_panels.add_column("Key", justify="center", style="b highlight")
            table_panels.add_column("Description")
            for key, description in sorted(panels.items()):
                table_panels.add_row(key, description)

            datapoints = {
                "Read Only": "If the host is in read-only mode",
                "Read Hit": "The percentage of how many reads are from InnoDB buffer pool compared to from disk",
                "Lag": (
                    "Retrieves metric from: Default -> SHOW SLAVE STATUS, HB -> Heartbeat table, PS -> Performance"
                    " Schema"
                ),
                "Chkpt Age": (
                    "This depicts how close InnoDB is before it starts to furiously flush dirty data to disk "
                    "(Lower is better)"
                ),
                "AHI Hit": (
                    "The percentage of how many lookups there are from Adapative Hash Index compared to it not"
                    " being used"
                ),
                "Diff": "This is the size difference of the binary log between each refresh interval",
                "Cache Hit": "The percentage of how many binary log lookups are from cache instead of from disk",
                "History List": "History list length (number of un-purged row changes in InnoDB's undo logs)",
                "QPS": "Queries per second from Com_queries in SHOW GLOBAL STATUS",
                "Latency": "How much time it takes to receive data from the host for each refresh interval",
                "Threads": "Con = Connected, Run = Running, Cac = Cached from SHOW GLOBAL STATUS",
                "Speed": "How many seconds were taken off of replication lag from the last refresh interval",
                "Tickets": "Relates to innodb_concurrency_tickets variable",
                "R-Lock/Mod": "Relates to how many rows are locked/modified for the thread's transaction",
                "GR": "Group Replication",
            }

            table_terminology = Table(
                box=box.HORIZONTALS, style="table_border", title="Terminology", title_style="bold"
            )
            table_terminology.add_column("Datapoint", style="highlight")
            table_terminology.add_column("Description")
            for datapoint, description in sorted(datapoints.items()):
                table_terminology.add_row(datapoint, description)

            screen_data = Group(
                Align.center(table_keys),
                "",
                Align.center(table_panels),
                "",
                Align.center(table_terminology),
                "",
                Align.center(
                    "[light_blue][b]Note[/b]: Textual puts your terminal in application mode which disables selecting"
                    " text.\nTo see how to select text on your terminal, visit: https://tinyurl.com/dolphie-select-text"
                ),
            )

        if screen_data:
            self.app.push_screen(CommandScreen(self.read_only, self.app_version, self.mysql_host, screen_data))

    def create_user_stats_table(self):
        table = Table(header_style="bold white", box=box.ROUNDED, style="table_border")

        columns = {}
        user_stats = {}

        if self.performance_schema_enabled:
            self.secondary_db_connection.execute(MySQLQueries.ps_user_statisitics)

            columns.update(
                {
                    "User": {"field": "user", "format_number": False},
                    "Active": {"field": "current_connections", "format_number": True},
                    "Total": {"field": "total_connections", "format_number": True},
                    "Rows Read": {"field": "rows_read", "format_number": True},
                    "Rows Sent": {"field": "rows_sent", "format_number": True},
                    "Rows Updated": {"field": "rows_affected", "format_number": True},
                    "Tmp Tables": {"field": "created_tmp_tables", "format_number": True},
                    "Tmp Disk Tables": {"field": "created_tmp_disk_tables", "format_number": True},
                    "Plugin": {"field": "plugin", "format_number": False},
                    "Password Expire": {"field": "password_expires_in", "format_number": False},
                }
            )
        else:
            return False

        users = self.secondary_db_connection.fetchall()
        for user in users:
            username = user["user"]
            user_stats[username] = {
                "user": username,
                "total_connections": user["total_connections"],
                "current_connections": user["current_connections"],
                "password_expires_in": user["password_expires_in"],
                "plugin": user["plugin"],
                "rows_affected": user["sum_rows_affected"],
                "rows_sent": user["sum_rows_sent"],
                "rows_read": user["sum_rows_examined"],
                "created_tmp_disk_tables": user["sum_created_tmp_disk_tables"],
                "created_tmp_tables": user["sum_created_tmp_tables"],
            }

        for column, data in columns.items():
            table.add_column(column, no_wrap=True)

        for user_data in user_stats.values():
            row_values = []
            for column, data in columns.items():
                value = user_data.get(data["field"])

                if data["format_number"]:
                    row_values.append(format_number(value) if value else "")
                else:
                    row_values.append(value or "")

            table.add_row(*row_values)

        return table if user_stats else False

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

    def fetch_replication_data(self, replica_cursor=None):
        if self.heartbeat_table:
            query = MySQLQueries.heartbeat_replica_lag
            replica_lag_source = "HB"
        elif self.is_mysql_version_at_least("8.0") and self.performance_schema_enabled and not self.mariadb:
            query = MySQLQueries.ps_replica_lag
            replica_lag_source = "PS"
        else:
            query = MySQLQueries.replication_status
            replica_lag_source = None

        replica_lag_data = None
        if replica_cursor:
            replica_cursor.execute(query)
            replica_lag_data = replica_cursor.fetchone()
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
                    and self.display_replication_panel
                    and self.global_variables.get("slave_parallel_workers", 0) > 1
                ):
                    self.main_db_connection.execute(MySQLQueries.replication_applier_status)
                    self.replication_applier_status = self.main_db_connection.fetchall()

                # Save this for the errant TRX check
                self.replication_primary_server_uuid = self.replication_status.get("Master_UUID")

        replica_lag = None
        if replica_lag_data and replica_lag_data["Seconds_Behind_Master"] is not None:
            replica_lag = int(replica_lag_data["Seconds_Behind_Master"])

        if replica_cursor:
            return replica_lag_source, replica_lag
        else:
            # Save the previous replica lag for to determine Speed data point
            self.previous_replica_sbm = self.replica_lag

            self.replica_lag_source = replica_lag_source
            self.replica_lag = replica_lag
