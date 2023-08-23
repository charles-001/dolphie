import ipaddress
import os
import re
import socket
from datetime import datetime
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
        self.metric_manager = MetricManager()

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
        self.use_processlist: bool = False
        self.show_idle_threads: bool = False
        self.show_trxs_only: bool = False
        self.show_additional_query_columns: bool = False
        self.sort_by_time_descending: bool = True
        self.hide_dashboard: bool = False
        self.heartbeat_table: str = None
        self.user_filter: str = None
        self.db_filter: str = None
        self.host_filter: str = None
        self.query_time_filter: str = 0
        self.query_filter: str = None

        self.dolphie_start_time: datetime = datetime.now()
        self.worker_start_time: datetime = datetime.now()
        self.worker_previous_start_time: datetime = datetime.now()
        self.worker_job_time: int = 0
        self.processlist_threads: dict = {}
        self.processlist_threads_snapshot: dict = {}
        self.pause_refresh: bool = False
        self.previous_binlog_position: int = 0
        self.previous_replica_sbm: int = 0
        self.quick_switch_hosts: list = []
        self.host_cache: dict = {}
        self.host_cache_from_file: dict = {}
        self.innodb_metrics: dict = {}
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

        # Panel display states
        self.display_dashboard_panel: bool = False
        self.display_processlist_panel: bool = False
        self.display_replication_panel: bool = False
        self.display_graphs_panel: bool = False

        # Database connection global_variables
        # Main connection is used for Textual's worker thread so it can run asynchronous
        self.main_db_connection: Database = None
        # Secondary connection is for ad-hoc commands that are not a part of the worker thread
        self.secondary_db_connection: Database = None
        self.main_db_connection_id: int = None
        self.secondary_db_connection_id: int = None
        self.use_performance_schema: bool = False
        self.performance_schema_enabled: bool = False
        self.host_is_rds: bool = False
        self.host_is_cluster: bool = False
        self.server_uuid: str = None
        self.mysql_version: str = None
        self.host_distro: str = None

        # Misc
        self.footer_timer = None

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

    def update_footer(self, output, hide=False, temporary=True):
        if len(self.app.screen_stack) > 1:
            return

        footer = self.app.query_one("#footer")

        footer.display = True
        footer.update(output)

        if hide:
            footer.display = False
        elif temporary:
            # Stop existing timer if it exists
            if self.footer_timer and self.footer_timer._active:
                self.footer_timer.stop()
            self.footer_timer = self.app.set_timer(7, lambda: setattr(footer, "display", False))

    def db_connect(self):
        self.main_db_connection = Database(self.host, self.user, self.password, self.socket, self.port, self.ssl)
        self.secondary_db_connection = Database(self.host, self.user, self.password, self.socket, self.port, self.ssl)

        # Reduce any issues with the queries Dolphie runs (mostly targetting only_full_group_by)
        self.main_db_connection.execute("SET SESSION sql_mode = ''")
        self.secondary_db_connection.execute("SET SESSION sql_mode = ''")

        self.mysql_host = self.main_db_connection.fetch_value_from_field("SELECT @@hostname")

        self.main_db_connection_id = self.main_db_connection.fetch_value_from_field("SELECT CONNECTION_ID()")

        query = "SELECT CONNECTION_ID() AS connection_id"
        self.secondary_db_connection_id = self.secondary_db_connection.fetch_value_from_field("SELECT CONNECTION_ID()")

        performance_schema = self.main_db_connection.fetch_value_from_field("SELECT @@performance_schema")
        if performance_schema == 1:
            self.performance_schema_enabled = True

            if not self.use_processlist:
                self.use_performance_schema = True

        version_comment = self.main_db_connection.fetch_value_from_field("SELECT @@version_comment").lower()
        basedir = self.main_db_connection.fetch_value_from_field("SELECT @@basedir")

        aurora_version = None
        query = "SHOW GLOBAL VARIABLES LIKE 'aurora_version'"
        aurora_version_data = self.main_db_connection.fetch_value_from_field(query)
        if aurora_version_data:
            aurora_version = aurora_version_data

        version = self.main_db_connection.fetch_value_from_field("SELECT @@version").lower()
        version_split = version.split(".")

        self.mysql_version = "%s.%s.%s" % (
            version_split[0],
            version_split[1],
            version_split[2].split("-")[0],
        )
        major_version = int(version_split[0])

        # Get proper host version and fork
        if "percona xtradb cluster" in version_comment:
            self.host_distro = "Percona XtraDB Cluster"
            self.host_is_cluster = True
        elif "percona server" in version_comment:
            self.host_distro = "Percona Server"
        elif "mariadb cluster" in version_comment:
            self.host_distro = "MariaDB Cluster"
            self.host_is_cluster = True
        elif "mariadb" in version_comment or "mariadb" in version:
            self.host_distro = "MariaDB"
        elif aurora_version:
            self.host_distro = "Amazon Aurora"
            self.host_is_rds = True
        elif "rdsdb" in basedir:
            self.host_distro = "Amazon RDS"
            self.host_is_rds = True
        else:
            self.host_distro = "MySQL"

        server_uuid_query = "SELECT @@server_uuid"
        if "MariaDB" in self.host_distro and major_version >= 10:
            server_uuid_query = "SELECT @@server_id"
        self.server_uuid = self.main_db_connection.fetch_value_from_field(server_uuid_query)

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

    def capture_key(self, key):
        screen_data = None

        if not self.main_db_connection:
            self.update_footer("Database connection must be established before using commands")

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

            self.update_footer("Cleared all filters")

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
                tables[table_counter] = Table(box=box.ROUNDED, show_header=False, style="#52608d")
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
                Align.center("Total: [b #91abec]%s[/b #91abec]" % db_count),
            )

        elif key == "e":
            if self.is_mysql_version_at_least("8.0"):
                self.app.push_screen(
                    EventLog(self.app_version, f"{self.mysql_host}:{self.port}", self.secondary_db_connection)
                )
            else:
                self.update_footer("Error log command requires MySQL 8")

        elif key == "f":

            def command_get_input(filter_data):
                filter_name = filter_data[0]
                filter_value = filter_data[1]

                if filter_name == "user":
                    self.user_filter = next(
                        (
                            data["user"]
                            for data in self.processlist_threads_snapshot.values()
                            if filter_value == data["user"]
                        ),
                        None,
                    )
                    if not self.user_filter:
                        self.update_footer(
                            f"[indian_red]User[/indian_red] {filter_value}[indian_red] was not found in processlist"
                        )
                        return
                elif filter_name == "database":
                    self.db_filter = next(
                        (
                            data["db"]
                            for data in self.processlist_threads_snapshot.values()
                            if filter_value == data["db"]
                        ),
                        None,
                    )
                    if not self.db_filter:
                        self.update_footer(
                            f"[indian_red]Database[/indian_red] {filter_value}[indian_red] was not found in processlist"
                        )
                        return
                elif filter_name == "host":
                    self.host_filter = next((ip for ip, addr in self.host_cache.items() if filter_value == addr), None)
                    if not self.host_filter:
                        self.update_footer(
                            f"[indian_red]Host[/indian_red] {filter_value}[indian_red] was not found in processlist"
                        )
                        return
                elif filter_name == "query_time":
                    if filter_value.isnumeric():
                        self.query_time_filter = int(filter_value)
                    else:
                        self.update_footer("[bright_red]Query time must be an integer!")
                        return
                elif filter_name == "query_text":
                    self.query_filter = filter_value

                self.update_footer("Now filtering %s by [b #91abec]%s[/b #91abec]" % (filter_name, filter_value))

            self.app.push_screen(
                CommandModal(
                    message="Select which field you'd like to filter by",
                    show_filter_options=True,
                    processlist_data=self.processlist_threads_snapshot,
                ),
                command_get_input,
            )

        elif key == "i":
            if self.show_idle_threads:
                self.show_idle_threads = False
                self.sort_by_time_descending = True

                self.update_footer("Processlist will now hide idle threads")
            else:
                self.show_idle_threads = True
                self.sort_by_time_descending = False

                self.update_footer("Processlist will now show idle threads")

        elif key == "k":

            def command_get_input(thread_id):
                if not thread_id:
                    self.update_footer("[indian_red]You did not specify a Thread ID")
                    return

                if thread_id in self.processlist_threads_snapshot:
                    try:
                        if self.host_is_rds:
                            self.secondary_db_connection.cursor.execute("CALL mysql.rds_kill(%s)" % thread_id)
                        else:
                            self.secondary_db_connection.cursor.execute("KILL %s" % thread_id)

                        self.update_footer("Killed thread [b #91abec]%s[/b #91abec]" % thread_id)
                    except Exception as e:
                        self.update_footer("[b][indian_red]Error killing query[/b]: %s" % e.args[1])
                else:
                    self.update_footer("Thread ID [b #91abec]%s[/b #91abec] does not exist" % thread_id)

            self.app.push_screen(
                CommandModal(message="Specify a Thread ID to kill", processlist_data=self.processlist_threads_snapshot),
                command_get_input,
            )

        elif key == "K":

            def command_get_input(data):
                def execute_kill(thread_id):
                    if self.host_is_rds:
                        self.secondary_db_connection.cursor.execute("CALL mysql.rds_kill(%s)" % thread_id)
                    else:
                        self.secondary_db_connection.cursor.execute("KILL %s" % thread_id)

                kill_type = data[0]
                kill_value = data[1]
                include_sleeping_queries = data[2]

                if not kill_value:
                    self.update_footer("[indian_red]You did not specify a %s" % kill_type)
                    return

                if kill_type == "username":
                    key = "user"
                elif kill_type == "host":
                    key = "host"
                elif kill_type == "time_range":
                    key = "time"
                    if re.search(r"(\d+-\d+)", kill_value):
                        time_range = kill_value.split("-")
                        lower_limit = int(time_range[0])
                        upper_limit = int(time_range[1])

                        if lower_limit > upper_limit:
                            self.update_footer("[indian_red]Invalid time range! Lower limit can't be higher than upper")
                            return
                    else:
                        self.update_footer("[indian_red]Invalid time range")
                        return
                else:
                    self.update_footer("[indian_red]Invalid option")
                    return

                threads_killed = 0
                commands_to_kill = ["Query", "Execute"]
                if include_sleeping_queries:
                    commands_to_kill.append("Sleep")

                for thread_id, thread in self.processlist_threads_snapshot.items():
                    try:
                        if thread["command"] in commands_to_kill:
                            if kill_type == "time_range":
                                if thread["time"] >= lower_limit and thread["time"] <= upper_limit:
                                    execute_kill(thread_id)
                                    threads_killed += 1
                            else:
                                if thread[key] == kill_value:
                                    execute_kill(thread_id)
                                    threads_killed += 1
                    except Exception as e:
                        self.update_footer("[b][indian_red]Error killing query[/b]: %s" % e.args[1])
                        return

                if threads_killed:
                    self.update_footer("Killed [#91abec]%s[/#91abec] threads" % threads_killed)
                else:
                    self.update_footer("No threads were killed")

            self.app.push_screen(
                CommandModal(
                    message="Kill threads based around parameters",
                    show_kill_options=True,
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

                deadlock = deadlock.replace("***", "[#f1fb82]*****[/#f1fb82]")
                screen_data = deadlock
            else:
                screen_data = Align.center("No deadlock detected")

        elif key == "o":
            screen_data = self.secondary_db_connection.fetch_value_from_field(MySQLQueries.innodb_status, "Status")

        elif key == "m":
            table_line_color = "#52608d"

            table_grid = Table.grid()

            table1 = Table(
                box=box.ROUNDED,
                style=table_line_color,
            )

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

            table2 = Table(
                box=box.ROUNDED,
                style=table_line_color,
            )
            table2.add_column("Code Area", header_style=header_style)
            table2.add_column("Current", header_style=header_style)

            self.secondary_db_connection.execute(MySQLQueries.memory_by_code_area)
            data = self.secondary_db_connection.fetchall()
            for row in data:
                table2.add_row(row["code_area"], format_sys_table_memory(row["current_allocated"]))

            table3 = Table(
                box=box.ROUNDED,
                style=table_line_color,
            )
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
                self.update_footer(
                    f"Refresh is paused! Press [b #91abec]{key}[/b #91abec] again to resume",
                    temporary=False,
                )
            else:
                self.pause_refresh = False
                self.update_footer("", hide=True)

        if key == "P":
            if self.use_performance_schema:
                self.use_performance_schema = False
                self.update_footer("Switched to using [b #91abec]Processlist")
            else:
                if self.performance_schema_enabled:
                    self.use_performance_schema = True
                    self.update_footer("Switched to using [b #91abec]Performance Schema")
                else:
                    self.update_footer("[indian_red]You can't switch to Performance Schema because it isn't enabled")

        elif key == "q":
            self.app.exit()

        elif key == "r":

            def command_get_input(refresh_interval):
                if refresh_interval.isnumeric():
                    self.refresh_interval = int(refresh_interval)
                else:
                    self.update_footer("[indian_red]Input must be an integer")

            self.app.push_screen(
                CommandModal(message="Specify refresh interval (in seconds)"),
                command_get_input,
            )

        elif key == "R":
            self.metric_manager = MetricManager()
            active_graph = self.app.query_one("#tabbed_content").active
            self.app.update_graphs(active_graph.split("tab_")[1])
            self.update_footer("Metrics have been reset")

        elif key == "s":
            if self.sort_by_time_descending:
                self.sort_by_time_descending = False
                self.update_footer("Processlist will now sort queries in ascending order")
            else:
                self.sort_by_time_descending = True
                self.update_footer("Processlist will now sort queries in descending order")

        elif key == "t":

            def command_get_input(thread_id):
                if thread_id:
                    if thread_id in self.processlist_threads_snapshot:
                        thread_data = self.processlist_threads_snapshot[thread_id]

                        table = Table(box=box.ROUNDED, show_header=False, style="#52608d")
                        table.add_column("")
                        table.add_column("")

                        table.add_row("[#c5c7d2]Thread ID", str(thread_id))
                        table.add_row("[#c5c7d2]User", thread_data["user"])
                        table.add_row("[#c5c7d2]Host", thread_data["host"])
                        table.add_row("[#c5c7d2]Database", thread_data["db"])
                        table.add_row("[#c5c7d2]Command", thread_data["command"])
                        table.add_row("[#c5c7d2]State", thread_data["state"])
                        table.add_row("[#c5c7d2]Time", thread_data["formatted_time_with_days"])
                        table.add_row("[#c5c7d2]Rows Locked", thread_data["trx_rows_locked"])
                        table.add_row("[#c5c7d2]Rows Modified", thread_data["trx_rows_modified"])

                        # Transaction history
                        transaction_history_title = ""
                        transaction_history_table = Table(box=box.ROUNDED, style="#52608d")
                        if self.is_mysql_version_at_least("5.7") and self.use_performance_schema:
                            query = MySQLQueries.thread_transaction_history.replace(
                                "$placeholder", str(thread_data["mysql_thread_id"])
                            )
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
                                            background_color="#000718",
                                        )

                                    transaction_history_table.add_row(
                                        query["start_time"].strftime("%Y-%m-%d %H:%M:%S"), formatted_query
                                    )

                        if (
                            "innodb_thread_concurrency" in self.global_variables
                            and self.global_variables["innodb_thread_concurrency"]
                        ):
                            table.add_row("[#c5c7d2]Tickets", thread_data["trx_concurrency_tickets"])

                        table.add_row("", "")
                        table.add_row("[#c5c7d2]TRX State", thread_data["trx_state"])
                        table.add_row("[#c5c7d2]TRX Operation", thread_data["trx_operation_state"])

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
                                background_color="#000718",
                            )

                            if query_db:
                                try:
                                    self.secondary_db_connection.cursor.execute("USE %s" % query_db)
                                    self.secondary_db_connection.cursor.execute("EXPLAIN %s" % query)

                                    explain_data = self.secondary_db_connection.fetchall()
                                except pymysql.Error as e:
                                    explain_failure = (
                                        "[b indian_red]EXPLAIN ERROR:[/b indian_red] [indian_red]%s" % e.args[1]
                                    )

                            if explain_data:
                                explain_table = Table(box=box.ROUNDED, style="#52608d")

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
                                            value = "[b white on red]NO INDEX[/b white on red]"

                                        if column == "rows":
                                            value = format_number(value)

                                        values.append(str(value))

                                    explain_table.add_row(*values)

                                screen_data = Group(
                                    Align.center(table),
                                    "",
                                    Align.center(formatted_query),
                                    "",
                                    Align.center(explain_table),
                                    "",
                                    Align.center(transaction_history_title),
                                    Align.center(transaction_history_table),
                                )
                            else:
                                screen_data = Group(
                                    Align.center(table),
                                    "",
                                    Align.center(formatted_query),
                                    "",
                                    Align.center(explain_failure),
                                    "",
                                    Align.center(transaction_history_title),
                                    Align.center(transaction_history_table),
                                )
                        else:
                            screen_data = Group(
                                Align.center(table),
                                "",
                                Align.center(transaction_history_title),
                                Align.center(transaction_history_table),
                            )

                        self.app.push_screen(
                            CommandScreen(self.app_version, f"{self.mysql_host}:{self.port}", screen_data)
                        )
                    else:
                        self.update_footer("Thread ID [b #91abec]%s[/b #91abec] does not exist" % thread_id)

            self.app.push_screen(
                CommandModal(
                    message="Specify a Thread ID to display its details",
                    processlist_data=self.processlist_threads_snapshot,
                ),
                command_get_input,
            )

        elif key == "T":
            if self.show_trxs_only:
                self.show_trxs_only = False
                self.update_footer("Processlist will now no longer only show threads that have an active transaction")
            else:
                self.show_trxs_only = True
                self.update_footer("Processlist will now only show threads that have an active transaction")

        elif key == "u":
            user_stat_data = self.create_user_stats_table()
            if user_stat_data:
                screen_data = Align.center(user_stat_data)
            else:
                self.update_footer(
                    "[b indian_red]Cannot use this command![/b indian_red] It requires Performance Schema to be enabled"
                )

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

                variable_data = self.secondary_db_connection.fetch_data("variables")
                for variable, value in variable_data.items():
                    if input_variable:
                        if input_variable in variable:
                            display_global_variables[variable] = variable_data[variable]
                    else:
                        display_global_variables[variable] = variable_data[variable]

                max_num_tables = 1 if len(display_global_variables) <= 50 else 2

                # Create the number of tables we want
                while table_counter <= max_num_tables:
                    tables[table_counter] = Table(box=box.ROUNDED, show_header=False, style="#52608d")
                    tables[table_counter].add_column("")
                    tables[table_counter].add_column("")

                    table_counter += 1

                # Calculate how many global_variables per table
                row_per_count = len(display_global_variables) // max_num_tables

                # Loop global_variables
                for variable, value in display_global_variables.items():
                    tables[variable_num].add_row("[#c5c7d2]%s" % variable, str(value))

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

                    self.app.push_screen(CommandScreen(self.app_version, f"{self.mysql_host}:{self.port}", screen_data))
                else:
                    if input_variable:
                        self.update_footer("No variable(s) found that match [b #91abec]%s[/b #91abec]" % input_variable)

            self.app.push_screen(
                CommandModal(message="Specify a variable to wildcard search\n[dim](leave blank for all)[/dim]"),
                command_get_input,
            )

        elif key == "z":
            if self.host_cache:
                table = Table(box=box.ROUNDED, style="#52608d")
                table.add_column("Host/IP")
                table.add_column("Hostname (if resolved)")

                for ip, addr in self.host_cache.items():
                    if ip:
                        table.add_row(ip, addr)

                screen_data = Group(
                    Align.center("[b]Host Cache[/b]"),
                    Align.center(table),
                    Align.center("Total: [b #91abec]%s" % len(self.host_cache)),
                )
            else:
                screen_data = Align.center("\nThere are currently no hosts resolved")

        elif key == "question_mark":
            table_line_color = "#52608d"

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
                "P": "Switch between using SHOW PROCESSLIST/Performance Schema for processlist panel",
                "q": "Quit",
                "r": "Set the refresh interval",
                "R": "Reset all metrics",
                "t": "Display details of a thread along with an EXPLAIN of its query",
                "T": "Toggle displaying threads that only have an active transaction",
                "s": "Sort processlist by time in descending/ascending order",
                "u": "List active connected users and their statistics",
                "v": "Variable wildcard search sourced from SHOW GLOBAL VARIABLES",
                "z": "Display all entries in the host cache",
            }

            table_keys = Table(box=box.HORIZONTALS, style=table_line_color, title="Commands", title_style="bold")
            table_keys.add_column("Key", justify="center", style="b #91abec")
            table_keys.add_column("Description")

            for key, description in keys.items():
                table_keys.add_row(key, description)

            panels = {
                "1": "Show/hide Dashboard",
                "2": "Show/hide Processlist",
                "3": "Show/hide Replication/Replicas",
                "4": "Show/hide Graph Metrics",
            }
            table_panels = Table(box=box.HORIZONTALS, style=table_line_color, title="Panels", title_style="bold")
            table_panels.add_column("Key", justify="center", style="b #91abec")
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
            }

            table_terminology = Table(
                box=box.HORIZONTALS, style=table_line_color, title="Terminology", title_style="bold"
            )
            table_terminology.add_column("Datapoint", style="#91abec")
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
                    "[#bbc8e8][b]Note[/b]: Textual puts your terminal in application mode which disables selecting"
                    " text.\nTo see how to select text on your terminal, visit: https://tinyurl.com/dolphie-select-text"
                ),
            )

        if screen_data:
            self.app.push_screen(CommandScreen(self.app_version, f"{self.mysql_host}:{self.port}", screen_data))

    def create_user_stats_table(self):
        table = Table(header_style="bold white", box=box.ROUNDED, style="#52608d")

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
        elif self.is_mysql_version_at_least("8.0") and self.performance_schema_enabled:
            query = MySQLQueries.ps_replica_lag
            replica_lag_source = "PS"
        else:
            query = MySQLQueries.replication_status
            replica_lag_source = None

        if replica_cursor:
            replica_cursor.execute(query)
            replica_lag_data = replica_cursor.fetchone()
        else:
            # Determine if this server is a replica or not
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

        if replica_lag_data:
            replica_lag = int(replica_lag_data["Seconds_Behind_Master"])

            if replica_lag < 0:
                replica_lag = 0
        else:
            replica_lag = 0

        if replica_cursor:
            return replica_lag_source, replica_lag
        else:
            # Save the previous replica lag for to determine Speed data point
            self.previous_replica_sbm = self.replica_lag

            self.replica_lag_source = replica_lag_source
            self.replica_lag = replica_lag
