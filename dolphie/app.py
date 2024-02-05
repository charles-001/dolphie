#!/usr/bin/env python3

# ****************************
# *        Dolphie           *
# * Author: Charles Thompson *
# ****************************

import os
import re
import sys
from argparse import ArgumentParser, RawTextHelpFormatter
from configparser import ConfigParser
from datetime import datetime, timedelta
from functools import partial
from urllib.parse import urlparse

import dolphie.Modules.MetricManager as MetricManager
import myloginpath
from dolphie import Dolphie
from dolphie.DataTypes import ProcesslistThread
from dolphie.Modules.Functions import format_number, format_sys_table_memory
from dolphie.Modules.ManualException import ManualException
from dolphie.Modules.Queries import MySQLQueries
from dolphie.Modules.TabManager import Tab, TabManager
from dolphie.Panels import (
    dashboard_panel,
    ddl_panel,
    locks_panel,
    processlist_panel,
    replication_panel,
)
from dolphie.Widgets.command_screen import CommandScreen
from dolphie.Widgets.event_log_screen import EventLog
from dolphie.Widgets.modal import CommandModal
from dolphie.Widgets.thread_screen import ThreadScreen
from dolphie.Widgets.topbar import TopBar
from rich import box
from rich.align import Align
from rich.console import Console, Group
from rich.prompt import Prompt
from rich.style import Style
from rich.syntax import Syntax
from rich.table import Table
from rich.theme import Theme
from rich.traceback import Traceback
from sqlparse import format as sqlformat
from textual import events, on, work
from textual.app import App, ComposeResult
from textual.css.query import NoMatches
from textual.widgets import Switch, TabbedContent
from textual.worker import Worker, WorkerState, get_current_worker


def parse_args(dolphie: Dolphie):
    epilog = """
Config file with [client] section supports these options:
    host
    user
    password
    port
    socket
    ssl_mode REQUIRED/VERIFY_CA/VERIFY_IDENTITY
    ssl_ca
    ssl_cert
    ssl_key

Login path file supports these options:
    host
    user
    password
    port
    socket

Environment variables support these options:
    DOLPHIE_USER
    DOLPHIE_PASSWORD
    DOLPHIE_HOST
    DOLPHIE_PORT
    DOLPHIE_SOCKET

"""
    parser = ArgumentParser(
        conflict_handler="resolve",
        description="Dolphie, an intuitive feature-rich top tool for monitoring MySQL in real time",
        epilog=epilog,
        formatter_class=RawTextHelpFormatter,
    )

    parser.add_argument(
        "uri",
        metavar="uri",
        type=str,
        nargs="?",
        help=(
            "Use a URI string for credentials - format: mysql://user:password@host:port (port is optional with"
            " default 3306)"
        ),
    )

    parser.add_argument(
        "-u",
        "--user",
        dest="user",
        type=str,
        help="Username for MySQL",
    )
    parser.add_argument("-p", "--password", dest="password", type=str, help="Password for MySQL")
    parser.add_argument(
        "--ask-pass",
        dest="ask_password",
        action="store_true",
        default=False,
        help="Ask for password (hidden text)",
    )
    parser.add_argument(
        "-h",
        "--host",
        dest="host",
        type=str,
        help="Hostname/IP address for MySQL",
    )
    parser.add_argument(
        "-P",
        "--port",
        dest="port",
        type=int,
        help="Port for MySQL (Socket has precendence)",
    )
    parser.add_argument(
        "-S",
        "--socket",
        dest="socket",
        type=str,
        help="Socket file for MySQL",
    )
    parser.add_argument(
        "-c",
        "--config-file",
        dest="config_file",
        type=str,
        help=(
            "Config file path to use. This should use [client] section. "
            "See below for options support [default: ~/.my.cnf]"
        ),
    )
    parser.add_argument(
        "-f",
        "--host-cache-file",
        dest="host_cache_file",
        type=str,
        help=(
            "Resolve IPs to hostnames when your DNS is unable to. Each IP/hostname pair should be on its own line "
            "using format: ip=hostname [default: ~/dolphie_host_cache]"
        ),
    )
    parser.add_argument(
        "-q",
        "--host-setup-file",
        dest="host_setup_file",
        type=str,
        help=(
            "Specify location of file that stores the available hosts to use in host setup modal [default:"
            " ~/dolphie_hosts]"
        ),
    )
    parser.add_argument(
        "-l",
        "--login-path",
        dest="login_path",
        default="client",
        type=str,
        help=(
            "Specify login path to use mysql_config_editor's file ~/.mylogin.cnf for encrypted login credentials. "
            "Supercedes config file [default: %(default)s]"
        ),
    )
    parser.add_argument(
        "-r",
        "--refresh_interval",
        dest="refresh_interval",
        default=1,
        type=int,
        help="How much time to wait in seconds between each refresh [default: %(default)s]",
    )
    parser.add_argument(
        "-H",
        "--heartbeat-table",
        dest="heartbeat_table",
        type=str,
        help=(
            "If your hosts use pt-heartbeat, specify table in format db.table to use the timestamp it "
            "has for replication lag instead of Seconds_Behind_Master from SHOW SLAVE STATUS"
        ),
    )
    parser.add_argument(
        "--ssl-mode",
        dest="ssl_mode",
        type=str,
        help=(
            "Desired security state of the connection to the host. Supports: "
            "REQUIRED/VERIFY_CA/VERIFY_IDENTITY [default: OFF]"
        ),
    )
    parser.add_argument(
        "--ssl-ca",
        dest="ssl_ca",
        type=str,
        help="Path to the file that contains a PEM-formatted CA certificate",
    )
    parser.add_argument(
        "--ssl-cert",
        dest="ssl_cert",
        type=str,
        help="Path to the file that contains a PEM-formatted client certificate",
    )
    parser.add_argument(
        "--ssl-key",
        dest="ssl_key",
        type=str,
        help="Path to the file that contains a PEM-formatted private key for the client certificate",
    )
    parser.add_argument(
        "--panels",
        dest="startup_panels",
        default="dashboard,processlist",
        type=str,
        help=(
            "What panels to display on startup separated by a comma. Supports:"
            f" {'/'.join(dolphie.panels.all())} [default: %(default)s]"
        ),
    )
    parser.add_argument(
        "--graph-marker",
        dest="graph_marker",
        default="braille",
        type=str,
        help=(
            "What marker to use for graphs (available options: https://tinyurl.com/dolphie-markers) [default:"
            " %(default)s]"
        ),
    )
    parser.add_argument(
        "--show-trxs-only",
        dest="show_trxs_only",
        action="store_true",
        default=False,
        help="Start with only showing threads that have an active transaction",
    )
    parser.add_argument(
        "--additional-columns",
        dest="show_additional_query_columns",
        action="store_true",
        default=False,
        help="Start with additional columns in Processlist panel",
    )
    parser.add_argument(
        "--use-processlist",
        dest="use_processlist",
        action="store_true",
        default=False,
        help="Start with using Information Schema instead of Performance Schema for processlist panel",
    )
    parser.add_argument(
        "-V", "--version", action="version", version=dolphie.app_version, help="Display version and exit"
    )

    console = Console(style="indian_red", highlight=False)

    home_dir = os.path.expanduser("~")

    parameter_options = vars(parser.parse_args())  # Convert object to dict
    basic_options = ["user", "password", "host", "port", "socket"]

    dolphie.config_file = f"{home_dir}/.my.cnf"
    if parameter_options["config_file"]:
        dolphie.config_file = parameter_options["config_file"]

    # Use config file for login credentials
    if os.path.isfile(dolphie.config_file):
        cfg = ConfigParser()
        cfg.read(dolphie.config_file)

        for option in basic_options:
            if cfg.has_option("client", option):
                setattr(dolphie, option, cfg.get("client", option))

        if cfg.has_option("client", "ssl_mode"):
            ssl_mode = cfg.get("client", "ssl_mode").upper()

            if ssl_mode == "REQUIRED":
                dolphie.ssl[""] = True
            elif ssl_mode == "VERIFY_CA":
                dolphie.ssl["check_hostname"] = False
            elif ssl_mode == "VERIFY_IDENTITY":
                dolphie.ssl["check_hostname"] = True
            else:
                sys.exit(console.print(f"Unsupported SSL mode [b]{ssl_mode}[/b]"))

        if cfg.has_option("client", "ssl_ca"):
            dolphie.ssl["ca"] = cfg.get("client", "ssl_ca")
        if cfg.has_option("client", "ssl_cert"):
            dolphie.ssl["cert"] = cfg.get("client", "ssl_cert")
        if cfg.has_option("client", "ssl_key"):
            dolphie.ssl["key"] = cfg.get("client", "ssl_key")

    # Use login path for login credentials
    if parameter_options["login_path"]:
        try:
            login_path_data = myloginpath.parse(parameter_options["login_path"])

            for option in basic_options:
                if option in login_path_data:
                    setattr(dolphie, option, login_path_data[option])
        except Exception as e:
            # Don't error out for default login path
            if parameter_options["login_path"] != "client":
                sys.exit(console.print(f"Problem reading login path file: {e}"))

    # Use environment variables for basic options if specified
    for option in basic_options:
        environment_var = "DOLPHIE_%s" % option.upper()
        if environment_var in os.environ and os.environ[environment_var]:
            setattr(dolphie, option, os.environ[environment_var])

    # Use parameter options if specified
    for option in basic_options:
        if parameter_options[option]:
            setattr(dolphie, option, parameter_options[option])

    # Lastly, parse URI if specified
    if parameter_options["uri"]:
        try:
            parsed = urlparse(parameter_options["uri"])

            if parsed.scheme != "mysql":
                sys.exit(
                    console.print("Invalid URI scheme: Only 'mysql' is supported (see --help for more information)")
                )

            dolphie.user = parsed.username
            dolphie.password = parsed.password
            dolphie.host = parsed.hostname
            dolphie.port = parsed.port or 3306
        except Exception as e:
            sys.exit(console.print(f"Invalid URI: {e} (see --help for more information)"))

    if parameter_options["ask_password"]:
        dolphie.password = Prompt.ask("[b #91abec]Password", password=True)

    if not dolphie.host:
        dolphie.host = "localhost"

    if parameter_options["refresh_interval"]:
        dolphie.refresh_interval = parameter_options["refresh_interval"]

    if parameter_options["heartbeat_table"]:
        pattern_match = re.search(r"^(\w+\.\w+)$", parameter_options["heartbeat_table"])
        if pattern_match:
            dolphie.heartbeat_table = parameter_options["heartbeat_table"]
            MySQLQueries.heartbeat_replica_lag = MySQLQueries.heartbeat_replica_lag.replace(
                "$1", dolphie.heartbeat_table
            )
        else:
            sys.exit(console.print("Your heartbeat table did not conform to the proper format: db.table"))

    if parameter_options["ssl_mode"]:
        ssl_mode = parameter_options["ssl_mode"].upper()

        if ssl_mode == "REQUIRED":
            dolphie.ssl[""] = True
        elif ssl_mode == "VERIFY_CA":
            dolphie.ssl["check_hostame"] = False
        elif ssl_mode == "VERIFY_IDENTITY":
            dolphie.ssl["check_hostame"] = True
        else:
            sys.exit(console.print(f"Unsupported SSL mode [b]{ssl_mode}[/b]"))

    if parameter_options["ssl_ca"]:
        dolphie.ssl["ca"] = parameter_options["ssl_ca"]
    if parameter_options["ssl_cert"]:
        dolphie.ssl["cert"] = parameter_options["ssl_cert"]
    if parameter_options["ssl_key"]:
        dolphie.ssl["key"] = parameter_options["ssl_key"]

    dolphie.host_cache_file = f"{home_dir}/dolphie_host_cache"
    if parameter_options["host_cache_file"]:
        dolphie.host_cache_file = parameter_options["host_cache_file"]

    dolphie.host_setup_file = f"{home_dir}/dolphie_hosts"
    if parameter_options["host_setup_file"]:
        dolphie.host_setup_file = parameter_options["host_setup_file"]

    dolphie.show_trxs_only = parameter_options["show_trxs_only"]
    dolphie.show_additional_query_columns = parameter_options["show_additional_query_columns"]

    if parameter_options["use_processlist"]:
        dolphie.use_performance_schema = False

    dolphie.startup_panels = parameter_options["startup_panels"].split(",")
    for panel in dolphie.startup_panels:
        if panel not in dolphie.panels.all():
            sys.exit(console.print(f"Panel '{panel}' is not valid (see --help for more information)"))

    dolphie.graph_marker = parameter_options["graph_marker"]

    if os.path.exists(dolphie.host_setup_file):
        with open(dolphie.host_setup_file, "r") as file:
            dolphie.host_setup_available_hosts = [line.strip() for line in file]


class DolphieApp(App):
    TITLE = "Dolphie"
    CSS_PATH = "Dolphie.css"

    def __init__(self, dolphie: Dolphie):
        super().__init__()

        dolphie.app = self
        self.dolphie = dolphie
        self.tab: Tab = None

        theme = Theme({
            "white": "#e9e9e9",
            "green": "#54efae",
            "yellow": "#f6ff8f",
            "red": "#fd8383",
            "purple": "#b565f3",
            "dark_gray": "#969aad",
            "highlight": "#91abec",
            "label": "#c5c7d2",
            "b label": "b #c5c7d2",
            "light_blue": "#bbc8e8",
            "b white": "b #e9e9e9",
            "b highlight": "b #91abec",
            "b red": "b #fd8383",
            "b light_blue": "b #bbc8e8",
            "panel_border": "#6171a6",
            "table_border": "#333f62",
        })
        self.console.push_theme(theme)
        self.console.set_window_title(self.TITLE)

    @work(thread=True, group="main")
    def run_worker_main(self, tab_id: int):
        tab = self.tab_manager.get_tab(tab_id)

        # Get our worker thread
        tab.worker = get_current_worker()
        tab.worker.name = tab_id

        # We have to queue the tab for removal because if we don't do it in the worker thread it will
        # be removed while the worker thread is running which will error due to objects not existing
        if tab.queue_for_removal:
            tab.disconnect(update_topbar=False)
            self.tab_manager.tabs.pop(tab.id, None)

            return

        dolphie = tab.dolphie
        try:
            if not dolphie.main_db_connection or not dolphie.main_db_connection.is_connected():
                tab.update_topbar(f"[[white]CONNECTING[/white]] {dolphie.host}:{dolphie.port}")
                tab.loading_indicator.display = True

                dolphie.db_connect()

            dolphie.worker_start_time = datetime.now()
            dolphie.polling_latency = (dolphie.worker_start_time - dolphie.worker_previous_start_time).total_seconds()
            dolphie.refresh_latency = round(dolphie.polling_latency - dolphie.refresh_interval, 2)
            dolphie.worker_previous_start_time = dolphie.worker_start_time

            dolphie.global_variables = dolphie.main_db_connection.fetch_status_and_variables("variables")
            dolphie.global_status = dolphie.main_db_connection.fetch_status_and_variables("status")
            dolphie.innodb_metrics = dolphie.main_db_connection.fetch_status_and_variables("innodb_metrics")

            if dolphie.performance_schema_enabled and dolphie.is_mysql_version_at_least("5.7"):
                find_replicas_query = MySQLQueries.ps_find_replicas
            else:
                find_replicas_query = MySQLQueries.pl_find_replicas

            dolphie.main_db_connection.execute(find_replicas_query)
            available_replicas = dolphie.main_db_connection.fetchall()
            # We update the replica ports used if the number of replicas have changed
            if len(available_replicas) != len(dolphie.replica_manager.available_replicas):
                dolphie.replica_manager.ports = {}

                dolphie.main_db_connection.execute(MySQLQueries.get_replicas)
                replica_data = dolphie.main_db_connection.fetchall()
                for row in replica_data:
                    dolphie.replica_manager.ports[row["Slave_UUID"]] = row["Port"]

                dolphie.replica_manager.available_replicas = available_replicas

            dolphie.main_db_connection.execute(MySQLQueries.ps_disk_io)
            dolphie.disk_io_metrics = dolphie.main_db_connection.fetchone()

            dolphie.fetch_replication_data()
            dolphie.massage_metrics_data()

            if dolphie.group_replication or dolphie.innodb_cluster:
                if dolphie.is_mysql_version_at_least("8.0.13"):
                    dolphie.main_db_connection.execute(MySQLQueries.group_replication_get_write_concurrency)
                    dolphie.group_replication_data = dolphie.main_db_connection.fetchone()

                dolphie.main_db_connection.execute(MySQLQueries.get_group_replication_members)
                dolphie.group_replication_members = dolphie.main_db_connection.fetchall()
                for member_role_data in dolphie.group_replication_members:
                    if (
                        member_role_data.get("MEMBER_ID") == dolphie.server_uuid
                        and member_role_data.get("MEMBER_ROLE") == "PRIMARY"
                    ):
                        dolphie.is_group_replication_primary = True
                        break

            if dolphie.panels.dashboard.visible:
                dolphie.main_db_connection.execute(MySQLQueries.binlog_status)
                dolphie.binlog_status = dolphie.main_db_connection.fetchone()

                # This can cause MySQL to crash: https://perconadev.atlassian.net/browse/PS-9066
                # if dolphie.global_variables.get("binlog_transaction_compression") == "ON":
                #     dolphie.main_db_connection.execute(MySQLQueries.get_binlog_transaction_compression_percentage)
                #     dolphie.binlog_transaction_compression_percentage = dolphie.main_db_connection.fetchone().get(
                #         "compression_percentage"
                #     )

            if dolphie.panels.processlist.visible:
                dolphie.processlist_threads = processlist_panel.fetch_data(tab)

            if dolphie.is_mysql_version_at_least("5.7"):
                # We don't check if panel is visible or not since we use this data for Locks graph
                dolphie.main_db_connection.execute(MySQLQueries.locks_query)
                dolphie.lock_transactions = dolphie.main_db_connection.fetchall()

                if dolphie.panels.ddl.visible:
                    dolphie.main_db_connection.execute(MySQLQueries.ddls)
                    dolphie.ddl = dolphie.main_db_connection.fetchall()

            dolphie.monitor_read_only_change()

            dolphie.metric_manager.refresh_data(
                worker_start_time=dolphie.worker_start_time,
                polling_latency=dolphie.polling_latency,
                global_variables=dolphie.global_variables,
                global_status=dolphie.global_status,
                innodb_metrics=dolphie.innodb_metrics,
                disk_io_metrics=dolphie.disk_io_metrics,
                lock_metrics=dolphie.lock_transactions,
                replication_status=dolphie.replication_status,
                replication_lag=dolphie.replica_lag,
            )
        except ManualException as e:
            # This will set up the worker state change function below to trigger the
            # host setup modal with the error
            tab.worker_cancel_error = e.output()

            tab.disconnect()

    def on_worker_state_changed(self, event: Worker.StateChanged):
        tab = self.tab_manager.get_tab(event.worker.name)
        if not tab:
            return

        dolphie = tab.dolphie

        if event.worker.group == "main":
            if event.state == WorkerState.SUCCESS:
                # Skip this if the conditions are right
                if (
                    len(self.screen_stack) > 1
                    or dolphie.pause_refresh
                    or not tab.dolphie.main_db_connection.is_connected()
                    or tab.id != self.tab.id
                ):
                    tab.worker_timer = self.set_timer(
                        tab.dolphie.refresh_interval, partial(self.run_worker_main, tab.id)
                    )

                    return

                self.refresh_screen(tab)

                tab.worker_timer = self.set_timer(tab.dolphie.refresh_interval, partial(self.run_worker_main, tab.id))
            elif event.state == WorkerState.CANCELLED:
                # Only show the modal if there's a worker cancel error
                if tab.worker_cancel_error:
                    if self.tab.id != tab.id:
                        self.notify(
                            f"Host [light_blue]{tab.dolphie.host}:{tab.dolphie.port}[/light_blue] has been"
                            " disconnected",
                            title="Error",
                            severity="error",
                            timeout=10,
                        )

                    self.tab_manager.switch_tab(tab.id)

                    tab.host_setup()
                    self.bell()
        elif event.worker.group == "replicas":
            if event.state == WorkerState.SUCCESS:
                # Skip this if the conditions are right
                if len(self.screen_stack) > 1 or dolphie.pause_refresh or tab.id != self.tab.id:
                    tab.replicas_worker_timer = self.set_timer(
                        tab.dolphie.refresh_interval, partial(self.run_worker_replicas, tab.id)
                    )

                    return

                if dolphie.panels.replication.visible and dolphie.replica_manager.available_replicas:
                    replication_panel.create_replica_panel(tab)

                tab.replicas_worker_timer = self.set_timer(2, partial(self.run_worker_replicas, tab.id))

    @work(thread=True, group="replicas")
    def run_worker_replicas(self, tab_id: int):
        tab = self.tab_manager.get_tab(tab_id)

        # Get our worker thread
        tab.replicas_worker = get_current_worker()
        tab.replicas_worker.name = tab_id

        dolphie = tab.dolphie

        if dolphie.panels.replication.visible:
            if dolphie.replica_manager.available_replicas:
                if not dolphie.replica_manager.replicas:
                    tab.replicas_container.display = True
                    tab.replicas_loading_indicator.display = True
                    tab.replicas_title.update(
                        f"[b]Loading [highlight]{len(dolphie.replica_manager.available_replicas)}[/highlight]"
                        " replicas...\n"
                    )

                replication_panel.fetch_replicas(tab)
            else:
                tab.replicas_container.display = False
        else:
            # If we're not displaying the replication panel, close all replica connections
            dolphie.replica_manager.remove_all()

    def refresh_screen(self, tab: Tab):
        dolphie = tab.dolphie

        try:
            loading_indicator = tab.loading_indicator
            if loading_indicator.display:
                # We only want to do this on startup
                # Set panels to be visible for the ones the user specifies
                for panel in dolphie.panels.all():
                    setattr(getattr(tab, f"panel_{panel}"), "display", False)

                for panel in dolphie.startup_panels:
                    setattr(getattr(tab, f"panel_{panel}"), "display", True)

                loading_indicator.display = False
                tab.main_container.display = True

                self.layout_graphs()

            if self.tab.id == tab.id:
                tab.update_topbar()

            if dolphie.panels.dashboard.visible:
                self.refresh_panel(dolphie.panels.dashboard.name)

                # Update the sparkline for queries per second
                sparkline = tab.sparkline
                sparkline_data = dolphie.metric_manager.metrics.dml.Queries.values
                if not sparkline.display:
                    sparkline_data = [0]
                    sparkline.display = True

                sparkline.data = sparkline_data
                sparkline.refresh()

            if dolphie.panels.processlist.visible:
                self.refresh_panel(dolphie.panels.processlist.name)

            if dolphie.panels.replication.visible:
                self.refresh_panel(dolphie.panels.replication.name)

            if dolphie.panels.locks.visible:
                self.refresh_panel(dolphie.panels.locks.name)

            if dolphie.panels.ddl.visible:
                self.refresh_panel(dolphie.panels.ddl.name)

            if dolphie.panels.graphs.visible:
                graph_panel = self.query_one(f"#tabbed_content_{tab.id}", TabbedContent)

                # Hide/show replication tab based on replication status
                if dolphie.replication_status:
                    graph_panel.show_tab(f"graph_tab_replication_lag_{tab.id}")
                else:
                    graph_panel.hide_tab(f"graph_tab_replication_lag_{tab.id}")

                # Refresh the graph(s) for the selected tab
                self.update_graphs(graph_panel.get_pane(graph_panel.active).name)

            # We take a snapshot of the processlist to be used for commands
            # since the data can change after a key is pressed
            dolphie.processlist_threads_snapshot = dolphie.processlist_threads.copy()

            # This denotes that we've gone through the first loop of the worker thread
            dolphie.first_loop = True
        except NoMatches:
            # This is thrown if a user toggles panels on and off and the display_* states aren't 1:1
            # with worker thread/state change due to asynchronous nature of the worker thread
            pass

    async def on_key(self, event: events.Key):
        if len(self.screen_stack) > 1:
            return

        await self.capture_key(event.key)

    async def on_mount(self):
        dolphie = self.dolphie

        self.tab_manager = TabManager(self)
        await self.tab_manager.create_tab("Main", dolphie)

        dolphie.load_host_cache_file()
        dolphie.check_for_update()

        self.run_worker_main(self.tab.id)
        self.run_worker_replicas(self.tab.id)

    def _handle_exception(self, error: Exception) -> None:
        self.bell()
        self.exit(message=Traceback(show_locals=True, width=None, locals_max_length=5))

    @on(TabbedContent.TabActivated, "#tabbed_content")
    def tab_changed(self, event: TabbedContent.TabActivated):
        self.tab_manager.switch_tab(event.pane.name)

        if self.tab.dolphie.main_db_connection:
            self.refresh_screen(self.tab)

    @on(TabbedContent.TabActivated, ".metrics_tabbed_content")
    def metric_tab_changed(self, event: TabbedContent.TabActivated):
        metric_instance_name = event.pane.name

        if metric_instance_name:
            self.update_graphs(metric_instance_name)

    def update_graphs(self, tab_metric_instance_name):
        if not self.tab.panel_graphs.display:
            return

        for metric_instance in self.tab.dolphie.metric_manager.metrics.__dict__.values():
            if tab_metric_instance_name == metric_instance.tab_name:
                for graph_name in metric_instance.graphs:
                    self.query_one(f"#{graph_name}_{self.tab.id}").render_graph(metric_instance)

        self.update_stats_label(tab_metric_instance_name)

    def update_stats_label(self, tab_metric_instance_name):
        stat_data = {}

        for metric_instance in self.tab.dolphie.metric_manager.metrics.__dict__.values():
            if hasattr(metric_instance, "tab_name") and metric_instance.tab_name == tab_metric_instance_name:
                number_format_func = MetricManager.get_number_format_function(metric_instance, color=True)
                for metric_data in metric_instance.__dict__.values():
                    if isinstance(metric_data, MetricManager.MetricData) and metric_data.values and metric_data.visible:
                        stat_data[metric_data.label] = number_format_func(metric_data.values[-1])

        formatted_stat_data = "  ".join(
            f"[b light_blue]{label}[/b light_blue] {value}" for label, value in stat_data.items()
        )
        self.query_one(f"#stats_{tab_metric_instance_name}_{self.tab.id}").update(formatted_stat_data)

    def toggle_panel(self, panel_name):
        panel = self.app.query_one(f"#panel_{panel_name}_{self.tab.id}")

        new_display = not panel.display
        panel.display = new_display

        setattr(getattr(self.tab.dolphie.panels, panel_name), "visible", new_display)

        if panel_name not in [self.tab.dolphie.panels.graphs.name]:
            self.app.refresh_panel(panel_name, toggled=True)

    def refresh_panel(self, panel_name, toggled=False):
        # If loading indicator is displaying, don't refresh
        if self.tab.loading_indicator.display:
            return

        if panel_name == self.tab.dolphie.panels.replication.name:
            # When replication panel status is changed, we need to refresh the dashboard panel as well since
            # it adds/removes it from there
            replication_panel.create_panel(self.tab)

            if toggled and self.tab.dolphie.replication_status:
                dashboard_panel.create_panel(self.tab)
        elif panel_name == self.tab.dolphie.panels.dashboard.name:
            dashboard_panel.create_panel(self.tab)
        elif panel_name == self.tab.dolphie.panels.processlist.name:
            processlist_panel.create_panel(self.tab)
        elif panel_name == self.tab.dolphie.panels.locks.name:
            locks_panel.create_panel(self.tab)
        elif panel_name == self.tab.dolphie.panels.ddl.name:
            ddl_panel.create_panel(self.tab)

        if toggled or not self.tab.dolphie.first_loop:  # Include this so we're not updating the sizes every loop
            # Update the sizes of the panels depending if replication container is visible or not
            if self.tab.dolphie.replication_status and not self.tab.dolphie.panels.replication.visible:
                self.tab.dashboard_host_information.styles.width = "25vw"
                self.tab.dashboard_binary_log.styles.width = "21vw"

                self.tab.dashboard_innodb.styles.width = "17vw"
                self.tab.dashboard_replication.styles.width = "25vw"
                self.tab.dashboard_statistics.styles.width = "12vw"
            else:
                self.tab.dashboard_host_information.styles.width = "32vw"
                self.tab.dashboard_binary_log.styles.width = "27vw"
                self.tab.dashboard_innodb.styles.width = "24vw"
                self.tab.dashboard_replication.styles.width = "0"
                self.tab.dashboard_statistics.styles.width = "17vw"

            self.tab.dashboard_host_information.styles.max_width = "45"
            self.tab.dashboard_binary_log.styles.max_width = "38"
            self.tab.dashboard_innodb.styles.max_width = "32"
            self.tab.dashboard_replication.styles.max_width = "50"
            self.tab.dashboard_statistics.styles.max_width = "22"

    def layout_graphs(self):
        if self.tab.dolphie.is_mysql_version_at_least("8.0.30"):
            self.query_one(f"#graph_redo_log_{self.tab.id}").styles.width = "55%"
            self.query_one(f"#graph_redo_log_bar_{self.tab.id}").styles.width = "12%"
            self.query_one(f"#graph_redo_log_active_count_{self.tab.id}").styles.width = "33%"
            self.tab.dolphie.metric_manager.metrics.redo_log_active_count.Active_redo_log_count.visible = True
            self.query_one(f"#graph_redo_log_active_count_{self.tab.id}").display = True
        else:
            self.query_one(f"#graph_redo_log_{self.tab.id}").styles.width = "88%"
            self.query_one(f"#graph_redo_log_bar_{self.tab.id}").styles.width = "12%"
            self.query_one(f"#graph_redo_log_active_count_{self.tab.id}").display = False

        self.query_one(f"#graph_adaptive_hash_index_{self.tab.id}").styles.width = "50%"
        self.query_one(f"#graph_adaptive_hash_index_hit_ratio_{self.tab.id}").styles.width = "50%"

    @on(Switch.Changed)
    def switch_changed(self, event: Switch.Changed):
        if len(self.screen_stack) > 1:
            return

        metric_instance_name = event.switch.name
        metric = event.switch.id

        metric_instance = getattr(self.tab.dolphie.metric_manager.metrics, metric_instance_name)
        metric_data: MetricManager.MetricData = getattr(metric_instance, metric)
        metric_data.visible = event.value

        self.update_graphs(metric_instance_name)

    async def capture_key(self, key):
        tab = self.tab_manager.get_tab(self.tab.id)

        exclude_keys = [
            "up",
            "down",
            "left",
            "right",
            "pageup",
            "pagedown",
            "home",
            "end",
            "tab",
            "enter",
            "grave_accent",
            "q",
            "question_mark",
            "plus",
            "minus",
            "ctrl+a",
            "ctrl+d",
        ]

        screen_data = None
        dolphie = tab.dolphie

        # Prevent commands from being run if the secondary connection is processing a query already
        if key not in exclude_keys:
            if dolphie.secondary_db_connection and dolphie.secondary_db_connection.running_query:
                self.notify("There's already a command running - please wait for it to finish")
                return

            if not tab.worker:
                self.notify("You must be connected to a host to use commands")
                return

        if key == "1":
            self.toggle_panel(dolphie.panels.dashboard.name)
        elif key == "2":
            self.toggle_panel(dolphie.panels.processlist.name)
            self.tab.processlist_datatable.clear()
        elif key == "3":
            self.toggle_panel(dolphie.panels.replication.name)

            tab.replicas_container.display = False
            if not dolphie.panels.replication.visible:
                for member in dolphie.app.query(f".replica_container_{dolphie.tab_id}"):
                    member.remove()
            else:
                if dolphie.replica_manager.available_replicas:
                    tab.replicas_container.display = True
                    tab.replicas_title.update(
                        f"[b]Loading [highlight]{len(dolphie.replica_manager.available_replicas)}[/highlight]"
                        " replicas...\n"
                    )
                    tab.replicas_loading_indicator.display = True

        elif key == "4":
            self.toggle_panel(dolphie.panels.graphs.name)
            self.app.update_graphs("dml")
        elif key == "5":
            if not dolphie.is_mysql_version_at_least("5.7"):
                self.notify("Locks panel requires MySQL 5.7+")
                return

            self.toggle_panel(dolphie.panels.locks.name)
            self.tab.locks_datatable.clear()
        elif key == "6":
            if not dolphie.is_mysql_version_at_least("5.7"):
                self.notify("DDL panel requires MySQL 5.7+")
                return

            query = "SELECT enabled FROM performance_schema.setup_instruments WHERE name LIKE 'stage/innodb/alter%';"
            dolphie.secondary_db_connection.execute(query)
            data = dolphie.secondary_db_connection.fetchall()
            for row in data:
                if row.get("enabled") == "NO":
                    self.notify("DDL panel requires Performance Schema to have 'stage/innodb/alter%' enabled")
                    return

            self.toggle_panel(dolphie.panels.ddl.name)
            self.tab.ddl_datatable.clear()
        elif key == "grave_accent":
            self.tab.host_setup()
        elif key == "space":
            if tab.worker.state != WorkerState.RUNNING:
                tab.worker_timer.stop()
                self.run_worker_main(tab.id)
        elif key == "plus":

            async def command_get_input(tab_name):
                await self.tab_manager.create_tab(tab_name, dolphie)
                self.tab.topbar.host = ""
                self.tab.host_setup()

            self.app.push_screen(
                CommandModal(command="new_tab", message="What would you like to name the new tab?"),
                command_get_input,
            )
        elif key == "minus":
            if tab.id == 1:
                self.notify("Main tab cannot be removed")
                return
            else:
                await self.tab_manager.remove_tab(tab.id)
        elif key == "ctrl+a" or key == "ctrl+d":
            all_tabs = self.tab_manager.get_all_tabs()

            if key == "ctrl+a":
                switch_to_tab = all_tabs[(all_tabs.index(tab.id) - 1) % len(all_tabs)]
            elif key == "ctrl+d":
                switch_to_tab = all_tabs[(all_tabs.index(tab.id) + 1) % len(all_tabs)]

            self.tab_manager.switch_tab(switch_to_tab)

        elif key == "a":
            if dolphie.show_additional_query_columns:
                dolphie.show_additional_query_columns = False
                self.notify("Processlist will now hide additional columns")
            else:
                dolphie.show_additional_query_columns = True
                self.notify("Processlist will now show additional columns")

        elif key == "c":
            dolphie.user_filter = ""
            dolphie.db_filter = ""
            dolphie.host_filter = ""
            dolphie.query_time_filter = ""
            dolphie.query_filter = ""

            self.notify("Cleared all filters", severity="success")

        elif key == "d":
            self.run_command_in_worker(key=key, dolphie=dolphie)

        elif key == "D":
            tab.disconnect()

        elif key == "e":
            if dolphie.is_mysql_version_at_least("8.0") and dolphie.performance_schema_enabled:
                self.app.push_screen(
                    EventLog(
                        dolphie.read_only_status,
                        dolphie.app_version,
                        dolphie.mysql_host,
                        dolphie.secondary_db_connection,
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
                    setattr(dolphie, attribute, int(filter_value) if attribute == "query_time_filter" else filter_value)
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
                    processlist_data=dolphie.processlist_threads_snapshot,
                    host_cache_data=dolphie.host_cache,
                ),
                command_get_input,
            )

        elif key == "i":
            if dolphie.show_idle_threads:
                dolphie.show_idle_threads = False
                dolphie.sort_by_time_descending = True

                self.notify("Processlist will now hide idle threads")
            else:
                dolphie.show_idle_threads = True
                dolphie.sort_by_time_descending = False

                self.notify("Processlist will now show idle threads")

        elif key == "k":

            def command_get_input(data):
                self.run_command_in_worker(key=key, dolphie=dolphie, additional_data=data)

            self.app.push_screen(
                CommandModal(
                    command="thread_kill_by_id",
                    message="Specify a Thread ID to kill",
                    processlist_data=dolphie.processlist_threads_snapshot,
                ),
                command_get_input,
            )

        elif key == "K":

            def command_get_input(data):
                self.run_command_in_worker(key=key, dolphie=dolphie, additional_data=data)

            self.app.push_screen(
                CommandModal(
                    command="thread_kill_by_parameter",
                    message="Kill threads based around parameters",
                    processlist_data=dolphie.processlist_threads_snapshot,
                ),
                command_get_input,
            )

        elif key == "l":
            self.run_command_in_worker(key=key, dolphie=dolphie)

        elif key == "o":
            self.run_command_in_worker(key=key, dolphie=dolphie)

        elif key == "m":
            if not dolphie.is_mysql_version_at_least("5.7") or not dolphie.performance_schema_enabled:
                self.notify("Memory usage command requires MySQL 5.7+ with Performance Schema enabled")
            else:
                self.run_command_in_worker(key=key, dolphie=dolphie)

        elif key == "p":
            if not dolphie.pause_refresh:
                dolphie.pause_refresh = True
                self.notify(f"Refresh is paused! Press [b highlight]{key}[/b highlight] again to resume")
            else:
                dolphie.pause_refresh = False
                self.notify("Refreshing has resumed", severity="success")

        if key == "P":
            if dolphie.use_performance_schema:
                dolphie.use_performance_schema = False
                self.notify("Switched to using [b highlight]Processlist")
            else:
                if dolphie.performance_schema_enabled:
                    dolphie.use_performance_schema = True
                    self.notify("Switched to using [b highlight]Performance Schema")
                else:
                    self.notify("You can't switch to Performance Schema because it isn't enabled")

        elif key == "q":
            self.app.exit()

        elif key == "r":

            def command_get_input(refresh_interval):
                dolphie.refresh_interval = refresh_interval

                self.notify(
                    f"Refresh interval set to [b highlight]{refresh_interval}[/b highlight] second(s)",
                    severity="success",
                )

            self.app.push_screen(
                CommandModal(command="refresh_interval", message="Specify refresh interval (in seconds)"),
                command_get_input,
            )

        elif key == "R":
            dolphie.metric_manager.reset()

            active_graph = self.app.query_one(f"#tabbed_content_{tab.id}", TabbedContent)
            self.update_graphs(active_graph.get_pane(active_graph.active).name)
            self.notify("Metrics have been reset", severity="success")

        elif key == "s":
            if dolphie.sort_by_time_descending:
                dolphie.sort_by_time_descending = False
                self.notify("Processlist will now sort threads by time in ascending order")
            else:
                dolphie.sort_by_time_descending = True
                self.notify("Processlist will now sort threads by time in descending order")

        elif key == "t":

            def command_get_input(data):
                self.run_command_in_worker(key=key, dolphie=dolphie, additional_data=data)

            self.app.push_screen(
                CommandModal(
                    command="show_thread",
                    message="Specify a Thread ID to display its details",
                    processlist_data=dolphie.processlist_threads_snapshot,
                ),
                command_get_input,
            )

        elif key == "T":
            if dolphie.show_trxs_only:
                dolphie.show_trxs_only = False
                dolphie.show_idle_threads = False
                self.notify("Processlist will now no longer only show threads that have an active transaction")
            else:
                dolphie.show_trxs_only = True
                dolphie.show_idle_threads = True
                self.notify("Processlist will now only show threads that have an active transaction")

        elif key == "u":
            if not dolphie.performance_schema_enabled:
                self.notify("User statistics command requires Performance Schema to be enabled")
            else:
                self.run_command_in_worker(key=key, dolphie=dolphie)

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

                for variable, value in dolphie.global_variables.items():
                    if input_variable == "all":
                        display_global_variables[variable] = dolphie.global_variables[variable]
                    else:
                        if input_variable:
                            if input_variable in variable:
                                display_global_variables[variable] = dolphie.global_variables[variable]

                max_num_tables = 1 if len(display_global_variables) <= 50 else 2

                # Create the number of tables we want
                while table_counter <= max_num_tables:
                    tables[table_counter] = Table(box=box.HORIZONTALS, show_header=False, style="table_border")
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

                    self.app.push_screen(
                        CommandScreen(dolphie.read_only_status, dolphie.app_version, dolphie.mysql_host, screen_data)
                    )
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
            if dolphie.host_cache:
                table = Table(
                    box=box.SIMPLE_HEAVY,
                    show_edge=False,
                    style="table_border",
                )
                table.add_column("Host/IP")
                table.add_column("Hostname (if resolved)")

                for ip, addr in dolphie.host_cache.items():
                    if ip:
                        table.add_row(ip, addr)

                screen_data = Group(
                    Align.center(
                        "[b light_blue]Host Cache[/b light_blue] ([b highlight]%s[/b highlight])\n"
                        % len(dolphie.host_cache)
                    ),
                    table,
                )
            else:
                screen_data = Group(
                    Align.center("[b light_blue]Host Cache[/b light_blue]\n"), "There are currently no hosts resolved"
                )

        elif key == "question_mark":
            keys = {
                "1": "Show/hide Dashboard",
                "2": "Show/hide Processlist",
                "3": "Show/hide Replication/Replicas",
                "4": "Show/hide Graph Metrics",
                "5": "Show/hide Locks",
                "6": "Show/hide DDLs",
                "`": "Open Host Setup",
                "+": "Create a new tab",
                "-": "Remove the current tab",
                "a": "Toggle additional processlist columns",
                "c": "Clear all filters set",
                "d": "Display all databases",
                "D": "Disconnect from the tab's host",
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
                "space": "Force a manual refresh of all panels",
                "ctrl+a": "Switch to the previous tab",
                "ctrl+d": "Switch to the next tab",
            }

            table_keys = Table(
                box=box.SIMPLE_HEAVY,
                show_edge=False,
                style="table_border",
                title="Commands",
                title_style="bold #bbc8e8",
                header_style="bold",
            )
            table_keys.add_column("Key", justify="center", style="b highlight")
            table_keys.add_column("Description")

            for key, description in keys.items():
                table_keys.add_row(key, description)

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
                box=box.SIMPLE_HEAVY,
                show_edge=False,
                style="table_border",
                title="Terminology",
                title_style="bold #bbc8e8",
                header_style="bold",
            )
            table_terminology.add_column("Datapoint", style="highlight")
            table_terminology.add_column("Description")
            for datapoint, description in sorted(datapoints.items()):
                table_terminology.add_row(datapoint, description)

            screen_data = Group(
                Align.center(table_keys),
                "",
                Align.center(table_terminology),
                "",
                Align.center(
                    "[light_blue][b]Note[/b]: Textual puts your terminal in application mode which disables selecting"
                    " text.\nTo see how to select text on your terminal, visit: https://tinyurl.com/dolphie-copy-text"
                ),
            )

        if screen_data:
            self.app.push_screen(
                CommandScreen(dolphie.read_only_status, dolphie.app_version, dolphie.mysql_host, screen_data)
            )

    @work(thread=True)
    def run_command_in_worker(self, key: str, dolphie: Dolphie, additional_data=None):
        tab = self.tab_manager.get_tab(self.tab.id)

        # These are the screens to display we use for the commands
        def show_command_screen():
            self.app.push_screen(
                CommandScreen(dolphie.read_only_status, dolphie.app_version, dolphie.mysql_host, screen_data)
            )

        def show_thread_screen():
            self.app.push_screen(
                ThreadScreen(
                    read_only=dolphie.read_only_status,
                    app_version=dolphie.app_version,
                    host=dolphie.mysql_host,
                    thread_table=thread_table,
                    user_thread_attributes_table=user_thread_attributes_table,
                    query=formatted_query,
                    explain_data=explain_data,
                    explain_failure=explain_failure,
                    transaction_history_table=transaction_history_table,
                )
            )

        tab.spinner.show()

        try:
            if key == "d":
                tables = {}
                all_tables = []

                db_count = dolphie.secondary_db_connection.execute(MySQLQueries.databases)
                databases = dolphie.secondary_db_connection.fetchall()

                # Determine how many tables to provide data
                max_num_tables = 1 if db_count <= 20 else 3

                # Calculate how many databases per table
                row_per_count = db_count // max_num_tables

                # Create dictionary of tables
                for table_counter in range(1, max_num_tables + 1):
                    table_box = box.HORIZONTALS
                    if max_num_tables == 1:
                        table_box = None

                    tables[table_counter] = Table(box=table_box, show_header=False, style="table_border")
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
                    Align.center("[b light_blue]Databases[/b light_blue] ([b highlight]%s[/b highlight])\n" % db_count),
                    Align.center(table_grid),
                )

                self.call_from_thread(show_command_screen)

            if key == "k":
                thread_id = additional_data
                try:
                    if dolphie.aws_rds:
                        dolphie.secondary_db_connection.execute("CALL mysql.rds_kill(%s)" % thread_id)
                    else:
                        dolphie.secondary_db_connection.execute("KILL %s" % thread_id)

                    self.notify("Killed Thread ID [b highlight]%s[/b highlight]" % thread_id, severity="success")
                except ManualException as e:
                    self.notify(e.reason, title="Error killing Thread ID", severity="error")

            if key == "K":

                def execute_kill(thread_id):
                    query = "CALL mysql.rds_kill(%s)" if dolphie.aws_rds else "KILL %s"
                    dolphie.secondary_db_connection.execute(query % thread_id)

                kill_type, kill_value, include_sleeping_queries, lower_limit, upper_limit = additional_data
                db_field = {"username": "user", "host": "host", "time_range": "time"}.get(kill_type)

                commands_to_kill = ["Query", "Execute"]

                if include_sleeping_queries:
                    commands_to_kill.append("Sleep")

                tab.spinner.show()
                threads_killed = 0
                for thread_id, thread in dolphie.processlist_threads_snapshot.items():
                    thread: ProcesslistThread
                    try:
                        if thread.command in commands_to_kill:
                            if kill_type == "time_range":
                                if lower_limit <= thread.time <= upper_limit:
                                    execute_kill(thread_id)
                                    threads_killed += 1
                            elif getattr(thread, db_field) == kill_value:
                                execute_kill(thread_id)
                                threads_killed += 1
                    except ManualException as e:
                        self.notify(e.reason, title=f"Error Killing Thread ID {thread_id}", severity="error")

                if threads_killed:
                    self.notify(f"Killed [highlight]{threads_killed}[/highlight] threads")
                else:
                    self.notify("No threads were killed")

            elif key == "l":
                deadlock = ""
                output = re.search(
                    r"------------------------\nLATEST\sDETECTED\sDEADLOCK\n------------------------"
                    "\n(.*?)------------\nTRANSACTIONS",
                    dolphie.secondary_db_connection.fetch_value_from_field(MySQLQueries.innodb_status, "Status"),
                    flags=re.S,
                )
                if output:
                    deadlock = output.group(1)

                    deadlock = deadlock.replace("***", "[yellow]*****[/yellow]")
                    screen_data = deadlock
                else:
                    screen_data = Align.center("No deadlock detected")

                self.call_from_thread(show_command_screen)

            elif key == "o":
                screen_data = dolphie.secondary_db_connection.fetch_value_from_field(
                    MySQLQueries.innodb_status, "Status"
                )

                self.call_from_thread(show_command_screen)

            elif key == "m":
                table_grid = Table.grid()
                table1 = Table(box=box.SIMPLE_HEAVY, style="table_border")

                header_style = Style(bold=True)
                table1.add_column("User", header_style=header_style)
                table1.add_column("Current", header_style=header_style)
                table1.add_column("Total", header_style=header_style)

                dolphie.secondary_db_connection.execute(MySQLQueries.memory_by_user)
                data = dolphie.secondary_db_connection.fetchall()
                for row in data:
                    table1.add_row(
                        row["user"],
                        format_sys_table_memory(row["current_allocated"]),
                        format_sys_table_memory(row["total_allocated"]),
                    )

                table2 = Table(box=box.SIMPLE_HEAVY, style="table_border")
                table2.add_column("Code Area", header_style=header_style)
                table2.add_column("Current", header_style=header_style)

                dolphie.secondary_db_connection.execute(MySQLQueries.memory_by_code_area)
                data = dolphie.secondary_db_connection.fetchall()
                for row in data:
                    table2.add_row(row["code_area"], format_sys_table_memory(row["current_allocated"]))

                table3 = Table(box=box.SIMPLE_HEAVY, style="table_border")
                table3.add_column("Host", header_style=header_style)
                table3.add_column("Current", header_style=header_style)
                table3.add_column("Total", header_style=header_style)

                dolphie.secondary_db_connection.execute(MySQLQueries.memory_by_host)
                data = dolphie.secondary_db_connection.fetchall()
                for row in data:
                    table3.add_row(
                        dolphie.get_hostname(row["host"]),
                        format_sys_table_memory(row["current_allocated"]),
                        format_sys_table_memory(row["total_allocated"]),
                    )

                table_grid.add_row("", Align.center("[b light_blue]Memory Allocation"), "")
                table_grid.add_row(table1, table3, table2)

                screen_data = Align.center(table_grid)

                self.call_from_thread(show_command_screen)
            elif key == "t":
                formatted_query = ""
                explain_failure = ""
                explain_data = ""

                thread_table = Table(box=None, show_header=False)
                thread_table.add_column("")
                thread_table.add_column("", overflow="fold")

                thread_id = additional_data
                thread_data: ProcesslistThread = dolphie.processlist_threads_snapshot.get(thread_id)
                thread_table.add_row("[label]Thread ID", thread_id)
                thread_table.add_row("[label]User", thread_data.user)
                thread_table.add_row("[label]Host", thread_data.host)
                thread_table.add_row("[label]Database", thread_data.db)
                thread_table.add_row("[label]Command", thread_data.command)
                thread_table.add_row("[label]State", thread_data.state)
                thread_table.add_row("[label]Time", str(timedelta(seconds=thread_data.time)).zfill(8))
                thread_table.add_row("[label]Rows Locked", thread_data.trx_rows_locked)
                thread_table.add_row("[label]Rows Modified", thread_data.trx_rows_modified)

                if dolphie.global_variables.get("innodb_thread_concurrency"):
                    thread_table.add_row("[label]Tickets", thread_data.trx_concurrency_tickets)

                thread_table.add_row("", "")
                thread_table.add_row("[label]TRX Time", thread_data.trx_time)
                thread_table.add_row("[label]TRX State", thread_data.trx_state)
                thread_table.add_row("[label]TRX Operation", thread_data.trx_operation_state)

                query = sqlformat(thread_data.query, reindent_aligned=True)
                query_db = thread_data.db
                if query:
                    formatted_query = Syntax(
                        query,
                        "sql",
                        line_numbers=False,
                        word_wrap=True,
                        theme="monokai",
                        background_color="#101626",
                    )

                    if query_db:
                        try:
                            dolphie.secondary_db_connection.execute("USE %s" % query_db)
                            dolphie.secondary_db_connection.execute("EXPLAIN %s" % query)

                            explain_data = dolphie.secondary_db_connection.fetchall()
                        except ManualException as e:
                            explain_failure = "[b indian_red]EXPLAIN ERROR:[/b indian_red] [indian_red]%s" % e.reason

                user_thread_attributes_table = None
                if dolphie.performance_schema_enabled:
                    user_thread_attributes_table = Table(box=None, show_header=False, expand=True)

                    dolphie.secondary_db_connection.execute(
                        MySQLQueries.user_thread_attributes.replace("$1", thread_id)
                    )

                    user_thread_attributes = dolphie.secondary_db_connection.fetchall()
                    if user_thread_attributes:
                        user_thread_attributes_table.add_column("")
                        user_thread_attributes_table.add_column("", overflow="fold")

                        for attribute in user_thread_attributes:
                            user_thread_attributes_table.add_row(
                                f"[label]{attribute['ATTR_NAME']}", attribute["ATTR_VALUE"]
                            )
                    else:
                        user_thread_attributes_table.add_column(justify="center")
                        user_thread_attributes_table.add_row("[b][label]None found")

                # Transaction history
                transaction_history_table = None
                if (
                    dolphie.is_mysql_version_at_least("5.7")
                    and dolphie.performance_schema_enabled
                    and thread_data.mysql_thread_id
                ):
                    query = MySQLQueries.thread_transaction_history.replace("$1", str(thread_data.mysql_thread_id))
                    dolphie.secondary_db_connection.execute(query)
                    transaction_history = dolphie.secondary_db_connection.fetchall()

                    if transaction_history:
                        transaction_history_table = Table(box=None)
                        transaction_history_table.add_column("Start Time")
                        transaction_history_table.add_column("Query", overflow="fold")

                        for query in transaction_history:
                            trx_history_formatted_query = ""
                            if query["sql_text"]:
                                trx_history_formatted_query = Syntax(
                                    re.sub(r"\s+", " ", query["sql_text"]),
                                    "sql",
                                    line_numbers=False,
                                    word_wrap=True,
                                    theme="monokai",
                                    background_color="#101626",
                                )

                            transaction_history_table.add_row(
                                query["start_time"].strftime("%Y-%m-%d %H:%M:%S"), trx_history_formatted_query
                            )

                self.call_from_thread(show_thread_screen)

            elif key == "u":
                if dolphie.is_mysql_version_at_least("5.7"):
                    dolphie.secondary_db_connection.execute(MySQLQueries.ps_user_statisitics)
                else:
                    dolphie.secondary_db_connection.execute(MySQLQueries.ps_user_statisitics_56)

                users = dolphie.secondary_db_connection.fetchall()

                columns = {
                    "User": {"field": "user", "format_number": False},
                    "Active": {"field": "current_connections", "format_number": True},
                    "Total": {"field": "total_connections", "format_number": True},
                    "Rows Read": {"field": "rows_examined", "format_number": True},
                    "Rows Sent": {"field": "rows_sent", "format_number": True},
                    "Rows Updated": {"field": "rows_affected", "format_number": True},
                    "Tmp Tables": {"field": "created_tmp_tables", "format_number": True},
                    "Tmp Disk Tables": {"field": "created_tmp_disk_tables", "format_number": True},
                    "Plugin": {"field": "plugin", "format_number": False},
                    "Password Expire": {"field": "password_expires_in", "format_number": False},
                }

                table = Table(
                    header_style="b",
                    box=box.SIMPLE_HEAVY,
                    show_edge=False,
                    style="table_border",
                )
                for column, data in columns.items():
                    table.add_column(column, no_wrap=True)

                for user in users:
                    row_values = []

                    for column, data in columns.items():
                        value = user.get(data["field"], "N/A")

                        if data["format_number"]:
                            row_values.append(format_number(value) if value else "0")
                        else:
                            row_values.append(value or "")

                    table.add_row(*row_values)

                screen_data = Group(
                    Align.center(f"[b light_blue]Users Connected ([highlight]{len(users)}[/highlight])\n"),
                    Align.center(table),
                )

                self.call_from_thread(show_command_screen)
        except ManualException as e:
            self.notify(e.reason, title=f"Error running command '{key}'", severity="error", timeout=10)

        tab.spinner.hide()

    def compose(self) -> ComposeResult:
        yield TopBar(host="", app_version=self.dolphie.app_version, help="press [b highlight]?[/b highlight] for help")
        yield TabbedContent(id="tabbed_content")


def main():
    # Set environment variables so Textual can use all the pretty colors
    os.environ["TERM"] = "xterm-256color"
    os.environ["COLORTERM"] = "truecolor"

    dolphie = Dolphie()
    parse_args(dolphie)

    app = DolphieApp(dolphie)
    app.run()


if __name__ == "__main__":
    main()
