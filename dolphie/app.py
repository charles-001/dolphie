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
from datetime import datetime
from urllib.parse import urlparse

import dolphie.Modules.MetricManager as MetricManager
import myloginpath
from dolphie import Dolphie
from dolphie.Modules.ManualException import ManualException
from dolphie.Modules.Queries import MySQLQueries
from dolphie.Panels import dashboard_panel, processlist_panel, replication_panel
from dolphie.Widgets.topbar import TopBar
from rich.console import Console
from rich.prompt import Prompt
from rich.traceback import Traceback
from textual import events, on, work
from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal, VerticalScroll
from textual.css.query import NoMatches
from textual.widgets import (
    DataTable,
    Label,
    LoadingIndicator,
    Sparkline,
    Static,
    Switch,
    TabbedContent,
    TabPane,
)
from textual.worker import Worker, WorkerState


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
        "--quick-switch-hosts-file",
        dest="quick_switch_hosts_file",
        type=str,
        help=(
            "Specify where the file is that stores the hosts you connect to for quick switching [default:"
            " ~/dolphie_quick_switch_hosts]"
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
        "--hide-dashboard",
        dest="hide_dashboard",
        action="store_true",
        default=False,
        help=(
            "Start without showing dashboard. This is good to use if you want to reclaim terminal space and "
            "not execute the additional queries for it"
        ),
    )
    parser.add_argument(
        "--show-trxs-only",
        dest="show_trxs_only",
        action="store_true",
        default=False,
        help="Start with only showing queries that have a running transaction",
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
        help="Start with using Processlist instead of Performance Schema for listing queries",
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
        dolphie.password = Prompt.ask("[b #bbc8e8]Password", password=True)

    if not dolphie.host:
        dolphie.host = "localhost"

    if parameter_options["refresh_interval"]:
        dolphie.refresh_interval = parameter_options["refresh_interval"]

    if parameter_options["heartbeat_table"]:
        pattern_match = re.search(r"^(\w+\.\w+)$", parameter_options["heartbeat_table"])
        if pattern_match:
            dolphie.heartbeat_table = parameter_options["heartbeat_table"]
            MySQLQueries.heartbeat_replica_lag = MySQLQueries.heartbeat_replica_lag.replace(
                "$placeholder", dolphie.heartbeat_table
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

    dolphie.quick_switch_hosts_file = f"{home_dir}/dolphie_quick_switch_hosts"
    if parameter_options["quick_switch_hosts_file"]:
        dolphie.quick_switch_hosts_file = parameter_options["quick_switch_hosts_file"]

    dolphie.show_trxs_only = parameter_options["show_trxs_only"]
    dolphie.show_additional_query_columns = parameter_options["show_additional_query_columns"]
    dolphie.use_processlist = parameter_options["use_processlist"]
    dolphie.hide_dashboard = parameter_options["hide_dashboard"]

    if os.path.exists(dolphie.quick_switch_hosts_file):
        with open(dolphie.quick_switch_hosts_file, "r") as file:
            dolphie.quick_switch_hosts = [line.strip() for line in file]


class DolphieApp(App):
    TITLE = "Dolphie"
    CSS_PATH = "Dolphie.css"

    def __init__(self, dolphie: Dolphie):
        super().__init__()

        self.dolphie = dolphie
        dolphie.app = self

        self.console.set_window_title(self.TITLE)

    @work(exclusive=True, thread=True)
    def worker_fetch_data(self):
        if len(self.screen_stack) > 1 or self.dolphie.pause_refresh:
            return

        try:
            dolphie = self.dolphie

            if not dolphie.main_db_connection or not dolphie.main_db_connection.connection.open:
                dolphie.db_connect()

            dolphie.worker_start_time = datetime.now()
            dolphie.worker_job_time = (dolphie.worker_start_time - dolphie.worker_previous_start_time).total_seconds()
            dolphie.worker_previous_start_time = dolphie.worker_start_time

            dolphie.global_variables = dolphie.main_db_connection.fetch_data("variables")
            dolphie.global_status = dolphie.main_db_connection.fetch_data("status")
            dolphie.innodb_metrics = dolphie.main_db_connection.fetch_data("innodb_metrics")
            dolphie.fetch_replication_data(connection=dolphie.main_db_connection)
            dolphie.massage_metrics_data()

            if dolphie.display_dashboard_panel:
                dolphie.binlog_status = dolphie.main_db_connection.fetch_data("binlog_status")

            if dolphie.display_replication_panel:
                dolphie.replica_data = dolphie.main_db_connection.fetch_data(
                    "find_replicas", dolphie.use_performance_schema
                )

                dolphie.replica_tables = replication_panel.fetch_replica_table_data(dolphie)

            if dolphie.display_processlist_panel:
                dolphie.processlist_threads = processlist_panel.fetch_data(dolphie)

            # If we're not displaying the replication panel, close all replica connections
            if not dolphie.display_replication_panel and dolphie.replica_connections:
                for connection in dolphie.replica_connections.values():
                    connection["connection"].close()

                dolphie.replica_connections = {}

            dolphie.worker_job_count += 1
        except ManualException as e:
            self.exit(message=e.output())

    def on_worker_state_changed(self, event: Worker.StateChanged):
        if event.state == WorkerState.SUCCESS:
            dolphie = self.dolphie

            if (
                len(self.screen_stack) > 1
                or dolphie.pause_refresh
                or not self.dolphie.main_db_connection.connection.open
            ):
                self.set_timer(0.5, self.worker_fetch_data)
                return

            dolphie.metric_manager.refresh_data(
                worker_start_time=dolphie.worker_start_time,
                worker_job_time=dolphie.worker_job_time,
                global_variables=dolphie.global_variables,
                global_status=dolphie.global_status,
                innodb_metrics=dolphie.innodb_metrics,
                replication_status=dolphie.replication_status,
                replication_lag=dolphie.replica_lag,
            )

            try:
                loading_indicator = self.app.query_one("LoadingIndicator")
                if loading_indicator.display:
                    loading_indicator.display = False
                    self.app.query_one("#main_container").display = True
                    self.layout_graphs()

                if dolphie.display_dashboard_panel:
                    self.query_one("#panel_dashboard_data", Static).update(dashboard_panel.create_panel(dolphie))

                if dolphie.display_processlist_panel:
                    processlist_panel.create_panel(dolphie)

                if dolphie.display_replication_panel:
                    self.query_one("#panel_replication_data", Static).update(replication_panel.create_panel(dolphie))

                if dolphie.display_graphs_panel:
                    # Add/remove replication tab based on replication status
                    replication_tab = self.app.query_one("#tabbed_content", TabbedContent)
                    if dolphie.replication_status:
                        if not self.app.query("#tab_replication_lag"):
                            replication_tab.add_pane(
                                pane=TabPane(
                                    "Replication",
                                    Label(id="stats_replication_lag", classes="stats_data"),
                                    MetricManager.Graph(id="graph_replication_lag", classes="panel_data"),
                                    id="tab_replication_lag",
                                )
                            )
                    else:
                        if self.app.query("#tab_replication_lag"):
                            replication_tab.remove_pane("tab_replication_lag")

                    # Refresh the graph(s) for the selected tab
                    active_graph = self.query_one("#tabbed_content", TabbedContent).active
                    metric_instance_name = active_graph.split("tab_")[1]
                    self.update_graphs(metric_instance_name)

            except NoMatches:
                # This is thrown if a user toggles panels on and off and the display_* states aren't 1:1
                # with worker thread/state change due to asynchronous nature of the worker thread
                pass

            dolphie.metric_manager.update_metrics_with_last_value()

            # Update the sparkline for queries per second
            sparkline = self.app.query_one("#panel_dashboard_queries_qps")
            sparkline_data = dolphie.metric_manager.metrics.dml.Queries.values
            if not sparkline.display:
                sparkline_data = [0]
                sparkline.display = True

            sparkline.data = sparkline_data
            sparkline.refresh()

            # We take a snapshot of the processlist to be used for commands
            # since the data can change after a key is pressed
            dolphie.processlist_threads_snapshot = dolphie.processlist_threads.copy()

            self.set_timer(self.dolphie.refresh_interval, self.worker_fetch_data)

    def on_key(self, event: events.Key):
        if len(self.screen_stack) > 1:
            return

        self.dolphie.capture_key(event.key)

    def on_mount(self):
        dolphie = self.dolphie

        dolphie.load_host_cache_file()

        # Set these components by default to not show
        components_to_disable = [
            ".panel_container",
            "#panel_processlist",
            "Sparkline",
            "#footer",
        ]
        exempt_components = {"panel_dashboard", "panel_processlist"}

        for component in components_to_disable:
            display_off_components = self.query(component)
            for display_off_component in display_off_components:
                if display_off_component.id not in exempt_components:
                    display_off_component.display = False

        dolphie.display_processlist_panel = True
        if dolphie.hide_dashboard:
            self.query_one("#panel_dashboard", Container).display = False
            dolphie.display_dashboard_panel = False
        else:
            dolphie.display_dashboard_panel = True

        # Set default switches to be toggled on
        switches = self.query(".switch_container Switch")
        switches_to_toggle = [switch for switch in switches if switch.id not in ["Queries", "Threads_connected"]]
        for switch in switches_to_toggle:
            switch.toggle()

        dolphie.check_for_update()

        self.worker_fetch_data()

    def _handle_exception(self, error: Exception) -> None:
        self.bell()

        # We have a ManualException class that we use to output errors in a nice format
        if error.__class__.__name__ == "ManualException":
            message = error.output()
        else:
            message = Traceback(show_locals=True, width=None, locals_max_length=5)

        self.exit(message=message)

    @on(TabbedContent.TabActivated)
    def tab_changed(self, event: TabbedContent.TabActivated):
        if len(self.screen_stack) > 1:
            return

        metric_instance_name = event.tab.id.split("tab_")[1]
        self.update_graphs(metric_instance_name)

    @on(Switch.Changed)
    def switch_changed(self, event: Switch.Changed):
        if len(self.screen_stack) > 1:
            return

        metric_instance_name = event.switch.name
        metric = event.switch.id

        metric_instance = getattr(self.dolphie.metric_manager.metrics, metric_instance_name)
        metric_data: MetricManager.MetricData = getattr(metric_instance, metric)
        metric_data.visible = event.value

        self.update_graphs(metric_instance_name)

    def generate_switches(self, metric_instance_name):
        metric_instance = getattr(self.dolphie.metric_manager.metrics, metric_instance_name)

        for metric, metric_data in metric_instance.__dict__.items():
            if isinstance(metric_data, MetricManager.MetricData) and metric_data.graphable:
                yield Label(metric_data.label)
                yield Switch(animate=False, id=metric, name=metric_instance_name)

    def update_graphs(self, tab_metric_instance_name):
        for metric_instance in self.dolphie.metric_manager.metrics.__dict__.values():
            if tab_metric_instance_name == metric_instance.tab_name:
                for graph_name in metric_instance.graphs:
                    self.query_one(f"#{graph_name}").render_graph(metric_instance)

        self.update_stats_label(tab_metric_instance_name)

    def update_stats_label(self, tab_metric_instance_name):
        stat_data = {}

        for metric_instance in self.dolphie.metric_manager.metrics.__dict__.values():
            if hasattr(metric_instance, "tab_name") and metric_instance.tab_name == tab_metric_instance_name:
                number_format_func = MetricManager.get_number_format_function(metric_instance, color=True)
                for metric_data in metric_instance.__dict__.values():
                    if isinstance(metric_data, MetricManager.MetricData) and metric_data.values and metric_data.visible:
                        stat_data[metric_data.label] = number_format_func(metric_data.values[-1])

        formatted_stat_data = "  ".join(f"[b #bbc8e8]{label}[/b #bbc8e8] {value}" for label, value in stat_data.items())
        self.query_one(f"#stats_{tab_metric_instance_name}").update(formatted_stat_data)

    def layout_graphs(self):
        if self.dolphie.is_mysql_version_at_least("8.0.30"):
            self.query_one("#graph_redo_log").styles.width = "55%"
            self.query_one("#graph_redo_log_bar").styles.width = "12%"
            self.query_one("#graph_redo_log_active_count").styles.width = "33%"
            self.dolphie.metric_manager.metrics.redo_log_active_count.Active_redo_log_count.visible = True
            self.query_one("#graph_redo_log_active_count").display = True
        else:
            self.query_one("#graph_redo_log").styles.width = "88%"
            self.query_one("#graph_redo_log_bar").styles.width = "12%"
            self.query_one("#graph_redo_log_active_count").display = False

        self.query_one("#graph_adaptive_hash_index").styles.width = "50%"
        self.query_one("#graph_adaptive_hash_index_hit_ratio").styles.width = "50%"

    def compose(self) -> ComposeResult:
        yield TopBar(host="Connecting to MySQL", app_version=self.dolphie.app_version, help="press [b]?[/b] for help")

        yield LoadingIndicator()
        with VerticalScroll(id="main_container"):
            with Container(id="panel_dashboard", classes="panel_container"):
                yield Static(id="panel_dashboard_data", classes="panel_data")
                yield Sparkline([], id="panel_dashboard_queries_qps")

            with Container(id="panel_graphs", classes="panel_container"):
                with TabbedContent(initial="tab_dml", id="tabbed_content"):
                    with TabPane("DML", id="tab_dml"):
                        yield Label(id="stats_dml", classes="stats_data")
                        yield MetricManager.Graph(id="graph_dml", classes="panel_data")

                        with Horizontal(classes="switch_container"):
                            yield from self.generate_switches("dml")

                    with TabPane("Table Cache", id="tab_table_cache"):
                        yield Label(id="stats_table_cache", classes="stats_data")
                        yield MetricManager.Graph(id="graph_table_cache", classes="panel_data")

                        with Horizontal(classes="switch_container"):
                            yield from self.generate_switches("table_cache")

                    with TabPane("Threads", id="tab_threads"):
                        yield Label(id="stats_threads", classes="stats_data")
                        yield MetricManager.Graph(id="graph_threads", classes="panel_data")

                        with Horizontal(classes="switch_container"):
                            yield from self.generate_switches("threads")

                    with TabPane("Buffer Pool Requests", id="tab_buffer_pool_requests"):
                        yield Label(id="stats_buffer_pool_requests", classes="stats_data")
                        yield MetricManager.Graph(id="graph_buffer_pool_requests", classes="panel_data")

                        with Horizontal(classes="switch_container"):
                            yield from self.generate_switches("buffer_pool_requests")
                    with TabPane("Checkpoint", id="tab_checkpoint"):
                        yield Label(id="stats_checkpoint", classes="stats_data")
                        yield MetricManager.Graph(id="graph_checkpoint", classes="panel_data")

                    with TabPane("Redo Log", id="tab_redo_log"):
                        yield Label(id="stats_redo_log", classes="stats_data")
                        with Horizontal():
                            yield MetricManager.Graph(id="graph_redo_log", classes="panel_data")
                            yield MetricManager.Graph(id="graph_redo_log_active_count", classes="panel_data")
                            yield MetricManager.Graph(bar=True, id="graph_redo_log_bar", classes="panel_data")

                    with TabPane("Adaptive Hash Index", id="tab_adaptive_hash_index"):
                        yield Label(id="stats_adaptive_hash_index", classes="stats_data")
                        with Horizontal():
                            yield MetricManager.Graph(id="graph_adaptive_hash_index", classes="panel_data")
                            yield MetricManager.Graph(id="graph_adaptive_hash_index_hit_ratio", classes="panel_data")

                        with Horizontal(classes="switch_container"):
                            yield from self.generate_switches("adaptive_hash_index")

            with VerticalScroll(id="panel_replication", classes="panel_container"):
                yield Static(id="panel_replication_data", classes="panel_data")

            with Container(id="panel", classes="panel_container"):
                yield Static(id="panel_data", classes="panel_data")

            with Container(id="panel_processlist"):
                yield DataTable(id="panel_processlist_data", classes="panel_data", show_cursor=False)

            yield Static(id="footer")


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
