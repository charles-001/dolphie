#!/usr/bin/env python3

# ****************************
# *        Dolphie           *
# * Author: Charles Thompson *
# ****************************

import os
import re
from argparse import ArgumentParser, RawTextHelpFormatter
from configparser import ConfigParser
from datetime import datetime

import dolphie.QPSManager as QPSManager
import myloginpath
from dolphie import Dolphie
from dolphie.ManualException import ManualException
from dolphie.Panels import (
    dashboard_panel,
    innodb_panel,
    processlist_panel,
    replication_panel,
)
from dolphie.Queries import Queries
from dolphie.Widgets.topbar import TopBar
from rich.prompt import Prompt
from rich.traceback import Traceback
from textual import events, work
from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal, VerticalScroll
from textual.css.query import NoMatches
from textual.widgets import DataTable, Label, Sparkline, Static, Switch
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
            "Absolute config file path to use. This should use [client] section. "
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
            "using format: ip=hostname [default: %(default)s]"
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
        help="Start with only showing queries that are running a transaction",
    )
    parser.add_argument(
        "--additional-columns",
        dest="show_additional_query_columns",
        action="store_true",
        default=False,
        help="Start with additional columns in processlist panel",
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

    parameter_options = vars(parser.parse_args())  # Convert object to dict
    basic_options = ["user", "password", "host", "port", "socket"]

    # Use specified config file if there is one, else use standard ~/.my.cnf
    if parameter_options["config_file"]:
        dolphie.config_file = parameter_options["config_file"]
    else:
        dolphie.config_file = "%s/.my.cnf" % os.path.expanduser("~")

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
                raise ManualException("Unsupported SSL mode [b]%s" % ssl_mode)

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
                raise ManualException("Problem reading login path file", reason=e)

    # Use environment variables for basic options if specified
    for option in basic_options:
        environment_var = "DOLPHIE_%s" % option.upper()
        if environment_var in os.environ and os.environ[environment_var]:
            setattr(dolphie, option, os.environ[environment_var])

    # Lastly, use parameter options if specified
    for option in basic_options:
        if parameter_options[option]:
            setattr(dolphie, option, parameter_options[option])

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
            Queries["heartbeat_replica_lag"] = Queries["heartbeat_replica_lag"].replace(
                "$placeholder", dolphie.heartbeat_table
            )
        else:
            raise ManualException("Your heartbeat table did not conform to the proper format: db.table")

    if parameter_options["ssl_mode"]:
        ssl_mode = parameter_options["ssl_mode"].upper()

        if ssl_mode == "REQUIRED":
            dolphie.ssl[""] = True
        elif ssl_mode == "VERIFY_CA":
            dolphie.ssl["check_hostame"] = False
        elif ssl_mode == "VERIFY_IDENTITY":
            dolphie.ssl["check_hostame"] = True
        else:
            raise ManualException(f"Unsupported SSL mode {ssl_mode}")

    if parameter_options["ssl_ca"]:
        dolphie.ssl["ca"] = parameter_options["ssl_ca"]
    if parameter_options["ssl_cert"]:
        dolphie.ssl["cert"] = parameter_options["ssl_cert"]
    if parameter_options["ssl_key"]:
        dolphie.ssl["key"] = parameter_options["ssl_key"]

    if parameter_options["host_cache_file"]:
        dolphie.host_cache_file = parameter_options["host_cache_file"]
    else:
        dolphie.host_cache_file = os.path.dirname(os.path.abspath(__file__)) + "/host_cache"

    dolphie.show_trxs_only = parameter_options["show_trxs_only"]
    dolphie.show_additional_query_columns = parameter_options["show_additional_query_columns"]
    dolphie.use_processlist = parameter_options["use_processlist"]
    dolphie.hide_dashboard = parameter_options["hide_dashboard"]


class DolphieApp(App):
    TITLE = "Dolphie"
    CSS_PATH = "Dolphie/Dolphie.css"

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

            if not dolphie.main_db_connection:
                dolphie.db_connect()

            dolphie.statuses = dolphie.main_db_connection.fetch_data("status")

            if dolphie.display_dashboard_panel:
                dolphie.binlog_status = dolphie.main_db_connection.fetch_data("binlog_status")

            if dolphie.display_dashboard_panel or dolphie.display_innodb_panel:
                dolphie.variables = dolphie.main_db_connection.fetch_data("variables")
                dolphie.innodb_status = dolphie.main_db_connection.fetch_data("innodb_status")

            if dolphie.display_dashboard_panel or dolphie.display_replication_panel:
                dolphie.replication_status = dolphie.main_db_connection.fetch_data("replication_status")

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
        except ManualException as e:
            self.exit(message=e.output())

    def on_worker_state_changed(self, event: Worker.StateChanged):
        if event.state != WorkerState.SUCCESS:
            return

        if len(self.screen_stack) > 1 or self.dolphie.pause_refresh:
            self.set_timer(self.dolphie.refresh_interval, self.worker_fetch_data)
            return

        dolphie = self.dolphie

        loop_time = datetime.now()
        dolphie.loop_duration_seconds = (loop_time - dolphie.previous_main_loop_time).total_seconds()

        try:
            if dolphie.display_dashboard_panel:
                self.query_one("#dashboard_panel_data", Static).update(dashboard_panel.create_panel(dolphie))

            if dolphie.display_processlist_panel:
                processlist_panel.create_panel(dolphie)

            if dolphie.display_replication_panel:
                self.query_one("#replication_panel_data", Static).update(replication_panel.create_panel(dolphie))

            if dolphie.display_innodb_panel:
                self.query_one("#innodb_panel_data", Static).update(innodb_panel.create_panel(dolphie))

            QPSManager.update_data(dolphie)
            self.query_one("#dml_panel_graph").update(QPSManager.create_plot(dolphie.qps_data))
        except NoMatches:
            pass

        dolphie.saved_status = dolphie.statuses.copy()
        dolphie.previous_main_loop_time = loop_time

        # We take a snapshot of the processlist to be used for commands
        # since the data can change after a key is pressed
        dolphie.processlist_threads_snapshot = dolphie.processlist_threads.copy()

        self.set_timer(self.dolphie.refresh_interval, self.worker_fetch_data)

    def on_key(self, event: events.Key):
        self.dolphie.capture_key(event.key)

    def on_mount(self):
        dolphie = self.dolphie
        dolphie.load_host_cache_file()

        # Set these components by default to not show
        components_to_disable = [
            "replication_panel",
            "innodb_panel",
            "footer",
            "dashboard_panel_queries",
            "dml_panel",
        ]
        for component in components_to_disable:
            self.query_one(f"#{component}").display = False

        dolphie.display_processlist_panel = True
        if dolphie.hide_dashboard:
            self.query_one("#dashboard_panel", Container).display = False
            dolphie.display_dashboard_panel = False
        else:
            self.query_one("#dashboard_panel_data", Static).update("[b #91abec]Loading Dashboard panel[/b #91abec]")
            dolphie.display_dashboard_panel = True

        # Set default switches to be toggled on
        dml_switches = self.query("#switch_container Switch")
        for switch in dml_switches:
            switch: Switch
            if switch.id != "dml_panel_queries_switch":
                switch.toggle()

        dolphie.check_for_update()

        # Update header's host
        header = self.app.query_one("#topbar_host", Label)
        header.update("Connecting to MySQL...")

        self.worker_fetch_data()

    def _handle_exception(self, error: Exception) -> None:
        self.bell()

        # We have a ManualException class that we use to output errors in a nice format
        if error.__class__.__name__ == "ManualException":
            message = error.output()
        else:
            message = Traceback(show_locals=True, width=None, locals_max_length=5)

        self.exit(message=message)

    def on_switch_changed(self, event: Switch.Changed):
        if len(self.screen_stack) > 1:
            return

        # Upon switch flip, update the plot data's dictionary to show/hide a DML type
        dml_type = event.switch.id.split("_")[2]
        self.dolphie.qps_data[f"plot_data_{dml_type}"]["visible"] = event.value
        self.query_one("#dml_panel_graph").update(QPSManager.create_plot(self.dolphie.qps_data))

    def compose(self) -> ComposeResult:
        yield TopBar(app_version=self.dolphie.app_version, help="press ? for help")

        with VerticalScroll(id="main_container"):
            with Container(id="dashboard_panel", classes="panel_container"):
                yield Static(id="dashboard_panel_data", classes="panel_data")
                yield Sparkline([], id="dashboard_panel_queries")

            with Container(id="dml_panel", classes="panel_container"):
                yield Static(id="dml_panel_graph", classes="panel_data")
                with Horizontal(id="switch_container"):
                    dml_types = ["QUERIES", "SELECT", "INSERT", "UPDATE", "DELETE"]
                    for dml_type in dml_types:
                        yield Label(dml_type)
                        yield Switch(animate=False, id=f"dml_panel_{dml_type.lower()}_switch")

            with VerticalScroll(id="replication_panel", classes="panel_container"):
                yield Static(id="replication_panel_data", classes="panel_data")

            with Container(id="innodb_panel", classes="panel_container"):
                yield Static(id="innodb_panel_data", classes="panel_data")

            with Container(id="processlist_panel"):
                yield DataTable(id="processlist_panel_data", classes="panel_data", show_cursor=False)

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
