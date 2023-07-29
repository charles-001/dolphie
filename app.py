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

import myloginpath
from dolphie import Dolphie
from dolphie.ManualException import ManualException
from dolphie.Panels import (
    dashboard_panel,
    innodb_io_panel,
    innodb_locks_panel,
    query_panel,
    replica_panel,
)
from dolphie.Queries import Queries
from rich.align import Align
from rich.prompt import Prompt
from textual import events
from textual.app import App, ComposeResult
from textual.containers import Horizontal, VerticalScroll
from textual.widgets import Label, Static, Switch


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
        dolphie.password = Prompt.ask("[b steel_blue1]Password", password=True)

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

    # Update header's host
    dolphie.header.host = dolphie.host


class DolphieApp(App):
    TITLE = "Dolphie"
    CSS_PATH = "dolphie/dolphie.css"

    def __init__(self):
        super().__init__()
        self.dolphie = Dolphie(self)

    def update_display(self):
        if len(self.screen_stack) > 1 or self.dolphie.pause_refresh:
            self.set_timer(self.dolphie.refresh_interval, self.update_display)
            return

        dolphie = self.dolphie
        loop_time = datetime.now()

        dolphie.statuses = dolphie.fetch_data("status")
        if not dolphie.saved_status:
            dolphie.saved_status = dolphie.statuses.copy()

        dashboard = self.query_one("#dashboard_panel")
        if dashboard.display:
            dolphie.variables = dolphie.fetch_data("variables")
            dolphie.primary_status = dolphie.fetch_data("primary_status")
            dolphie.replica_status = dolphie.fetch_data("replica_status")
            dolphie.innodb_status = dolphie.fetch_data("innodb_status")

            dashboard.update(dashboard_panel.create_panel(self.dolphie))

        processlist = self.query_one("#processlist_panel")
        if processlist.display:
            query_panel.create_panel(self.dolphie)

        replica = self.query_one("#replica_panel")
        if replica.display:
            replica.update(replica_panel.create_panel(self.dolphie))

        innodb_io = self.query_one("#innodb_io_panel")
        if innodb_io.display:
            innodb_io.update(innodb_io_panel.create_panel(self.dolphie))

        innodb_locks = self.query_one("#innodb_locks_panel")
        if innodb_locks.display:
            innodb_locks.update(innodb_locks_panel.create_panel(self.dolphie))

        # This is for the many stats per second in Dolphie
        dolphie.saved_status = dolphie.statuses.copy()

        dolphie.loop_duration_seconds = (loop_time - dolphie.previous_main_loop_time).total_seconds()
        dolphie.previous_main_loop_time = loop_time

        self.set_timer(self.dolphie.refresh_interval, self.update_display)

    def on_key(self, event: events.Key):
        self.dolphie.capture_key(event.key)

    def on_mount(self):
        parse_args(self.dolphie)
        # self.dolphie.check_for_update()
        self.dolphie.db_connect()
        self.dolphie.load_host_cache_file()

        # Set default panels to not show
        panels = self.query(".panel")
        for panel in panels:
            panel.display = False
        footer = self.query_one("#footer")
        footer.display = False

        # To not make the default panels lag when they're first shown, we set display to true here
        # instead of toggle
        processlist = self.query_one("#processlist_panel")
        processlist.display = True
        if not self.dolphie.hide_dashboard:
            dashboard = self.query_one("#dashboard_panel")
            dashboard.display = True

        # Set default switches to be toggled on
        switches_to_toggle_on = ["switch_dashboard", "switch_processlist"]
        for switch_name in switches_to_toggle_on:
            if switch_name == "switch_dashboard" and self.dolphie.hide_dashboard:
                continue

            self.query_one(f"#{switch_name}").toggle()

        self.update_display()

    def on_switch_changed(self, event: Switch.Changed):
        if len(self.screen_stack) > 1:
            return

        panels = {
            "switch_dashboard": self.query_one("#dashboard_panel"),
            "switch_processlist": self.query_one("#processlist_panel"),
            "switch_replication": self.query_one("#replica_panel"),
            "switch_innodb_io": self.query_one("#innodb_io_panel"),
            "switch_innodb_locks": self.query_one("#innodb_locks_panel"),
        }

        panel = panels.get(event.switch.id)
        if panel:
            if panel.display != event.value:
                if panel.id == "replica_panel":
                    if self.dolphie.use_performance_schema:
                        find_replicas_query = Queries["ps_find_replicas"]
                    else:
                        find_replicas_query = Queries["pl_find_replicas"]

                    self.dolphie.db.cursor.execute(find_replicas_query)
                    data = self.dolphie.db.fetchall()
                    if not data and not self.dolphie.replica_status:
                        self.dolphie.update_footer(
                            "[b]Cannot use this panel![/b] This host is not a replica and has no replicas connected"
                        )
                        event.switch.toggle()
                        return

                    if panel.display:
                        for connection in self.dolphie.replica_connections.values():
                            connection["connection"].close()

                        self.dolphie.replica_connections = {}
                elif panel.id == "innodb_locks_panel":
                    if not self.dolphie.innodb_locks_sql:
                        self.dolphie.update_footer(
                            "[b]Cannot use this panel![/b] InnoDB Locks panel isn't supported for this host's version"
                        )
                        event.switch.toggle()
                        return

                if panel.id != "processlist_panel":
                    panel.update(Align.center("[b #91abec]Loading[/b #91abec]…"))

                panel.display = event.value

    def compose(self) -> ComposeResult:
        yield self.dolphie.header
        with Horizontal(id="main_switch_container"):
            yield Label("Dashboard")
            yield Switch(animate=False, id="switch_dashboard")
            yield Label("Processlist")
            yield Switch(animate=False, id="switch_processlist")
            yield Label("Replication")
            yield Switch(animate=False, id="switch_replication")
            yield Label("InnoDB IO")
            yield Switch(animate=False, id="switch_innodb_io")
            yield Label("InnoDB Locks")
            yield Switch(animate=False, id="switch_innodb_locks")
        yield Static(id="dashboard_panel", classes="panel")
        yield Static(id="replica_panel", classes="panel")
        yield Static(id="innodb_io_panel", classes="panel")
        yield Static(id="innodb_locks_panel", classes="panel")
        yield VerticalScroll(self.dolphie.processlist_datatable, id="processlist_panel", classes="panel")
        yield Static(id="footer")


def main():
    # Set environment variables so Textual can use all the pretty colors
    os.environ["TERM"] = "xterm-256color"
    os.environ["COLORTERM"] = "truecolor"

    app = DolphieApp()
    app.run()


if __name__ == "__main__":
    main()
