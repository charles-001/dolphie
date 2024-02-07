import argparse
import os
import re
import sys
from configparser import ConfigParser
from dataclasses import dataclass, field
from typing import Dict, List
from urllib.parse import urlparse

import myloginpath
from dolphie.DataTypes import Panels
from dolphie.Modules.Queries import MySQLQueries
from rich.console import Console
from rich.prompt import Prompt


@dataclass
class Config:
    app_version: str = None

    user: str = None
    password: str = None
    host: str = None
    port: int = 3306
    socket: str = None
    ssl: Dict = field(default_factory=dict)
    config_file: str = None
    host_cache_file: str = None
    host_setup_file: str = None
    refresh_interval: int = 1
    use_processlist: bool = False
    show_idle_threads: bool = False
    show_trxs_only: bool = False
    show_additional_query_columns: bool = False
    sort_by_time_descending: bool = True
    heartbeat_table: str = None
    user_filter: str = None
    db_filter: str = None
    host_filter: str = None
    query_time_filter: str = 0
    query_filter: str = None
    host_setup_available_hosts: List[str] = field(default_factory=list)
    startup_panels: str = None
    graph_marker: str = None


class ArgumentParser:
    def __init__(self, app_version: str):
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
        self.parser = argparse.ArgumentParser(
            conflict_handler="resolve",
            description="Dolphie, an intuitive feature-rich top tool for monitoring MySQL in real time",
            epilog=epilog,
            formatter_class=argparse.RawTextHelpFormatter,
        )
        self.config = Config()
        self.panels = Panels()
        self.console = Console(style="indian_red", highlight=False)

        self.config.app_version = app_version

        self._add_options()
        self._parse()

    def _add_options(self):
        self.parser.add_argument(
            "uri",
            metavar="uri",
            type=str,
            nargs="?",
            help=(
                "Use a URI string for credentials - format: mysql://user:password@host:port (port is optional with"
                " default 3306)"
            ),
        )

        self.parser.add_argument(
            "-u",
            "--user",
            dest="user",
            type=str,
            help="Username for MySQL",
        )
        self.parser.add_argument("-p", "--password", dest="password", type=str, help="Password for MySQL")
        self.parser.add_argument(
            "--ask-pass",
            dest="ask_password",
            action="store_true",
            default=False,
            help="Ask for password (hidden text)",
        )
        self.parser.add_argument(
            "-h",
            "--host",
            dest="host",
            type=str,
            help="Hostname/IP address for MySQL",
        )
        self.parser.add_argument(
            "-P",
            "--port",
            dest="port",
            type=int,
            help="Port for MySQL (Socket has precendence)",
        )
        self.parser.add_argument(
            "-S",
            "--socket",
            dest="socket",
            type=str,
            help="Socket file for MySQL",
        )
        self.parser.add_argument(
            "-c",
            "--config-file",
            dest="config_file",
            type=str,
            help=(
                "Config file path to use. This should use [client] section. "
                "See below for options support [default: ~/.my.cnf]"
            ),
        )
        self.parser.add_argument(
            "-f",
            "--host-cache-file",
            dest="host_cache_file",
            type=str,
            help=(
                "Resolve IPs to hostnames when your DNS is unable to. Each IP/hostname pair should be on its own line "
                "using format: ip=hostname [default: ~/dolphie_host_cache]"
            ),
        )
        self.parser.add_argument(
            "-q",
            "--host-setup-file",
            dest="host_setup_file",
            type=str,
            help=(
                "Specify location of file that stores the available hosts to use in host setup modal [default:"
                " ~/dolphie_hosts]"
            ),
        )
        self.parser.add_argument(
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
        self.parser.add_argument(
            "-r",
            "--refresh_interval",
            dest="refresh_interval",
            default=1,
            type=int,
            help="How much time to wait in seconds between each refresh [default: %(default)s]",
        )
        self.parser.add_argument(
            "-H",
            "--heartbeat-table",
            dest="heartbeat_table",
            type=str,
            help=(
                "If your hosts use pt-heartbeat, specify table in format db.table to use the timestamp it "
                "has for replication lag instead of Seconds_Behind_Master from SHOW SLAVE STATUS"
            ),
        )
        self.parser.add_argument(
            "--ssl-mode",
            dest="ssl_mode",
            type=str,
            help=(
                "Desired security state of the connection to the host. Supports: "
                "REQUIRED/VERIFY_CA/VERIFY_IDENTITY [default: OFF]"
            ),
        )
        self.parser.add_argument(
            "--ssl-ca",
            dest="ssl_ca",
            type=str,
            help="Path to the file that contains a PEM-formatted CA certificate",
        )
        self.parser.add_argument(
            "--ssl-cert",
            dest="ssl_cert",
            type=str,
            help="Path to the file that contains a PEM-formatted client certificate",
        )
        self.parser.add_argument(
            "--ssl-key",
            dest="ssl_key",
            type=str,
            help="Path to the file that contains a PEM-formatted private key for the client certificate",
        )
        self.parser.add_argument(
            "--panels",
            dest="startup_panels",
            default="dashboard,processlist",
            type=str,
            help=(
                "What panels to display on startup separated by a comma. Supports:"
                f" {'/'.join(self.panels.all())} [default: %(default)s]"
            ),
        )
        self.parser.add_argument(
            "--graph-marker",
            dest="graph_marker",
            default="braille",
            type=str,
            help=(
                "What marker to use for graphs (available options: https://tinyurl.com/dolphie-markers) [default:"
                " %(default)s]"
            ),
        )
        self.parser.add_argument(
            "--show-trxs-only",
            dest="show_trxs_only",
            action="store_true",
            default=False,
            help="Start with only showing threads that have an active transaction",
        )
        self.parser.add_argument(
            "--additional-columns",
            dest="show_additional_query_columns",
            action="store_true",
            default=False,
            help="Start with additional columns in Processlist panel",
        )
        self.parser.add_argument(
            "--use-processlist",
            dest="use_processlist",
            action="store_true",
            default=False,
            help="Start with using Information Schema instead of Performance Schema for processlist panel",
        )
        self.parser.add_argument(
            "-V", "--version", action="version", version=self.config.app_version, help="Display version and exit"
        )

    def _parse(self):
        home_dir = os.path.expanduser("~")

        parameter_options = vars(self.parser.parse_args())  # Convert object to dict
        basic_options = ["user", "password", "host", "port", "socket"]

        self.config.config_file = f"{home_dir}/.my.cnf"
        if parameter_options["config_file"]:
            self.config.config_file = parameter_options["config_file"]

        # Use config file for login credentials
        if os.path.isfile(self.config.config_file):
            cfg = ConfigParser()
            cfg.read(self.config.config_file)

            for option in basic_options:
                if cfg.has_option("client", option):
                    setattr(self.config, option, cfg.get("client", option))

            if cfg.has_option("client", "ssl_mode"):
                ssl_mode = cfg.get("client", "ssl_mode").upper()

                if ssl_mode == "REQUIRED":
                    self.config.ssl[""] = True
                elif ssl_mode == "VERIFY_CA":
                    self.config.ssl["check_hostname"] = False
                elif ssl_mode == "VERIFY_IDENTITY":
                    self.config.ssl["check_hostname"] = True
                else:
                    sys.exit(self.console.print(f"Unsupported SSL mode [b]{ssl_mode}[/b]"))

            if cfg.has_option("client", "ssl_ca"):
                self.config.ssl["ca"] = cfg.get("client", "ssl_ca")
            if cfg.has_option("client", "ssl_cert"):
                self.config.ssl["cert"] = cfg.get("client", "ssl_cert")
            if cfg.has_option("client", "ssl_key"):
                self.config.ssl["key"] = cfg.get("client", "ssl_key")

        # Use login path for login credentials
        if parameter_options["login_path"]:
            try:
                login_path_data = myloginpath.parse(parameter_options["login_path"])

                for option in basic_options:
                    if option in login_path_data:
                        setattr(self.config, option, login_path_data[option])
            except Exception as e:
                # Don't error out for default login path
                if parameter_options["login_path"] != "client":
                    sys.exit(self.console.print(f"Problem reading login path file: {e}"))

        # Use environment variables for basic options if specified
        for option in basic_options:
            environment_var = "DOLPHIE_%s" % option.upper()
            if environment_var in os.environ and os.environ[environment_var]:
                setattr(self.config, option, os.environ[environment_var])

        # Use parameter options if specified
        for option in basic_options:
            if parameter_options[option]:
                setattr(self.config, option, parameter_options[option])

        # Lastly, parse URI if specified
        if parameter_options["uri"]:
            try:
                parsed = urlparse(parameter_options["uri"])

                if parsed.scheme != "mysql":
                    sys.exit(
                        self.console.print(
                            "Invalid URI scheme: Only 'mysql' is supported (see --help for more information)"
                        )
                    )

                self.config.user = parsed.username
                self.config.password = parsed.password
                self.config.host = parsed.hostname
                self.config.port = parsed.port or 3306
            except Exception as e:
                sys.exit(self.console.print(f"Invalid URI: {e} (see --help for more information)"))

        if parameter_options["ask_password"]:
            self.config.password = Prompt.ask("[b #91abec]Password", password=True)

        if not self.config.host:
            self.config.host = "localhost"

        if parameter_options["refresh_interval"]:
            self.config.refresh_interval = parameter_options["refresh_interval"]

        if parameter_options["heartbeat_table"]:
            pattern_match = re.search(r"^(\w+\.\w+)$", parameter_options["heartbeat_table"])
            if pattern_match:
                self.config.heartbeat_table = parameter_options["heartbeat_table"]
                MySQLQueries.heartbeat_replica_lag = MySQLQueries.heartbeat_replica_lag.replace(
                    "$1", self.config.heartbeat_table
                )
            else:
                sys.exit(self.console.print("Your heartbeat table did not conform to the proper format: db.table"))

        if parameter_options["ssl_mode"]:
            ssl_mode = parameter_options["ssl_mode"].upper()

            if ssl_mode == "REQUIRED":
                self.config.ssl[""] = True
            elif ssl_mode == "VERIFY_CA":
                self.config.ssl["check_hostame"] = False
            elif ssl_mode == "VERIFY_IDENTITY":
                self.config.ssl["check_hostame"] = True
            else:
                sys.exit(self.console.print(f"Unsupported SSL mode [b]{ssl_mode}[/b]"))

        if parameter_options["ssl_ca"]:
            self.config.ssl["ca"] = parameter_options["ssl_ca"]
        if parameter_options["ssl_cert"]:
            self.config.ssl["cert"] = parameter_options["ssl_cert"]
        if parameter_options["ssl_key"]:
            self.config.ssl["key"] = parameter_options["ssl_key"]

        self.config.host_cache_file = f"{home_dir}/dolphie_host_cache"
        if parameter_options["host_cache_file"]:
            self.config.host_cache_file = parameter_options["host_cache_file"]

        self.config.host_setup_file = f"{home_dir}/dolphie_hosts"
        if parameter_options["host_setup_file"]:
            self.config.host_setup_file = parameter_options["host_setup_file"]

        self.config.show_trxs_only = parameter_options["show_trxs_only"]
        self.config.show_additional_query_columns = parameter_options["show_additional_query_columns"]
        self.config.use_processlist = parameter_options["use_processlist"]

        self.config.startup_panels = parameter_options["startup_panels"].split(",")
        for panel in self.config.startup_panels:
            if panel not in self.panels.all():
                sys.exit(self.console.print(f"Panel '{panel}' is not valid (see --help for more information)"))

        self.config.graph_marker = parameter_options["graph_marker"]

        if os.path.exists(self.config.host_setup_file):
            with open(self.config.host_setup_file, "r") as file:
                self.config.host_setup_available_hosts = [line.strip() for line in file]
