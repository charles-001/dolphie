import argparse
import os
import re
import sys
from configparser import ConfigParser
from dataclasses import dataclass, field, fields
from typing import Dict, List
from urllib.parse import urlparse

import myloginpath
from dolphie.DataTypes import Panels
from dolphie.Modules.Queries import MySQLQueries
from rich.console import Console


@dataclass
class Config:
    app_version: str
    user: str = None
    password: str = None
    host: str = None
    port: int = 3306
    socket: str = None
    ssl: Dict = field(default_factory=dict)
    ssl_mode: str = None
    ssl_ca: str = None
    ssl_cert: str = None
    ssl_key: str = None
    config_file: str = field(default_factory=lambda: f"{os.path.expanduser('~')}/.dolphie")
    mycnf_file: str = field(default_factory=lambda: f"{os.path.expanduser('~')}/.my.cnf")
    login_path: str = "client"
    host_cache_file: str = field(default_factory=lambda: f"{os.path.expanduser('~')}/dolphie_host_cache")
    host_setup_file: str = field(default_factory=lambda: f"{os.path.expanduser('~')}/dolphie_hosts")
    refresh_interval: int = 1
    heartbeat_table: str = None
    host_setup_available_hosts: List[str] = field(default_factory=list)
    startup_panels: str = "dashboard,processlist"
    graph_marker: str = "braille"
    pypi_repository: str = "https://pypi.org/pypi/dolphie/json"
    show_trxs_only: bool = False
    show_additional_query_columns: bool = False
    historical_locks: bool = False


class ArgumentParser:
    def __init__(self, app_version: str):
        self.config_options = {}
        for variable in fields(Config):
            if variable.name not in ["app_version", "config_file", "host_setup_available_hosts", "ssl"]:
                self.config_options[variable.name] = variable.type

        self.formatted_options = "\n\t".join(
            [f"({data_type.__name__}) {option}" for option, data_type in self.config_options.items()]
        )
        epilog = f"""
MySQL my.cnf file supports these options under [client] section:
\thost
\tuser
\tpassword
\tport
\tsocket
\tssl_mode REQUIRED/VERIFY_CA/VERIFY_IDENTITY
\tssl_ca
\tssl_cert
\tssl_key

Login path file supports these options:
\thost
\tuser
\tpassword
\tport
\tsocket

Environment variables support these options:
\tDOLPHIE_USER
\tDOLPHIE_PASSWORD
\tDOLPHIE_HOST
\tDOLPHIE_PORT
\tDOLPHIE_SOCKET

Dolphie config file supports these options under [dolphie] section:
\t{self.formatted_options}
"""
        self.parser = argparse.ArgumentParser(
            conflict_handler="resolve",
            description="Dolphie, an intuitive feature-rich top tool for monitoring MySQL in real time",
            epilog=epilog,
            formatter_class=argparse.RawTextHelpFormatter,
        )
        self.config = Config(app_version)
        self.panels = Panels()
        self.console = Console(style="indian_red", highlight=False)

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
                f" default {self.config.port})"
            ),
        )

        self.parser.add_argument("-u", "--user", dest="user", type=str, help="Username for MySQL", metavar="")
        self.parser.add_argument("-p", "--password", dest="password", type=str, help="Password for MySQL", metavar="")
        self.parser.add_argument(
            "-h",
            "--host",
            dest="host",
            type=str,
            help="Hostname/IP address for MySQL",
            metavar="",
        )
        self.parser.add_argument(
            "-P",
            "--port",
            dest="port",
            type=int,
            help="Port for MySQL (Socket has precendence)",
            metavar="",
        )
        self.parser.add_argument(
            "-S",
            "--socket",
            dest="socket",
            type=str,
            help="Socket file for MySQL",
            metavar="",
        )
        self.parser.add_argument(
            "--config-file",
            dest="config_file",
            type=str,
            help=f"Dolphie's config file to use [default: {self.config.config_file}]",
            metavar="",
        )
        self.parser.add_argument(
            "--mycnf-file",
            dest="mycnf_file",
            type=str,
            help=(
                "MySQL config file path to use. This should use [client] section. "
                f"See below for options support [default: {self.config.mycnf_file}]"
            ),
            metavar="",
        )
        self.parser.add_argument(
            "-f",
            "--host-cache-file",
            dest="host_cache_file",
            type=str,
            help=(
                "Resolve IPs to hostnames when your DNS is unable to. Each IP/hostname pair should be on its own line "
                f"using format: ip=hostname [default: {self.config.host_cache_file}]"
            ),
            metavar="",
        )
        self.parser.add_argument(
            "-q",
            "--host-setup-file",
            dest="host_setup_file",
            type=str,
            help=(
                "Specify location of file that stores the available hosts to use in host setup modal [default:"
                f" {self.config.host_setup_file}]"
            ),
            metavar="",
        )
        self.parser.add_argument(
            "-l",
            "--login-path",
            dest="login_path",
            type=str,
            help=(
                "Specify login path to use mysql_config_editor's file ~/.mylogin.cnf for encrypted login credentials. "
                f"Supercedes config file [default: {self.config.login_path}]"
            ),
            metavar="",
        )
        self.parser.add_argument(
            "-r",
            "--refresh_interval",
            dest="refresh_interval",
            type=int,
            help=f"How much time to wait in seconds between each refresh [default: {self.config.refresh_interval}]",
            metavar="",
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
            metavar="",
        )
        self.parser.add_argument(
            "--ssl-mode",
            dest="ssl_mode",
            type=str,
            help=(
                "Desired security state of the connection to the host. Supports: "
                "REQUIRED/VERIFY_CA/VERIFY_IDENTITY [default: OFF]"
            ),
            metavar="",
        )
        self.parser.add_argument(
            "--ssl-ca",
            dest="ssl_ca",
            type=str,
            help="Path to the file that contains a PEM-formatted CA certificate",
            metavar="",
        )
        self.parser.add_argument(
            "--ssl-cert",
            dest="ssl_cert",
            type=str,
            help="Path to the file that contains a PEM-formatted client certificate",
            metavar="",
        )
        self.parser.add_argument(
            "--ssl-key",
            dest="ssl_key",
            type=str,
            help="Path to the file that contains a PEM-formatted private key for the client certificate",
            metavar="",
        )
        self.parser.add_argument(
            "--panels",
            dest="startup_panels",
            type=str,
            help=(
                "What panels to display on startup separated by a comma. Supports:"
                f" {'/'.join(self.panels.all())} [default: {self.config.startup_panels}]"
            ),
            metavar="",
        )
        self.parser.add_argument(
            "--graph-marker",
            dest="graph_marker",
            type=str,
            help=(
                "What marker to use for graphs (available options: https://tinyurl.com/dolphie-markers) [default:"
                f" {self.config.graph_marker}]"
            ),
            metavar="",
        )
        self.parser.add_argument(
            "--pypi-repository",
            dest="pypi_repository",
            type=str,
            help=(
                "What PyPi repository to use when checking for a new version."
                " If not specified, it will use Dolphie's PyPi repository"
            ),
            metavar="",
        )
        self.parser.add_argument(
            "--show-trxs-only",
            dest="show_trxs_only",
            action="store_true",
            help="Start with only showing threads that have an active transaction",
        )
        self.parser.add_argument(
            "--additional-columns",
            dest="show_additional_query_columns",
            action="store_true",
            help="Start with additional columns in Processlist panel",
        )
        self.parser.add_argument(
            "--historical-locks",
            dest="historical_locks",
            action="store_true",
            help=(
                "Always run the locks query so it can save historical data to its graph instead of only when "
                "the Locks panel is open. This query can be expensive in some environments"
            ),
        )
        self.parser.add_argument(
            "-V",
            "--version",
            action="version",
            version=self.config.app_version,
            help="Display version and exit",
        )

    def _parse(self):
        login_options = ["user", "password", "host", "port", "socket"]

        options = vars(self.parser.parse_args())  # Convert object to dictionary

        command_line_login_options_used = {option: options[option] for option in login_options if options[option]}

        if options["config_file"]:
            self.config.config_file = options["config_file"]

        dolphie_config_login_options_used = {}
        if os.path.isfile(self.config.config_file):
            cfg = ConfigParser()
            cfg.read(self.config.config_file)

            # Loop through all of available options
            for option, data_type in self.config_options.items():
                # If the option is in the config file
                if cfg.has_option("dolphie", option):
                    # Check if the value is of the correct data type
                    value = self.verify_config_value(option, cfg.get("dolphie", option), data_type)

                    # Set the option to the value from the config file
                    setattr(self.config, option, value)
                    # print("dolphie config - Setting", option, "to", value)

                    # Save the login option to be used later
                    if option in login_options:
                        dolphie_config_login_options_used[option] = value

        # If the options are not set from Dolphie config, set them to what command-line is or default
        for option in self.config_options.keys():
            value = getattr(self.config, option)

            # Override the option with command-line arguments
            if options[option] and value != options[option]:
                setattr(self.config, option, options[option])
                # print("command line - Setting", option, "to", options[option])

            # If the option is still not set, set it to the default from dataclass
            if not options[option]:
                options[option] = getattr(self.config, option)

        # Use MySQL's my.cnf file for login options if specified
        self.config.mycnf_file = options.get("mycnf_file")
        if os.path.isfile(self.config.mycnf_file):
            cfg = ConfigParser()
            cfg.read(self.config.mycnf_file)

            for option in login_options:
                if cfg.has_option("client", option):
                    setattr(self.config, option, cfg.get("client", option))
                    # print("my.cnf - Setting", option, "to", cfg.get("client", option))

            self.parse_ssl_options(cfg)

        # Use login path for login options if specified
        if options["login_path"]:
            try:
                login_path_data = myloginpath.parse(options["login_path"])

                for option in login_options:
                    if option in login_path_data:
                        setattr(self.config, option, login_path_data[option])
                        # print("login path - Setting", option, "to", login_path_data[option])
            except Exception as e:
                # Don't error out for default login path
                if options["login_path"] != "client":
                    sys.exit(self.console.print(f"Problem reading login path file: {e}"))

        # Update config object with Dolphie config for all options at this point
        for option, value in dolphie_config_login_options_used.items():
            setattr(self.config, option, value)
            # print("dolphie config - Setting", option, "to", value)

        for option in login_options:
            # Use environment variables if specified
            environment_var = "DOLPHIE_%s" % option.upper()
            if environment_var in os.environ and os.environ[environment_var]:
                setattr(self.config, option, os.environ[environment_var])
                # print("environment - Setting", option, "to", os.environ[environment_var])

        # Override login options with command-line arguments that we saved earlier
        for option, value in command_line_login_options_used.items():
            setattr(self.config, option, value)
            # print("command line - Setting", option, "to", value)

        # Lastly, parse URI if specified
        if options["uri"]:
            try:
                parsed = urlparse(options["uri"])

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

        if not self.config.host:
            self.config.host = "localhost"

        if options["heartbeat_table"]:
            pattern_match = re.search(r"^(\w+\.\w+)$", options["heartbeat_table"])
            if pattern_match:
                self.config.heartbeat_table = options["heartbeat_table"]
                MySQLQueries.heartbeat_replica_lag = MySQLQueries.heartbeat_replica_lag.replace(
                    "$1", self.config.heartbeat_table
                )
            else:
                sys.exit(self.console.print("Your heartbeat table did not conform to the proper format: db.table"))

        self.parse_ssl_options(options)

        self.config.host_cache_file = options.get("host_cache_file")
        self.config.host_setup_file = options.get("host_setup_file")

        self.config.show_trxs_only = options["show_trxs_only"]
        self.config.show_additional_query_columns = options["show_additional_query_columns"]

        self.config.pypi_repository = options["pypi_repository"]
        self.config.historical_locks = options["historical_locks"]

        self.config.startup_panels = options["startup_panels"].split(",")
        for panel in self.config.startup_panels:
            if panel not in self.panels.all():
                sys.exit(self.console.print(f"Panel '{panel}' is not valid (see --help for more information)"))

        self.config.graph_marker = options["graph_marker"]

        if os.path.exists(self.config.host_setup_file):
            with open(self.config.host_setup_file, "r") as file:
                self.config.host_setup_available_hosts = [line.strip() for line in file]

    def parse_ssl_options(self, data):
        if isinstance(data, ConfigParser):
            ssl_mode = data.get("client", "ssl_mode", fallback=None)
            ssl_ca = data.get("client", "ssl_ca", fallback=None)
            ssl_cert = data.get("client", "ssl_cert", fallback=None)
            ssl_key = data.get("client", "ssl_key", fallback=None)
        else:
            ssl_mode = data.get("ssl_mode")
            ssl_ca = data.get("ssl_ca")
            ssl_cert = data.get("ssl_cert")
            ssl_key = data.get("ssl_key")

        if ssl_mode:
            ssl_mode = ssl_mode.upper()

            if ssl_mode == "REQUIRED":
                self.config.ssl[""] = True
            elif ssl_mode == "VERIFY_CA":
                self.config.ssl["check_hostname"] = False
            elif ssl_mode == "VERIFY_IDENTITY":
                self.config.ssl["check_hostname"] = True
            else:
                sys.exit(self.console.print(f"Unsupported SSL mode [b]{ssl_mode}[/b]"))

        if ssl_ca:
            self.config.ssl["ca"] = ssl_ca
        if ssl_cert:
            self.config.ssl["cert"] = ssl_cert
        if ssl_key:
            self.config.ssl["key"] = ssl_key

    def verify_config_value(self, option, value, data_type):
        if data_type == bool:
            if value.lower() == "true":
                return True
            elif value.lower() == "false":
                return False
            else:
                sys.exit(
                    self.console.print(
                        f"Error with dolphie config: {option} is a boolean and must either be true/false"
                    )
                )
        elif data_type == int:
            try:
                return int(value)
            except ValueError:
                sys.exit(self.console.print(f"Error with dolphie config: {option} is an integer and must be a number"))
        else:
            return value
