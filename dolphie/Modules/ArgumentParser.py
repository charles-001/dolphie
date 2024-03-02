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
from rich import box
from rich.console import Console
from rich.table import Table
from rich.theme import Theme


@dataclass
class Config:
    app_version: str
    host_setup: bool = False
    user: str = None
    password: str = None
    host: str = "localhost"
    port: int = 3306
    socket: str = None
    ssl: Dict = field(default_factory=dict)
    ssl_mode: str = None
    ssl_ca: str = None
    ssl_cert: str = None
    ssl_key: str = None
    config_file: str = field(default_factory=lambda: f"{os.path.expanduser('~')}/.dolphie.cnf")
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
    hostgroup: str = None
    hostgroup_hosts: Dict[str, List[str]] = field(default_factory=dict)
    show_trxs_only: bool = False
    show_additional_query_columns: bool = False
    # historical_trx_locks: bool = False


class ArgumentParser:
    def __init__(self, app_version: str):
        self.config_options = {}
        for variable in fields(Config):
            # Exclude these options since we handle them differently
            if variable.name not in [
                "app_version",
                "config_file",
                "host_setup_available_hosts",
                "ssl",
                "hostgroup_hosts",
            ]:
                self.config_options[variable.name] = variable.type

        self.formatted_options = "\n\t".join(
            [f"({data_type.__name__}) {option}" for option, data_type in self.config_options.items()]
        )
        epilog = f"""
Order of precedence for methods that pass options to Dolphie:
\t1. Command-line
\t2. Environment variables
\t3. Dolphie's config (set by --config-file)
\t4. ~/.mylogin.cnf (mysql_config_editor)
\t5. ~/.my.cnf (set by --mycnf-file)

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

Dolphie's config supports these options under [dolphie] section:
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

        self.console = Console(style="#e9e9e9", highlight=False)
        self.console.push_theme(
            Theme(
                {
                    "red2": "b #fb9a9a",
                }
            )
        )

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

        self.parser.add_argument(
            "--host-setup",
            dest="host_setup",
            action="store_true",
            help="Start Dolphie by showing the Host Setup modal instead of automatically connecting",
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
            help="Port for MySQL (socket has precendence)",
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
            help=(
                f"Dolphie's config file to use. Options are read from these files in the given order: "
                f"/etc/dolphie.cnf, {self.config.config_file}"
            ),
            metavar="",
        )
        self.parser.add_argument(
            "--mycnf-file",
            dest="mycnf_file",
            type=str,
            help=(
                "MySQL config file path to use. This should use [client] section "
                f"[default: {self.config.mycnf_file}]"
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
                f"using format ip=hostname [default: {self.config.host_cache_file}]"
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
                "Specify login path to use with mysql_config_editor's file ~/.mylogin.cnf for encrypted login"
                f" credentials [default: {self.config.login_path}]"
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
                "has for replication lag instead of Seconds_Behind_Master from SHOW REPLICA STATUS"
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
            help="Path to the file that contains a CA (certificate authority)",
            metavar="",
        )
        self.parser.add_argument(
            "--ssl-cert",
            dest="ssl_cert",
            type=str,
            help="Path to the file that contains a certificate",
            metavar="",
        )
        self.parser.add_argument(
            "--ssl-key",
            dest="ssl_key",
            type=str,
            help="Path to the file that contains a private key for the certificate",
            metavar="",
        )
        self.parser.add_argument(
            "--panels",
            dest="startup_panels",
            type=str,
            help=(
                "What panels to display on startup separated by a comma. Supports:"
                f" {','.join(self.panels.all())} [default: {self.config.startup_panels}]"
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
            "--hostgroup",
            dest="hostgroup",
            type=str,
            help=(
                "This is used for creating tabs and connecting to them for hosts you specify in"
                " Dolphie's config file under a hostgroup section. As an example, you'll have a section"
                " called [cluster1] then below it you will list each host on a new line in the format"
                " key=host (keys have no meaning). Hosts support optional port (default is whatever port parameter is)"
                " in the format host:port. You can also name the tabs by suffixing"
                " ~tab_name to the host (i.e. 1=host~tab_name)"
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
        # self.parser.add_argument(
        #     "--historical-trx-locks",
        #     dest="historical_trx_locks",
        #     action="store_true",
        #     help=(
        #         "Always run the InnoDB TRX Locks query so it can save historical data to its graph instead of only "
        #         "when the panel is open. This query can be expensive in some environments"
        #     ),
        # )
        self.parser.add_argument(
            "--debug-options",
            dest="debug_options",
            action="store_true",
            help="Display options that are set and what they're set by (command-line, dolphie config, etc) then exit",
        )
        self.parser.add_argument(
            "-V",
            "--version",
            action="version",
            version=self.config.app_version,
            help="Display version and exit",
        )

    def set_config_value(self, source, option, value):
        setattr(self.config, option, value)

        if self.debug_options:
            self.debug_options_table.add_row(source, option, str(value))

    def _parse(self):
        login_options = ["user", "password", "host", "port", "socket"]

        options = vars(self.parser.parse_args())  # Convert object to dictionary

        dolphie_config_login_options_used = {}
        hostgroups = {}

        self.debug_options = False
        if options["debug_options"]:
            self.debug_options = True

            self.debug_options_table = Table(box=box.SIMPLE_HEAVY, header_style="b", style="#333f62")
            self.debug_options_table.add_column("Source")
            self.debug_options_table.add_column("Option", style="#91abec")
            self.debug_options_table.add_column("Value", style="#bbc8e8")

        config_files = ["/etc/dolphie.cnf", self.config.config_file]
        if options["config_file"]:
            config_files = [options["config_file"]]

        # Loop through config files to find the supplied options
        for config_file in config_files:
            if os.path.isfile(config_file):
                cfg = ConfigParser()
                cfg.read(config_file)

                # Loop through all of available options
                for option, data_type in self.config_options.items():
                    # If the option is in the config file
                    if cfg.has_option("dolphie", option):
                        # Check if the value is of the correct data type
                        value = self.verify_config_value(option, cfg.get("dolphie", option), data_type)

                        # Set the option to the value from the config file
                        self.set_config_value(f"dolphie config {config_file}", option, value)

                        # Save the login option to be used later
                        if option in login_options:
                            dolphie_config_login_options_used[option] = value

                # Save all hostgroups found to the config object
                for hostgroup in cfg.sections():
                    hosts = []
                    if hostgroup == "dolphie":
                        continue

                    for key in cfg.options(hostgroup):
                        host = cfg.get(hostgroup, key).strip()
                        if host:
                            hosts.append(host)
                        else:
                            self.exit(
                                f"{config_file}: Hostgroup [red2]{hostgroup}[/red2] has an empty host"
                                f" for key [red2]{key}[/red2]"
                            )

                    if not hosts:
                        self.exit(
                            f"{config_file}: Hostgroup [red2]{hostgroup}[/red2] cannot be loaded because"
                            f" it doesn't have any hosts listed under its section in Dolphie's config"
                        )

                    hostgroups[hostgroup] = hosts

        # Save the hostgroups found to the config object
        self.config.hostgroup_hosts = hostgroups

        # If the options are not set from Dolphie configs, set them to what command-line is or default
        for option in self.config_options.keys():
            value = getattr(self.config, option)

            # Override the option with command-line arguments if the option isn't for login
            if option not in login_options and options[option] and value != options[option]:
                self.set_config_value("command-line", option, options[option])

            # If the option is not set, set it to the default from Config's dataclass
            if not options[option]:
                options[option] = value

        # Use MySQL's my.cnf file for login options if specified
        if os.path.isfile(self.config.mycnf_file):
            cfg = ConfigParser()
            cfg.read(self.config.mycnf_file)

            for option in login_options:
                if cfg.has_option("client", option):
                    self.set_config_value("my.cnf", option, cfg.get("client", option))

            self.parse_ssl_options(cfg)

        # Use login path for login options if specified
        if self.config.login_path:
            try:
                login_path_data = myloginpath.parse(self.config.login_path)

                for option in login_options:
                    if option in login_path_data:
                        self.set_config_value("login path", option, login_path_data[option])
            except Exception as e:
                # Don't error out for default login path
                if self.config.login_path != "client":
                    self.exit(f"Problem reading login path file: {e}")

        # Update login options based on precedence
        for option in login_options:
            # Update config object with Dolphie config
            dolphie_value = dolphie_config_login_options_used.get(option)
            if dolphie_value is not None:
                self.set_config_value("dolphie config", option, dolphie_value)

            # Use environment variables if specified
            environment_var = f"DOLPHIE_{option.upper()}"
            env_value = os.environ.get(environment_var)
            if env_value is not None:
                self.set_config_value("environment", option, env_value)

            # Use command-line arguments if specified
            if options.get(option):
                self.set_config_value("command-line", option, options[option])

        # Lastly, parse URI if specified
        if options["uri"]:
            try:
                parsed = urlparse(options["uri"])

                if parsed.scheme != "mysql":
                    self.exit("Invalid URI scheme: Only 'mysql' is supported (see --help for more information)")

                self.config.user = parsed.username
                self.config.password = parsed.password
                self.config.host = parsed.hostname
                self.config.port = parsed.port or 3306
            except Exception as e:
                self.exit(f"Invalid URI: {e} (see --help for more information)")

        # Sanity check for hostgroup
        if self.config.hostgroup:
            if self.config.hostgroup not in hostgroups:
                self.exit(
                    f"Hostgroup [red2]{self.config.hostgroup}[/red2] cannot be used because"
                    f" it wasn't found in Dolphie's config"
                )

        if self.config.heartbeat_table:
            pattern_match = re.search(r"^(\w+\.\w+)$", self.config.heartbeat_table)
            if pattern_match:
                MySQLQueries.heartbeat_replica_lag = MySQLQueries.heartbeat_replica_lag.replace(
                    "$1", self.config.heartbeat_table
                )
            else:
                self.exit("Your heartbeat table did not conform to the proper format: db.table")

        self.parse_ssl_options(options)

        self.config.startup_panels = self.config.startup_panels.split(",")
        for panel in self.config.startup_panels:
            if panel not in self.panels.all():
                self.exit(f"Panel [red2]{panel}[/red2] is not valid (see --help for more information)")

        if os.path.exists(self.config.host_setup_file):
            with open(self.config.host_setup_file, "r") as file:
                self.config.host_setup_available_hosts = [line.strip() for line in file]

        if self.debug_options:
            self.console.print(self.debug_options_table)
            self.console.print("[#969aad]Note: Options are set by their source in the order they appear")
            sys.exit()

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
                self.config.ssl["required"] = True
            elif ssl_mode == "VERIFY_CA":
                if not ssl_ca:
                    self.exit("SSL mode [red2]VERIFY_CA[/red2] requires a CA file (--ssl-ca) to be specified")

                self.config.ssl["check_hostname"] = False
                self.config.ssl["verify_mode"] = True
            elif ssl_mode == "VERIFY_IDENTITY":
                if not ssl_ca:
                    self.exit("SSL mode [red2]VERIFY_IDENTITY[/red2] requires a CA file (--ssl-ca) to be specified")

                self.config.ssl["check_hostname"] = True
                self.config.ssl["verify_mode"] = True
            else:
                self.exit(f"Unsupported SSL mode [red2]{ssl_mode}[/red2]")

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
                self.exit(
                    f"Error with Dolphie config: [red2]{option}[/red2] is a boolean and must either be true/false"
                )

        elif data_type == int:
            try:
                return int(value)
            except ValueError:
                self.exit(f"Error with Dolphie config: [red2]{option}[/red2] is an integer and must be a number")
        else:
            return value

    def exit(self, message):
        self.console.print(f"[indian_red]{message}[/indian_red]")
        sys.exit()
