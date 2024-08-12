import argparse
import os
import re
import sys
from configparser import ConfigParser
from dataclasses import dataclass, field, fields
from typing import Dict, List
from urllib.parse import ParseResult, urlparse

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
    config_file: List[str] = field(
        default_factory=lambda: [
            "/etc/dolphie.cnf",
            "/etc/dolphie/dolphie.cnf",
            f"{os.path.expanduser('~')}/.dolphie.cnf",
        ]
    )
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
    record_for_replay: bool = False
    daemon_mode: bool = False
    daemon_mode_log_file: str = field(default_factory=lambda: f"{os.path.expanduser('~')}/dolphie_daemon.log")
    replay_file: str = None
    replay_dir: str = None
    replay_retention_hours: int = 48
    exclude_notify_global_vars: str = None


class ArgumentParser:
    def __init__(self, app_version: str):
        self.config_object_options = {}

        for variable in fields(Config):
            # Exclude these options since we handle them differently
            if variable.name not in [
                "app_version",
                "host_setup_available_hosts",
                "ssl",
                "hostgroup_hosts",
            ]:
                self.config_object_options[variable.name] = variable.type

        self.formatted_options = "\n\t".join(
            [
                f"({data_type.__name__}) {option}" if hasattr(data_type, "__name__") else f"(str) {option} []"
                for option, data_type in self.config_object_options.items()
                if option != "config_file"
            ]
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
                "Use a URI string for credentials (mysql/proxysql) - format: mysql://user:password@host:port "
                f"(port is optional with default {self.config.port}, or 6032 for ProxySQL)"
            ),
        )

        self.parser.add_argument(
            "--host-setup",
            dest="host_setup",
            action="store_true",
            help="Start Dolphie by showing the Host Setup modal instead of automatically connecting",
        )
        self.parser.add_argument("-u", "--user", dest="user", type=str, help="Username", metavar="")
        self.parser.add_argument("-p", "--password", dest="password", type=str, help="Password", metavar="")
        self.parser.add_argument(
            "-h",
            "--host",
            dest="host",
            type=str,
            help="Hostname/IP address",
            metavar="",
        )
        self.parser.add_argument(
            "-P",
            "--port",
            dest="port",
            type=int,
            help="Port (socket has precedence)",
            metavar="",
        )
        self.parser.add_argument(
            "-S",
            "--socket",
            dest="socket",
            type=str,
            help="Socket file",
            metavar="",
        )
        self.parser.add_argument(
            "-c",
            "--config-file",
            dest="config_file",
            type=str,
            help=(
                f"Dolphie's config file to use. Options are read from these files in the given order: "
                f"{self.config.config_file}"
            ),
            metavar="",
        )
        self.parser.add_argument(
            "-m",
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
            "-l",
            "--login-path",
            dest="login_path",
            type=str,
            help=(
                "Specify login path to use with mysql_config_editor's file ~/.mylogin.cnf for encrypted login "
                f"credentials [default: {self.config.login_path}]"
            ),
            metavar="",
        )
        self.parser.add_argument(
            "-r",
            "--refresh-interval",
            dest="refresh_interval",
            type=int,
            help=f"How much time to wait in seconds between each refresh [default: {self.config.refresh_interval}]",
            metavar="",
        )
        self.parser.add_argument(
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
            "--host-setup-file",
            dest="host_setup_file",
            type=str,
            help=(
                "Specify location of file that stores the available hosts to use in host setup modal [default: "
                f"{self.config.host_setup_file}]"
            ),
            metavar="",
        )
        self.parser.add_argument(
            "--heartbeat-table",
            dest="heartbeat_table",
            type=str,
            help=(
                "(MySQL only) If your hosts use pt-heartbeat, specify table in format db.table to use the timestamp it "
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
                "What panels to display on startup separated by a comma. Supports: "
                f"{','.join(self.panels.all())} [default: {self.config.startup_panels}]"
            ),
            metavar="",
        )
        self.parser.add_argument(
            "--graph-marker",
            dest="graph_marker",
            type=str,
            help=(
                "What marker to use for graphs (available options: https://tinyurl.com/dolphie-markers) [default: "
                f"{self.config.graph_marker}]"
            ),
            metavar="",
        )
        self.parser.add_argument(
            "--pypi-repo",
            dest="pypi_repository",
            type=str,
            help=(
                "What PyPi repository to use when checking for a new version "
                f"default: [{self.config.pypi_repository}]"
            ),
            metavar="",
        )
        self.parser.add_argument(
            "-H",
            "--hostgroup",
            dest="hostgroup",
            type=str,
            help=(
                "This is used for creating tabs and connecting to them for hosts you specify in "
                "Dolphie's config file under a hostgroup section. As an example, you'll have a section "
                "called [cluster1] then below it you will list each host on a new line in the format "
                "key=host (keys have no meaning). Hosts support optional port (default is whatever port parameter is) "
                "in the format host:port. You can also name the tabs by suffixing "
                "~tab_name to the host (i.e. 1=host~tab_name)"
            ),
            metavar="",
        )
        self.parser.add_argument(
            "-R",
            "--record",
            dest="record_for_replay",
            action="store_true",
            help=(
                "Enables recording of Dolphie's data to a replay file. "
                "Note: This can use significant disk space. Monitor accordingly!"
            ),
        )
        self.parser.add_argument(
            "-D",
            "--daemon",
            dest="daemon_mode",
            action="store_true",
            help=(
                "Starts Dolphie in daemon mode. This will not show the TUI and is designed be put into the "
                "background with whatever solution you decide to use. Automatically enables --record. "
                "This mode is solely used for recording data to a replay file"
            ),
        )
        self.parser.add_argument(
            "--daemon-log-file",
            dest="daemon_mode_log_file",
            type=str,
            help="Full path of the log file for daemon mode",
            metavar="",
        )
        self.parser.add_argument(
            "--replay-file",
            dest="replay_file",
            type=str,
            help="Specify the full path of the replay file to load and enable replay mode",
            metavar="",
        )
        self.parser.add_argument(
            "--replay-dir",
            dest="replay_dir",
            type=str,
            help="Directory to store replay data files",
            metavar="",
        )
        self.parser.add_argument(
            "--replay-retention-hours",
            dest="replay_retention_hours",
            type=int,
            help=(
                f"Number of hours to keep replay data. Data will be purged every hour "
                f"[default: {self.config.replay_retention_hours}]"
            ),
            metavar="",
        )
        self.parser.add_argument(
            "--exclude-notify-vars",
            dest="exclude_notify_global_vars",
            type=str,
            help=(
                "Dolphie will let you know when a global variable has been changed. If you have variables that change "
                "frequently and you don't want to see them, you can specify which ones with this option separated by "
                "a comma (i.e. --exclude-notify-vars=variable1,variable2)"
            ),
            metavar="",
        )
        self.parser.add_argument(
            "--show-trxs-only",
            dest="show_trxs_only",
            action="store_true",
            help="(MySQL only) Start with only showing threads that have an active transaction",
        )
        self.parser.add_argument(
            "--additional-columns",
            dest="show_additional_query_columns",
            action="store_true",
            help="Start with additional columns in Processlist panel",
        )
        self.parser.add_argument(
            "--debug-options",
            dest="debug_options",
            action="store_true",
            help=(
                "Display options that are set and what they're set by (command-line, dolphie config, etc) then exit. "
                "WARNING: This will show passwords and other sensitive information in plain text"
            ),
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

        for option in self.config_object_options.keys():
            if self.debug_options:
                self.debug_options_table.add_row("default", option, str(getattr(self.config, option)))

                # If last option, add a separator
                if option == list(self.config_object_options.keys())[-1]:
                    self.debug_options_table.add_row("", "", "")

        if options["config_file"]:
            self.config.config_file = [options["config_file"]]

        # Loop through config files to find the supplied options
        for config_file in self.config.config_file:
            if os.path.isfile(config_file):
                cfg = ConfigParser()
                cfg.read(config_file)

                # Loop through all of available options
                for option, data_type in self.config_object_options.items():
                    # If the option is in the config file
                    if cfg.has_option("dolphie", option):
                        # Check if the value is of the correct data type
                        value = self.verify_config_value(option, cfg.get("dolphie", option), data_type)

                        # If the option is not a login option, save it to the config object
                        if option not in login_options and value:
                            self.set_config_value("dolphie config", option, value)
                        else:
                            # Save the login option to be used later
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
                                f"{config_file}: Hostgroup [red2]{hostgroup}[/red2] has an empty host "
                                f"for key [red2]{key}[/red2]"
                            )

                    if not hosts:
                        self.exit(
                            f"{config_file}: Hostgroup [red2]{hostgroup}[/red2] cannot be loaded because "
                            f"it doesn't have any hosts listed under its section in Dolphie's config"
                        )

                    hostgroups[hostgroup] = hosts

        # Save the hostgroups found to the config object
        self.config.hostgroup_hosts = hostgroups

        # We need to loop through all options and set non-login options so we can use them for the logic below
        for option in self.config_object_options.keys():
            if option not in login_options and options[option]:
                self.set_config_value("command-line", option, options[option])

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

        # Loop through all login options and set them in order of precedence
        for option in login_options:
            # Update config object with Dolphie config
            dolphie_value = dolphie_config_login_options_used.get(option)
            if dolphie_value:
                self.set_config_value("dolphie config", option, dolphie_value)

            # Use environment variables if specified
            environment_var = f"DOLPHIE_{option.upper()}"
            env_value = os.environ.get(environment_var)
            if env_value:
                self.set_config_value("env variable", option, env_value)

            # Use command-line arguments if specified
            if options[option]:
                self.set_config_value("command-line", option, options[option])

        # Lastly, parse URI if specified
        if options["uri"]:
            try:
                parsed_result: ParseResult = urlparse(options["uri"])

                if parsed_result.scheme == "mysql":
                    port = parsed_result.port or 3306
                elif parsed_result.scheme == "proxysql":
                    port = parsed_result.port or 6032
                else:
                    self.exit(
                        "Invalid URI scheme: Only 'mysql' or 'proxysql' are supported (see --help for more information)"
                    )

                self.set_config_value("uri", "user", parsed_result.username)
                self.set_config_value("uri", "password", parsed_result.password)
                self.set_config_value("uri", "host", parsed_result.hostname)
                self.set_config_value("uri", "port", port)
            except Exception as e:
                self.exit(f"Invalid URI: {e} (see --help for more information)")

        # Sanity check for hostgroup
        if self.config.hostgroup:
            if self.config.hostgroup not in hostgroups:
                self.exit(
                    f"Hostgroup [red2]{self.config.hostgroup}[/red2] cannot be used because "
                    f"it wasn't found in Dolphie's config"
                )

        if self.config.heartbeat_table:
            pattern_match = re.search(r"^(\w+\.\w+)$", self.config.heartbeat_table)
            if pattern_match:
                MySQLQueries.heartbeat_replica_lag = MySQLQueries.heartbeat_replica_lag.replace(
                    "$1", self.config.heartbeat_table
                )
            else:
                self.exit("Your heartbeat table did not conform to the proper format: db.table")

        if self.config.exclude_notify_global_vars:
            self.config.exclude_notify_global_vars = self.config.exclude_notify_global_vars.split(",")

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

        # Verify parameters for replay & daemon mode
        if self.config.daemon_mode:
            self.config.record_for_replay = True
            if not self.config.replay_dir:
                self.exit("Daemon mode ([red2]--daemon[/red2]) requires [red2]--replay-dir[/red2] to be specified")

        if self.config.replay_file and not os.path.isfile(self.config.replay_file):
            self.exit(f"Replay file [red2]{self.config.replay_file}[/red2] does not exist")

        if self.config.record_for_replay and not self.config.replay_dir:
            self.exit("[red2]--record[/red2] requires [red2]--replay-dir[/red2] to be specified")

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
