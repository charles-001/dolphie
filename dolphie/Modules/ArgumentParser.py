import argparse
import json
import os
import re
import sys
from configparser import RawConfigParser
from dataclasses import dataclass, field, fields
from typing import Dict, List
from urllib.parse import ParseResult, urlparse

import myloginpath
from rich import box
from rich.console import Console
from rich.table import Table
from rich.theme import Theme

from dolphie.DataTypes import Panels
from dolphie.Modules.Queries import MySQLQueries


@dataclass
class CredentialProfile:
    name: str
    user: str = None
    password: str = None
    socket: str = None
    ssl: Dict = field(default_factory=dict)
    ssl_mode: str = None
    ssl_ca: str = None
    ssl_cert: str = None
    ssl_key: str = None


@dataclass
class HostGroupMember:
    tab_title: str
    host: str
    port: int = None
    credential_profile: CredentialProfile = None


@dataclass
class Config:
    app_version: str
    tab_setup: bool = False
    credential_profile: str = None
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
    tab_setup_file: str = field(default_factory=lambda: f"{os.path.expanduser('~')}/dolphie_hosts")
    refresh_interval: int = 1
    heartbeat_table: str = None
    credential_profiles: Dict[str, CredentialProfile] = field(default_factory=dict)
    tab_setup_available_hosts: List[str] = field(default_factory=list)
    startup_panels: str = "dashboard,processlist"
    graph_marker: str = "braille"
    pypi_repository: str = "https://pypi.org/pypi/dolphie/json"
    hostgroup: str = None
    hostgroup_hosts: Dict[str, List[HostGroupMember]] = field(default_factory=dict)
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
                "tab_setup_available_hosts",
                "ssl",
                "hostgroup_hosts",
                "credential_profiles",
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
\t2. Credential profile (set by --cred-profile)
\t3. Environment variables
\t4. Dolphie's config (set by --config-file)
\t5. ~/.mylogin.cnf (mysql_config_editor)
\t6. ~/.my.cnf (set by --mycnf-file)

Credential profiles can be defined in Dolphie's config file as a way to store credentials for easy access.
A profile can be created by adding a section in the config file with the format: [credential_profile_<name>]
When using a credential profile, do not include the prefix 'credential_profile' (i.e. -C production)
The following options are supported in credential profiles:
\tuser
\tpassword
\tsocket
\tmycnf_file
\tlogin_path

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
\tDOLPHIE_SSL_MODE REQUIRED/VERIFY_CA/VERIFY_IDENTITY
\tDOLPHIE_SSL_CA
\tDOLPHIE_SSL_CERT
\tDOLPHIE_SSL_KEY

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
            "--tab-setup",
            dest="tab_setup",
            action="store_true",
            help=(
                "Start Dolphie by showing the Tab Setup modal instead of automatically connecting "
                "with the specified options"
            ),
        )
        self.parser.add_argument(
            "-C",
            "--cred-profile",
            dest="credential_profile",
            type=str,
            help="Credential profile to use. See below for more information",
            metavar="",
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
            "--tab-setup-file",
            dest="tab_setup_file",
            type=str,
            help=(
                "Specify location of file that stores the available hosts to use in Tab Setup modal [default: "
                f"{self.config.tab_setup_file}]"
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
        self.add_to_debug_options(source, option, value)

    def add_to_debug_options(self, source, option, value):
        if self.debug_options:
            self.debug_options_table.add_row(source, option, str(value))

    def _parse(self):
        login_options = ["user", "password", "host", "port", "socket", "ssl_mode", "ssl_ca", "ssl_cert", "ssl_key"]

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
                cfg = RawConfigParser()
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

                # First, loop sections for credential profiles so hostgroups can reference them
                # when the profiles are not before hostgroup declarations in Dolphie's config file
                for section in cfg.sections():
                    if section.startswith("credential_profile"):
                        self.parse_credential_profile(cfg, section)

                # Then, loop sections for hostgroups
                for section in cfg.sections():
                    if section == "dolphie" or section.startswith("credential_profile"):
                        continue

                    # Treat anything else as a hostgroup
                    hosts = self.parse_hostgroup(cfg, section, config_file)
                    if hosts:
                        hostgroups[section] = hosts

        # Save the hostgroups found to the config object
        self.config.hostgroup_hosts = hostgroups

        # We need to loop through all options and set non-login options so we can use them for the logic below
        for option in self.config_object_options.keys():
            if option not in login_options and options[option]:
                self.set_config_value("command-line", option, options[option])

        if self.config.credential_profile and self.config.credential_profile not in self.config.credential_profiles:
            self.exit(
                f"Credential profile [red2]{self.config.credential_profile}[/red2] does not exist in "
                "Dolphie's config file"
            )

        # Use MySQL's my.cnf file for login options if specified
        if os.path.isfile(self.config.mycnf_file):
            cfg = RawConfigParser()
            cfg.read(self.config.mycnf_file)

            for option in login_options:
                if cfg.has_option("client", option):
                    self.set_config_value("my.cnf", option, cfg.get("client", option))

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

            # Use credential profile if specified
            if self.config.credential_profile:
                profile = self.config.credential_profiles.get(self.config.credential_profile)
                if profile and hasattr(profile, option) and getattr(profile, option):
                    self.set_config_value(f"cred profile {profile.name}", option, getattr(profile, option))

            # Use command-line arguments if specified
            if options.get(option):
                self.set_config_value("command-line", option, options[option])

        # Create SSL object from config after all login options are set
        ssl_payload = {
            opt.name: getattr(self.config, opt.name)
            for opt in fields(Config)
            if opt.name.startswith("ssl_") and getattr(self.config, opt.name)
        }
        if ssl_payload:
            self.set_config_value("ssl object", "ssl", self.create_ssl_object(ssl_payload))

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

        if self.config.hostgroup:
            # Sanity check for hostgroup
            if self.config.hostgroup not in hostgroups:
                self.exit(f"Hostgroup [red2]{self.config.hostgroup}[/red2] does not exist in Dolphie's config file")

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

        self.config.startup_panels = self.config.startup_panels.split(",")
        for panel in self.config.startup_panels:
            if panel not in self.panels.all():
                self.exit(f"Panel [red2]{panel}[/red2] is not valid (see --help for more information)")

        if os.path.exists(self.config.tab_setup_file):
            with open(self.config.tab_setup_file, "r") as file:
                self.config.tab_setup_available_hosts = [line.strip() for line in file]

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

        # Set replay directory if replay file is specified
        if self.config.replay_file and not self.config.replay_dir:
            self.config.replay_dir = os.path.dirname(os.path.dirname(self.config.replay_file))

    def parse_hostgroup(self, cfg, section, config_file) -> List[HostGroupMember]:
        hosts = []
        for key in cfg.options(section):
            host_json = cfg.get(section, key).strip()

            # Validate that the host's JSON is valid
            try:
                host_data = json.loads(host_json)
                if not host_data:
                    self.exit(f"{config_file}: Hostgroup [red2]{section}[/red2] has no data for key [red2]{key}[/red2]")
            except json.JSONDecodeError:
                self.exit(
                    f"{config_file}: Invalid JSON value for hostgroup [red2]{section}[/red2], key [red2]{key}[/red2]"
                )

            host = host_data.get("host")
            port = self.config.port
            if ":" in host:
                host, port = host.split(":")

            tab_title = host_data.get("tab_title")
            credential_profile = host_data.get("credential_profile")
            if credential_profile and credential_profile not in self.config.credential_profiles:
                self.exit(
                    f"{config_file}: Credential profile [red2]{credential_profile}[/red2] "
                    f"not found for hostgroup [red2]{section}[/red2], key [red2]{key}[/red2]"
                )

            hosts.append(
                HostGroupMember(tab_title=tab_title, host=host, port=port, credential_profile=credential_profile)
            )

        if not hosts:
            self.exit(
                f"{config_file}: Hostgroup [red2]{section}[/red2] cannot be loaded because "
                f"it doesn't have any hosts listed under its section in Dolphie's config"
            )

        return hosts

    def parse_credential_profile(self, cfg: RawConfigParser, section: str):
        # Options that can be set directly
        credential_profile_options = ["user", "password", "socket", "ssl_mode", "ssl_ca", "ssl_cert", "ssl_key"]

        # All options. mycnf_file and login_path are processed instead of directly set
        supported_options = credential_profile_options + ["mycnf_file", "login_path"]

        credential_name = section.split("credential_profile_")[1]
        credential = CredentialProfile(name=credential_name)

        for key in cfg.options(section):
            value = cfg.get(section, key).strip()

            if key in credential_profile_options:
                setattr(credential, key, value)
                self.add_to_debug_options(f"cred profile setup - {credential_name}", key, value)
            elif key == "mycnf_file":
                if not os.path.isfile(value):
                    self.exit(
                        f"mycnf file [red2]{value}[/red2] for credential profile [red2]{section}[/red2] does not exist"
                    )

                # Parse client options from a my.cnf file
                mycnf = RawConfigParser()
                mycnf.read(value)
                if mycnf.has_section("client"):
                    for option in credential_profile_options:
                        if mycnf.has_option("client", option):
                            option_value = mycnf.get("client", option)

                            setattr(credential, option, option_value)
                            self.add_to_debug_options(
                                f"cred profile setup - {credential_name}", f"{key}/{option}", option_value
                            )
                else:
                    self.exit(
                        f"mycnf file [red2]{value}[/red2] for credential profile [red2]{section}[/red2] "
                        "does not have a client section"
                    )
            elif key == "login_path":
                # Parse login path options from a login path via mysql_config_editor
                try:
                    login_path_data = myloginpath.parse(value)
                    for option in credential_profile_options:
                        if option in login_path_data:
                            option_value = login_path_data[option]

                            setattr(credential, option, option_value)
                            self.add_to_debug_options(
                                f"cred profile setup - {credential_name}", f"{key}/{option}", option_value
                            )
                except Exception as e:
                    self.exit(f"Error reading login path file for credential profile [red2]{section}[/red2]: {e}")
            else:
                self.exit(
                    f"Invalid option [red2]{key}[/red2] found in credential profile [red2]{section}[/red2]. "
                    f"Supported options are: {', '.join(supported_options)}"
                )

        # If section has no options listed
        if not any(getattr(credential, option) for option in credential_profile_options):
            self.exit(
                (
                    f"Credential profile [red2]{credential_name}[/red2] has no valid options set. "
                    f"Supported options are: {', '.join(supported_options)}"
                )
            )

        credential.ssl = self.create_ssl_object(credential.__dict__)
        if credential.ssl:
            self.add_to_debug_options(f"cred profile setup - {credential_name}", "ssl object", credential.ssl)

        self.config.credential_profiles[credential_name] = credential

    def create_ssl_object(self, data: Dict) -> Dict:
        ssl_payload = {}

        ssl_mode = data.get("ssl_mode")
        ssl_ca = data.get("ssl_ca")
        ssl_cert = data.get("ssl_cert")
        ssl_key = data.get("ssl_key")

        if ssl_mode:
            ssl_mode = ssl_mode.upper()

            if ssl_mode == "REQUIRED":
                ssl_payload["required"] = True
            elif ssl_mode == "VERIFY_CA":
                if not ssl_ca:
                    self.exit("SSL mode [red2]VERIFY_CA[/red2] requires a CA file (--ssl-ca) to be specified")

                ssl_payload["check_hostname"] = False
                ssl_payload["verify_mode"] = True
            elif ssl_mode == "VERIFY_IDENTITY":
                if not ssl_ca:
                    self.exit("SSL mode [red2]VERIFY_IDENTITY[/red2] requires a CA file (--ssl-ca) to be specified")

                ssl_payload["check_hostname"] = True
                ssl_payload["verify_mode"] = True
            else:
                self.exit(f"Unsupported SSL mode [red2]{ssl_mode}[/red2]")

            if ssl_ca:
                ssl_payload["ca"] = ssl_ca
            if ssl_cert:
                ssl_payload["cert"] = ssl_cert
            if ssl_key:
                ssl_payload["key"] = ssl_key

        return ssl_payload

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
