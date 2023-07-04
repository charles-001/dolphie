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
from time import sleep

import myloginpath
from dolphie import Dolphie
from dolphie.Panels import (
    dashboard_panel,
    innodb_io_panel,
    innodb_locks_panel,
    processlist_panel,
    replica_panel,
)
from dolphie.Queries import Queries
from rich.live import Live
from rich.prompt import Prompt


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
        "-R",
        "--refresh_interval_innodb_status",
        dest="refresh_interval_innodb_status",
        default=1,
        type=int,
        help=(
            "How much time to wait in seconds to execute SHOW ENGINE INNODB STATUS to refresh data its responsible "
            "for [default: %(default)s]"
        ),
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
        dest="dashboard",
        action="store_false",
        default=True,
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
        "--debug",
        action="store_true",
        default=False,
        help="Print tracebacks on errors for more verbose debugging",
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

    if parameter_options["debug"]:
        dolphie.debug = parameter_options["debug"]

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
                raise Exception("Unsupported SSL mode [b]%s" % ssl_mode)

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
                raise Exception(f"Problem reading login path file Reason: {e}")

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

    if parameter_options["refresh_interval_innodb_status"]:
        dolphie.refresh_interval_innodb_status = parameter_options["refresh_interval_innodb_status"]

    if parameter_options["heartbeat_table"]:
        pattern_match = re.search(r"^(\w+\.\w+)$", parameter_options["heartbeat_table"])
        if pattern_match:
            dolphie.heartbeat_table = parameter_options["heartbeat_table"]
            Queries["heartbeat_replica_lag"] = Queries["heartbeat_replica_lag"].replace(
                "$placeholder", dolphie.heartbeat_table
            )
        else:
            raise Exception("Your heartbeat table did not conform to the proper format db.table")

    if parameter_options["ssl_mode"]:
        ssl_mode = parameter_options["ssl_mode"].upper()

        if ssl_mode == "REQUIRED":
            dolphie.ssl[""] = True
        elif ssl_mode == "VERIFY_CA":
            dolphie.ssl["check_hostame"] = False
        elif ssl_mode == "VERIFY_IDENTITY":
            dolphie.ssl["check_hostame"] = True
        else:
            raise Exception(f"Unsupported SSL mode {ssl_mode}")

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

    dolphie.dashboard = parameter_options["dashboard"]
    dolphie.show_trxs_only = parameter_options["show_trxs_only"]
    dolphie.show_additional_query_columns = parameter_options["show_additional_query_columns"]
    dolphie.use_processlist = parameter_options["use_processlist"]

    if dolphie.dashboard:
        dolphie.layout["dashboard"].visible = True
    else:
        dolphie.layout["dashboard"].visible = False


def main():
    dolphie = Dolphie()

    try:
        dolphie.create_rich_layout()
        parse_args(dolphie)
        dolphie.check_for_update()
        dolphie.db_connect()
        dolphie.load_host_cache_file()

        with Live(
            dolphie.layout, vertical_overflow="crop", screen=True, transient=True, auto_refresh=False
        ) as dolphie.rich_live:
            while True:
                if dolphie.pause_refresh is False:
                    loop_time = datetime.now()

                    dolphie.statuses = dolphie.fetch_data("status")
                    if dolphie.first_loop:
                        dolphie.saved_status = dolphie.statuses.copy()

                    dolphie.loop_duration_seconds = (loop_time - dolphie.previous_main_loop_time).total_seconds()
                    loop_duration_innodb_status_seconds = (
                        loop_time - dolphie.previous_innodb_status_loop_time
                    ).total_seconds()

                    if dolphie.layout["processlist"].visible:
                        dolphie.processlist_threads = processlist_panel.get_data(dolphie)
                        dolphie.layout["processlist"].update(processlist_panel.create_panel(dolphie))

                    if dolphie.dashboard:
                        dolphie.variables = dolphie.fetch_data("variables")
                        dolphie.primary_status = dolphie.fetch_data("primary_status")
                        dolphie.replica_status = dolphie.fetch_data("replica_status")

                        if (
                            dolphie.first_loop
                            or loop_duration_innodb_status_seconds >= dolphie.refresh_interval_innodb_status
                        ):
                            dolphie.innodb_status = dolphie.fetch_data("innodb_status")

                        dolphie.layout["dashboard"].update(dashboard_panel.create_panel(dolphie))

                        # Save some variables to be used in next refresh
                        dolphie.previous_binlog_position = 0
                        if dolphie.primary_status:
                            dolphie.previous_binlog_position = dolphie.primary_status["Position"]

                    if dolphie.layout["replicas"].visible:
                        dolphie.layout["replicas"].update(replica_panel.create_panel(dolphie))

                    if dolphie.layout["innodb_io"].visible:
                        dolphie.layout["innodb_io"].update(innodb_io_panel.create_panel(dolphie))

                    if dolphie.layout["innodb_locks"].visible:
                        dolphie.layout["innodb_locks"].update(innodb_locks_panel.create_panel(dolphie))

                    if dolphie.replica_status:
                        dolphie.previous_replica_sbm = 0
                        if dolphie.replica_status["Seconds_Behind_Master"] is not None:
                            dolphie.previous_replica_sbm = dolphie.replica_status["Seconds_Behind_Master"]

                    # This is for the many stats per second in Dolphie
                    dolphie.saved_status = dolphie.statuses.copy()
                    dolphie.previous_main_loop_time = loop_time

                    if loop_duration_innodb_status_seconds >= dolphie.refresh_interval_innodb_status:
                        dolphie.previous_innodb_status_loop_time = loop_time

                    dolphie.rich_live.update(dolphie.layout, refresh=True)
                else:
                    # To prevent main loop from eating up 100% CPU
                    sleep(0.01)

                # Detect a keypress loop
                loop_counter = 0
                while loop_counter <= dolphie.refresh_interval * 10:
                    if dolphie.kb.key_press():
                        if dolphie.pause_refresh is False:
                            key = dolphie.kb.getch()

                        if key:
                            valid_key = dolphie.capture_key(key)
                            if valid_key:
                                break

                    # refresh_interval * 10 * 100ms equals to the second of sleep a user wants. This allows us to
                    # capture a key faster than sleeping the whole second of refresh_interval
                    sleep(0.100)

                    loop_counter += 1

                dolphie.first_loop = False
    except Exception as e:
        if dolphie.debug:
            dolphie.console.print_exception()
        else:
            dolphie.console.print(f"[b][red]ERROR[/red]![/b] {str(e)}", highlight=False)


if __name__ == "__main__":
    main()
