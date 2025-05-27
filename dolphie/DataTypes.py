import hashlib
from dataclasses import dataclass, field
from typing import Dict, List, Union

import pymysql
from rich.table import Table

from dolphie.Modules.Functions import format_query, format_time


@dataclass
class ConnectionSource:
    mysql = "MySQL"
    proxysql = "ProxySQL"
    mariadb = "MariaDB"


@dataclass
class ConnectionStatus:
    connecting = "CONNECTING"
    connected = "CONNECTED"
    disconnected = "DISCONNECTED"
    read_write = "R/W"
    read_only = "RO"


@dataclass
class Replica:
    row_key: str
    thread_id: int
    host: str
    port: int = None
    host_distro: str = None
    connection: pymysql.Connection = None
    connection_source_alt: ConnectionSource = None
    table: Table = None
    replication_status: Dict[str, Union[str, int]] = field(default_factory=dict)
    mysql_version: str = None


class ReplicaManager:
    def __init__(self):
        self.available_replicas: List[Dict[str, str]] = []
        self.replicas: Dict[str, Replica] = {}
        self.ports: Dict[str, Dict[str, Union[str, bool]]] = {}

    # This is mainly for MariaDB since it doesn't have a way to map a replica in processlist to a specific port
    # Instead of using the thread_id as key, we use the host and port to create a unique row key
    # for the replica sections
    def create_replica_row_key(self, host: str, port: int) -> str:
        input_string = f"{host}:{port}"
        return hashlib.sha256(input_string.encode()).hexdigest()

    def add_replica(self, row_key: str, thread_id: int, host: str, port: int) -> Replica:
        self.replicas[row_key] = Replica(row_key=row_key, thread_id=thread_id, host=host, port=port)

        return self.replicas[row_key]

    def remove_replica(self, row_key: str):
        del self.replicas[row_key]

    def get_replica(self, row_key: str) -> Replica:
        return self.replicas.get(row_key)

    def remove_all_replicas(self):
        if self.replicas:
            for replica in self.replicas.values():
                if replica.connection:
                    replica.connection.close()

            self.replicas = {}

    def get_sorted_replicas(self) -> List[Replica]:
        return sorted(self.replicas.values(), key=lambda x: x.host)


@dataclass
class Panel:
    name: str
    display_name: str
    key: str = None
    visible: bool = False
    daemon_supported: bool = True


class Panels:
    def __init__(self):
        self.dashboard = Panel("dashboard", "Dashboard", "¹", daemon_supported=False)
        self.processlist = Panel("processlist", "Processlist", "²")
        self.graphs = Panel("graphs", "Metric Graphs", "³", daemon_supported=False)
        self.replication = Panel("replication", "Replication", "⁴", daemon_supported=False)
        self.metadata_locks = Panel("metadata_locks", "Metadata Locks", "⁵")
        self.ddl = Panel("ddl", "DDL", "⁶", daemon_supported=False)
        self.pfs_metrics = Panel("pfs_metrics", "Performance Schema Metrics", "⁷")
        self.statements_summary = Panel("statements_summary", "Statements Summary", "⁸")
        self.proxysql_hostgroup_summary = Panel("proxysql_hostgroup_summary", "Hostgroup Summary", "⁴")
        self.proxysql_mysql_query_rules = Panel(
            "proxysql_mysql_query_rules", "Query Rules", "⁵", daemon_supported=False
        )
        self.proxysql_command_stats = Panel("proxysql_command_stats", "Command Stats", "⁶", daemon_supported=False)

    def validate_panels(self, panel_list_str: Union[str, List[str]], valid_panel_names: List[str]) -> List[str]:
        panels = panel_list_str.split(",") if isinstance(panel_list_str, str) else panel_list_str

        invalid_panels = [panel for panel in panels if panel not in valid_panel_names]
        if invalid_panels:
            raise ValueError(
                f"Panel(s) [red2]{', '.join(invalid_panels)}[/red2] are not valid (see --help for more information)"
            )

        return panels

    def get_panel(self, panel_name: str) -> Panel:
        return self.__dict__.get(panel_name, None)

    def get_all_daemon_panel_names(self) -> List[str]:
        return [panel.name for panel in self.__dict__.values() if isinstance(panel, Panel) and panel.daemon_supported]

    def get_all_panels(self) -> List[Panel]:
        return [panel for panel in self.__dict__.values() if isinstance(panel, Panel)]

    def get_key(self, panel_name: str) -> str:
        # This uses Rich's syntax for highlighting, not Textual's Content system
        return f"[b highlight]{self.get_panel(panel_name).key}[/b highlight]"

    def get_panel_title(self, panel_name: str) -> str:
        panel = self.get_panel(panel_name)
        return f"[$b_highlight]{panel.key}[/$b_highlight]{panel.display_name}"

    def all(self) -> List[str]:
        return [
            panel.name
            for name, panel in self.__dict__.items()
            if not name.startswith("__") and isinstance(panel, Panel)
        ]


class ProcesslistThread:
    def __init__(self, thread_data: Dict[str, str]):
        self.thread_data = thread_data

        self.id = str(thread_data.get("id", ""))
        self.mysql_thread_id = thread_data.get("mysql_thread_id")
        self.user = thread_data.get("user", "")
        self.host = thread_data.get("host", "")
        self.db = thread_data.get("db", "")
        self.time = int(thread_data.get("time", 0))
        self.protocol = self._get_formatted_string(thread_data.get("connection_type", ""))
        self.formatted_query = self._get_formatted_query(thread_data.get("query", ""))
        self.formatted_time = self._get_formatted_time()
        self.command = self._get_formatted_command(thread_data.get("command", ""))
        self.state = self._get_formatted_string(thread_data.get("state", ""))
        self.trx_state = self._get_formatted_string(thread_data.get("trx_state", ""))
        self.trx_operation_state = self._get_formatted_string(thread_data.get("trx_operation_state", ""))
        self.trx_rows_locked = self._get_formatted_number(thread_data.get("trx_rows_locked", 0))
        self.trx_rows_modified = self._get_formatted_number(thread_data.get("trx_rows_modified", 0))
        self.trx_concurrency_tickets = self._get_formatted_number(thread_data.get("trx_concurrency_tickets", 0))
        self.trx_time = self._get_formatted_trx_time(thread_data.get("trx_time", ""))

    def _get_formatted_time(self) -> str:
        thread_color = self._get_time_color()
        return f"[{thread_color}]{format_time(self.time)}[/{thread_color}]" if thread_color else format_time(self.time)

    def _get_time_color(self) -> str:
        thread_color = ""
        if "Group replication" not in self.formatted_query.code:  # Don't color GR threads
            if "SELECT /*!40001 SQL_NO_CACHE */ *" in self.formatted_query.code:
                thread_color = "purple"
            elif self.formatted_query.code:
                if self.time >= 10:
                    thread_color = "red"
                elif self.time >= 5:
                    thread_color = "yellow"
                else:
                    thread_color = "green"
        return thread_color

    def _get_formatted_command(self, command: str):
        return "[red]Killed[/red]" if command == "Killed" else command

    def _get_formatted_trx_time(self, trx_time: str):
        return format_time(int(trx_time)) if trx_time else "[dark_gray]N/A"

    def _get_formatted_query(self, query: str):
        return format_query(query)

    def _get_formatted_string(self, string: str):
        if not string:
            return "[dark_gray]N/A"

        return string

    def _get_formatted_number(self, number):
        if not number or number == "0":
            return "[dark_gray]0"

        return number


class ProxySQLProcesslistThread:
    def __init__(self, thread_data: Dict[str, str]):
        self.thread_data = thread_data

        self.id = str(thread_data.get("id", ""))
        self.hostgroup = int(thread_data.get("hostgroup"))
        self.user = thread_data.get("user", "")
        self.frontend_host = self._get_formatted_string(thread_data.get("frontend_host", ""))
        self.host = self._get_formatted_string(thread_data.get("backend_host", ""))
        self.db = thread_data.get("db", "")
        self.time = int(thread_data.get("time", 0)) / 1000  # Convert to seconds since ProxySQL returns milliseconds
        self.formatted_query = self._get_formatted_query(thread_data.get("query", "").strip(" \t\n\r"))
        self.formatted_time = self._get_formatted_time()
        self.command = self._get_formatted_command(thread_data.get("command", ""))
        self.extended_info = thread_data.get("extended_info", "")

    def _get_formatted_time(self) -> str:
        thread_color = self._get_time_color()
        return f"[{thread_color}]{format_time(self.time)}[/{thread_color}]" if thread_color else format_time(self.time)

    def _get_time_color(self) -> str:
        thread_color = ""
        if self.formatted_query.code:
            if self.time >= 10:
                thread_color = "red"
            elif self.time >= 5:
                thread_color = "yellow"
            else:
                thread_color = "green"
        return thread_color

    def _get_formatted_command(self, command: str):
        return "[red]Killed[/red]" if command == "Killed" else command

    def _get_formatted_trx_time(self, trx_time: str):
        return format_time(int(trx_time)) if trx_time else "[dark_gray]N/A"

    def _get_formatted_query(self, query: str):
        return format_query(query)

    def _get_formatted_string(self, string: str):
        if not string:
            return "[dark_gray]N/A"

        return string

    def _get_formatted_number(self, number):
        if not number or number == "0":
            return "[dark_gray]0"

        return number


class HotkeyCommands:
    show_thread = "show_thread"
    thread_filter = "thread_filter"
    thread_kill_by_parameter = "thread_kill_by_parameter"
    variable_search = "variable_search"
    rename_tab = "rename_tab"
    refresh_interval = "refresh_interval"
    replay_seek = "replay_seek"
    maximize_panel = "maximize_panel"
