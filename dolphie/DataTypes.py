from __future__ import annotations

from dataclasses import dataclass, field

import pymysql
from dolphie.Modules.Functions import format_query, format_time
from rich.table import Table


class ConnectionSource:
    mysql = "MySQL"
    proxysql = "ProxySQL"
    mariadb = "MariaDB"


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
    port: int | None = None
    host_distro: str | None = None
    connection: pymysql.Connection | None = None
    connection_source_alt: ConnectionSource | None = None
    table: Table | None = None
    replication_status: dict[str, str | int] = field(default_factory=dict)
    mysql_version: str | None = None


class ReplicaManager:
    def __init__(self):
        self.available_replicas: list[dict[str, str]] = []
        self.replicas: dict[str, Replica] = {}
        self.ports: dict[str, dict[str, str | int | bool]] = {}

    # Dots/colons are invalid in Textual widget IDs - translate to hyphens in one pass
    _widget_id_sanitize = str.maketrans({".": "-", ":": "-"})

    @classmethod
    def create_replica_row_key(cls, host: str, port: int) -> str:
        return f"{host}-{port}".translate(cls._widget_id_sanitize)

    def add_replica(self, row_key: str, thread_id: int, host: str, port: int) -> Replica:
        self.replicas[row_key] = Replica(row_key=row_key, thread_id=thread_id, host=host, port=port)
        return self.replicas[row_key]

    def remove_replica(self, row_key: str):
        replica = self.replicas.pop(row_key, None)
        if replica and replica.connection:
            replica.connection.close()

    def get_replica(self, row_key: str) -> Replica | None:
        return self.replicas.get(row_key)

    def remove_all_replicas(self):
        for replica in self.replicas.values():
            if replica.connection:
                replica.connection.close()
        self.replicas = {}

    def get_sorted_replicas(self) -> list[Replica]:
        return sorted(self.replicas.values(), key=lambda x: x.host)


@dataclass
class Panel:
    name: str
    display_name: str
    key: str | None = None
    visible: bool = False
    daemon_supported: bool = True

    @property
    def formatted_key(self) -> str:
        # This uses Rich's syntax for highlighting, not Textual's Content system
        return f"[b highlight]{self.key}[/b highlight]"

    @property
    def title(self) -> str:
        return f"[$b_highlight]{self.key}[/$b_highlight]{self.display_name}"


class Panels:
    def __init__(self):
        self._registry: dict[str, Panel] = {}

        self.dashboard = self._add("dashboard", "Dashboard", "¹", daemon_supported=False)
        self.processlist = self._add("processlist", "Processlist", "²")
        self.graphs = self._add("graphs", "Metric Graphs", "³", daemon_supported=False)
        self.replication = self._add("replication", "Replication", "⁴", daemon_supported=False)
        self.metadata_locks = self._add("metadata_locks", "Metadata Locks", "⁵")
        self.ddl = self._add("ddl", "DDL", "⁶", daemon_supported=False)
        self.pfs_metrics = self._add("pfs_metrics", "Performance Schema Metrics", "⁷")
        self.statements_summary = self._add("statements_summary", "Statements Summary", "⁸")
        self.proxysql_hostgroup_summary = self._add("proxysql_hostgroup_summary", "Hostgroup Summary", "⁴")
        self.proxysql_mysql_query_rules = self._add(
            "proxysql_mysql_query_rules", "Query Rules", "⁵", daemon_supported=False
        )
        self.proxysql_command_stats = self._add("proxysql_command_stats", "Command Stats", "⁶", daemon_supported=False)

    def _add(self, name: str, display_name: str, key: str | None = None, daemon_supported: bool = True) -> Panel:
        panel = Panel(name=name, display_name=display_name, key=key, daemon_supported=daemon_supported)
        self._registry[name] = panel
        return panel

    def validate_panels(self, panel_list_str: str | list[str], valid_panel_names: list[str]) -> list[str]:
        panels = panel_list_str.split(",") if isinstance(panel_list_str, str) else panel_list_str

        invalid_panels = [panel for panel in panels if panel not in valid_panel_names]
        if invalid_panels:
            raise ValueError(
                f"Panel(s) [red2]{', '.join(invalid_panels)}[/red2] are not valid (see --help for more information)"
            )

        return panels

    def get_all_daemon_panel_names(self) -> list[str]:
        return [panel.name for panel in self._registry.values() if panel.daemon_supported]

    def get_all_panels(self) -> list[Panel]:
        return list(self._registry.values())

    def all(self) -> list[str]:
        return list(self._registry.keys())


class BaseProcesslistThread:
    def __init__(self, thread_data: dict[str, str]):
        self.thread_data = thread_data
        self.id = str(thread_data.get("id", ""))
        self.user = thread_data.get("user", "")
        self.db = thread_data.get("db", "")

    @staticmethod
    def _get_time_color(time: float, query_code: str) -> str:
        if not query_code:
            return ""
        if time >= 10:
            return "red"
        elif time >= 5:
            return "yellow"
        return "green"

    @staticmethod
    def _format_time_with_color(time: float, color: str) -> str:
        return f"[{color}]{format_time(time)}[/{color}]" if color else format_time(time)

    @staticmethod
    def _format_command(command: str) -> str:
        return "[red]Killed[/red]" if command == "Killed" else command

    @staticmethod
    def _format_string(string: str) -> str:
        return string if string else "[dark_gray]N/A"

    @staticmethod
    def _format_number(number) -> str:
        return "[dark_gray]0" if not number or number == "0" else number


class ProcesslistThread(BaseProcesslistThread):
    def __init__(self, thread_data: dict[str, str]):
        super().__init__(thread_data)

        self.mysql_thread_id = thread_data.get("mysql_thread_id")
        self.host = thread_data.get("host", "")
        self.time = int(thread_data.get("time", 0))
        self.protocol = self._format_string(thread_data.get("connection_type", ""))
        self.formatted_query = format_query(thread_data.get("query", ""))
        self.formatted_time = self._format_time_with_color(self.time, self._mysql_time_color())
        self.command = self._format_command(thread_data.get("command", ""))
        self.state = self._format_string(thread_data.get("state", ""))
        self.trx_state = self._format_string(thread_data.get("trx_state", ""))
        self.trx_operation_state = self._format_string(thread_data.get("trx_operation_state", ""))
        self.trx_rows_locked = self._format_number(thread_data.get("trx_rows_locked", 0))
        self.trx_rows_modified = self._format_number(thread_data.get("trx_rows_modified", 0))
        self.trx_concurrency_tickets = self._format_number(thread_data.get("trx_concurrency_tickets", 0))
        trx_time = thread_data.get("trx_time", "")
        self.trx_time = format_time(int(trx_time)) if trx_time else "[dark_gray]N/A"

    def _mysql_time_color(self) -> str:
        if "Group replication" in self.formatted_query.code:  # Don't color GR threads
            return ""
        if "SELECT /*!40001 SQL_NO_CACHE */ *" in self.formatted_query.code:
            return "purple"
        return self._get_time_color(self.time, self.formatted_query.code)


class ProxySQLProcesslistThread(BaseProcesslistThread):
    def __init__(self, thread_data: dict[str, str]):
        super().__init__(thread_data)

        self.hostgroup = int(thread_data.get("hostgroup"))
        self.frontend_host = self._format_string(thread_data.get("frontend_host", ""))
        self.host = self._format_string(thread_data.get("backend_host", ""))
        self.time = int(thread_data.get("time", 0)) / 1000  # Convert to seconds since ProxySQL returns milliseconds
        self.formatted_query = format_query(thread_data.get("query", "").strip(" \t\n\r"))
        color = self._get_time_color(self.time, self.formatted_query.code)
        self.formatted_time = self._format_time_with_color(self.time, color)
        self.command = self._format_command(thread_data.get("command", ""))
        self.extended_info = thread_data.get("extended_info", "")


class HotkeyCommands:
    show_thread = "show_thread"
    thread_filter = "thread_filter"
    thread_kill_by_parameter = "thread_kill_by_parameter"
    variable_search = "variable_search"
    rename_tab = "rename_tab"
    refresh_interval = "refresh_interval"
    replay_seek = "replay_seek"
    maximize_panel = "maximize_panel"
