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
        self.status_flags = self._get_formatted_status_flags(thread_data.get("status_flags", ""))
        
        # Extract JSON fields from extended_info
        json_fields = self._extract_json_fields(self.extended_info)
        
        # Extended info fields extracted from JSON
        self.backend_multiplex_disabled = self._get_formatted_int_field(json_fields.get("backend_multiplex_disabled"))
        self.backend_multiplex_disabled_ext = self._get_formatted_int_field(json_fields.get("backend_multiplex_disabled_ext"))
        self.status_compression = self._get_formatted_int_field(json_fields.get("status_compression"))
        self.status_found_rows = self._get_formatted_int_field(json_fields.get("status_found_rows"))
        self.status_get_lock = self._get_formatted_int_field(json_fields.get("status_get_lock"))
        self.status_has_savepoint = self._get_formatted_int_field(json_fields.get("status_has_savepoint"))
        self.status_has_warnings = self._get_formatted_int_field(json_fields.get("status_has_warnings"))
        self.status_lock_tables = self._get_formatted_int_field(json_fields.get("status_lock_tables"))
        self.status_no_multiplex = self._get_formatted_int_field(json_fields.get("status_no_multiplex"))
        self.status_no_multiplex_hg = self._get_formatted_int_field(json_fields.get("status_no_multiplex_hg"))
        self.status_prepared_statement = self._get_formatted_int_field(json_fields.get("status_prepared_statement"))
        self.status_temporary_table = self._get_formatted_int_field(json_fields.get("status_temporary_table"))
        self.status_user_variable = self._get_formatted_int_field(json_fields.get("status_user_variable"))

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

    def _get_formatted_status_flags(self, status_flags: str):
        if not status_flags:
            return "[dark_gray]N/A"
        
        return status_flags
        
    def _get_formatted_int_field(self, value):
        """Format integer fields extracted from JSON (can be None, int, or string)"""
        if value is None:
            return "[dark_gray]-"
        
        # Handle JSON values which may return int, string, or None
        try:
            int_value = int(value)
            return str(int_value)
        except (ValueError, TypeError):
            return "[dark_gray]-"

    def _extract_json_fields(self, extended_info: str):
        """Extract specific fields from extended_info JSON string"""
        try:
            if not extended_info:
                return {}
            
            import json
            data = json.loads(extended_info)
            
            # Extract backend connection info if available
            backend_info = data.get('backends', [{}])[0] if 'backends' in data else {}
            conn_info = backend_info.get('conn', {})
            status_info = conn_info.get('status', {})
            
            return {
                'backend_multiplex_disabled': conn_info.get('MultiplexDisabled'),
                'backend_multiplex_disabled_ext': conn_info.get('MultiplexDisabled_ext'),
                'status_compression': status_info.get('compression'),
                'status_found_rows': status_info.get('found_rows'),
                'status_get_lock': status_info.get('get_lock'),
                'status_has_savepoint': status_info.get('has_savepoint'),
                'status_has_warnings': status_info.get('has_warnings'),
                'status_lock_tables': status_info.get('lock_tables'),
                'status_no_multiplex': status_info.get('no_multiplex'),
                'status_no_multiplex_hg': status_info.get('no_multiplex_HG'),
                'status_prepared_statement': status_info.get('prepared_statement'),
                'status_temporary_table': status_info.get('temporary_table'),
                'status_user_variable': status_info.get('user_variable'),
            }
        except (json.JSONDecodeError, KeyError, IndexError) as e:
            # Log error if needed, return empty dict to gracefully handle malformed JSON
            return {}


class HotkeyCommands:
    show_thread = "show_thread"
    thread_filter = "thread_filter"
    thread_kill_by_parameter = "thread_kill_by_parameter"
    variable_search = "variable_search"
    rename_tab = "rename_tab"
    refresh_interval = "refresh_interval"
    replay_seek = "replay_seek"
    maximize_panel = "maximize_panel"
