from dataclasses import dataclass
from typing import Dict, List

from dolphie.Modules.Functions import format_query, format_time
from dolphie.Modules.MySQL import Database
from rich.table import Table


@dataclass
class ConnectionStatus:
    connecting = "CONNECTING"
    disconnected = "DISCONNECTED"
    read_write = "R/W"
    read_only = "RO"


@dataclass
class Replica:
    thread_id: int
    host: str
    connection: Database = None
    table: Table = None
    replication_status: Dict[str, str] = None
    lag_source: str = None
    lag: int = None
    previous_sbm: int = 0
    mysql_version: str = None


class ReplicaManager:
    def __init__(self):
        self.available_replicas: list = []
        self.replicas: Dict[int, Replica] = {}
        self.ports: Dict[str, int] = {}

    def add(self, thread_id: int, host: str) -> Replica:
        self.replicas[thread_id] = Replica(thread_id=thread_id, host=host)

        return self.replicas[thread_id]

    def remove(self, thread_id: int):
        del self.replicas[thread_id]

    def get(self, thread_id: int) -> Replica:
        return self.replicas.get(thread_id)

    def remove_all(self):
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
    visible: bool = False


class Panels:
    def __init__(self):
        self.dashboard = Panel("dashboard")
        self.processlist = Panel("processlist")
        self.graphs = Panel("graphs")
        self.replication = Panel("replication")
        # self.innodb_trx_locks = Panel("innodb_trx_locks")
        self.metadata_locks = Panel("metadata_locks")
        self.ddl = Panel("ddl")

    def get_panel(self, panel_name: str) -> Panel:
        return self.__dict__.get(panel_name, None)

    def get_all_panels(self) -> List[Panel]:
        return [panel for panel in self.__dict__.values() if isinstance(panel, Panel)]

    def all(self) -> List[str]:
        return [
            panel.name
            for name, panel in self.__dict__.items()
            if not name.startswith("__") and isinstance(panel, Panel)
        ]


class ProcesslistThread:
    def __init__(self, thread_data: Dict[str, str]):
        self.id = str(thread_data.get("id", ""))
        self.mysql_thread_id = thread_data.get("mysql_thread_id")
        self.user = thread_data.get("user", "")
        self.host = thread_data.get("host", "")
        self.db = thread_data.get("db", "")
        self.query = thread_data.get("query", "")
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
        if "Group replication" not in self.query:  # Don't color GR threads
            if "SELECT /*!40001 SQL_NO_CACHE */ *" in self.query:
                thread_color = "purple"
            elif self.query:
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
    thread_kill_by_id = "thread_kill_by_id"
    thread_kill_by_parameter = "thread_kill_by_parameter"
    variable_search = "variable_search"
    rename_tab = "rename_tab"
    refresh_interval = "refresh_interval"
