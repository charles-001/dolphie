import re
from dataclasses import dataclass
from typing import Dict, List

from dolphie.Modules.Functions import format_time
from dolphie.Modules.MySQL import Database
from rich.markup import escape as markup_escape
from rich.table import Table


@dataclass
class Replica:
    thread_id: int
    host: str
    connection: Database = None
    table: Table = None
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
        self.locks = Panel("locks")
        self.ddl = Panel("ddl")

    def all(self):
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
        self.formatted_query = self._get_formatted_query(thread_data.get("query", ""))
        self.formatted_time = self._get_formatted_time()
        self.command = self._get_formatted_command(thread_data.get("command", ""))
        self.state = thread_data.get("state", "")
        self.trx_state = thread_data.get("trx_state", "")
        self.trx_operation_state = thread_data.get("trx_operation_state", "")
        self.trx_rows_locked = thread_data.get("trx_rows_locked", "")
        self.trx_rows_modified = thread_data.get("trx_rows_modified", "")
        self.trx_concurrency_tickets = thread_data.get("trx_concurrency_tickets", "")
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

    def _get_formatted_command(self, command: str) -> str:
        return "[red]Killed[/red]" if command == "Killed" else command

    def _get_formatted_trx_time(self, trx_time: str) -> str:
        return format_time(int(trx_time)) if trx_time else ""

    def _get_formatted_query(self, query: str) -> str:
        return markup_escape(re.sub(r"\s+", " ", query)) if query else ""
