from dataclasses import dataclass
from typing import Dict, List

from dolphie.Modules.MySQL import Database
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
                replica.connection.close()

            self.replicas = {}

    def get_sorted_replicas(self) -> List[Replica]:
        return sorted(self.replicas.values(), key=lambda x: x.host)


@dataclass
class Panel:
    name: str
    visible: bool = False


class Panels:
    dashboard = Panel("dashboard")
    processlist = Panel("processlist")
    graphs = Panel("graphs")
    replication = Panel("replication")
    locks = Panel("locks")

    @classmethod
    def all(cls):
        return [
            panel.name for name, panel in cls.__dict__.items() if not name.startswith("__") and isinstance(panel, Panel)
        ]
