from dataclasses import dataclass
from typing import List

from dolphie.Modules.MySQL import Database
from rich.table import Table


class EnumBase:
    @classmethod
    def ALL(cls):
        return [value for name, value in cls.__dict__.items() if not name.startswith("__") and isinstance(value, str)]


@dataclass
class Replica:
    thread_id: int
    ai_id: int
    host: str
    connection: Database = None
    table: Table = None
    previous_sbm: int = 0
    mysql_version: str = None


class ReplicaManager:
    def __init__(self):
        self.replicas: dict = {}
        self.ports: dict = {}

        self.replica_increment_num: int = 1

    def add(self, thread_id: int, host: str) -> Replica:
        self.replicas[thread_id] = Replica(thread_id=thread_id, ai_id=self.replica_increment_num, host=host)
        self.replica_increment_num += 1

        return self.replicas[thread_id]

    def remove(self, thread_id):
        del self.replicas[thread_id]

    def get(self, thread_id: int) -> Replica:
        return self.replicas[thread_id]

    def remove_all(self):
        for replica in self.replicas.values():
            replica.connection.close()

        self.replicas = {}

    def get_sorted_replicas(self) -> List[Replica]:
        return sorted(self.replicas.values(), key=lambda x: x.host)


class Panels(EnumBase):
    DASHBOARD = "dashboard"
    PROCESSLIST = "processlist"
    GRAPHS = "graphs"
    REPLICATION = "replication"
    LOCKS = "locks"
