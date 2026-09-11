from __future__ import annotations

import os
from collections.abc import Collection
from dataclasses import dataclass
from pathlib import Path

from dolphie.DataTypes import ConnectionSource, ConnectionSourceType

COMPOSE_ROOT = Path(__file__).parent / "compose"
STANDALONE_DIR = COMPOSE_ROOT / "standalone"
STANDALONE_PROJECT = "dolphie-standalone"


@dataclass(frozen=True)
class Server:
    """One endpoint from the standalone compose file, addressed by its compose profile."""

    id: str
    port: int
    flavor: ConnectionSourceType
    version: str
    host: str = "127.0.0.1"
    user: str = "root"
    password: str = "root"
    image: str = ""
    # Set when the image exists for one architecture only, so the host needs emulation to run it.
    platform: str | None = None

    @property
    def is_mariadb(self) -> bool:
        return self.flavor == ConnectionSource.mariadb

    @property
    def distro(self) -> str:
        """The host_distro Dolphie reports for this server."""
        if self.is_mariadb:
            return "MariaDB"
        return "Percona Server" if self.id.startswith("percona") else "MySQL"


STANDALONE_SERVERS: dict[str, Server] = {
    server.id: server
    for server in (
        Server("mysql57", 33057, ConnectionSource.mysql, "5.7", image="mysql:5.7", platform="linux/amd64"),
        Server("mysql80", 33080, ConnectionSource.mysql, "8.0"),
        Server("mysql84", 33084, ConnectionSource.mysql, "8.4"),
        Server("mysql97", 33097, ConnectionSource.mysql, "9.7"),
        Server("percona84", 33184, ConnectionSource.mysql, "8.4"),
        Server("mariadb1011", 33111, ConnectionSource.mariadb, "10.11"),
        Server("mariadb114", 33114, ConnectionSource.mariadb, "11.4"),
        Server("mariadb118", 33118, ConnectionSource.mariadb, "11.8"),
        Server("mariadb123", 33123, ConnectionSource.mariadb, "12.3"),
        Server("proxysql", 36032, ConnectionSource.proxysql, "3.0", user="radmin", password="radmin"),
    )
}

# ProxySQL's client-facing port. Traffic sent here lands on the mysql84 backend and moves the ProxySQL counters.
PROXYSQL_FRONTEND = Server("proxysql-frontend", 36033, ConnectionSource.mysql, "8.4")


def _selected(env_var: str, known: Collection[str]) -> set[str]:
    """Ids from a comma separated environment variable, or every known id when unset or "all"."""
    known_ids = set(known)
    raw = os.environ.get(env_var, "").strip()
    if not raw or raw == "all":
        return known_ids

    ids = {item.strip() for item in raw.split(",") if item.strip()}
    unknown = sorted(ids - known_ids)
    if unknown:
        raise ValueError(f"Unknown {env_var} entries: {unknown}. Known: {sorted(known_ids)}")
    return ids


def selected_server_ids() -> list[str]:
    """Standalone servers to run against, in matrix order, from DOLPHIE_IT_SERVERS."""
    selected = _selected("DOLPHIE_IT_SERVERS", STANDALONE_SERVERS)
    return [server_id for server_id in STANDALONE_SERVERS if server_id in selected]


@dataclass(frozen=True)
class Topology:
    """A multi-node compose directory and the endpoints the tests connect to."""

    id: str
    endpoints: dict[str, Server]

    @property
    def directory(self) -> Path:
        return COMPOSE_ROOT / self.id

    @property
    def project(self) -> str:
        return f"dolphie-{self.id}"


TOPOLOGIES: dict[str, Topology] = {
    topology.id: topology
    for topology in (
        Topology(
            "mariadb",
            {
                "primary": Server("mariadb-primary", 3341, ConnectionSource.mariadb, "11.4"),
                "replica_1": Server("mariadb-replica-1", 3342, ConnectionSource.mariadb, "11.4"),
                "replica_2": Server("mariadb-replica-2", 3343, ConnectionSource.mariadb, "11.4"),
            },
        ),
        Topology(
            "gr",
            {
                "node1": Server("gr-node1", 3321, ConnectionSource.mysql, "8.4"),
                "node2": Server("gr-node2", 3322, ConnectionSource.mysql, "8.4"),
                "node3": Server("gr-node3", 3323, ConnectionSource.mysql, "8.4"),
                "async_replica": Server("gr-async", 3324, ConnectionSource.mysql, "8.4"),
            },
        ),
        Topology(
            "galera",
            {
                "node1": Server("galera-node1", 3307, ConnectionSource.mariadb, "11.4"),
                "node2": Server("galera-node2", 3308, ConnectionSource.mariadb, "11.4"),
                "node3": Server("galera-node3", 3309, ConnectionSource.mariadb, "11.4"),
                "async_replica": Server("galera-async", 3310, ConnectionSource.mariadb, "11.4"),
            },
        ),
        Topology(
            "multi-source",
            {
                "primary_a": Server("ms-primary-a", 3331, ConnectionSource.mysql, "8.4"),
                "primary_b": Server("ms-primary-b", 3332, ConnectionSource.mysql, "8.4"),
                "replica": Server("ms-replica", 3333, ConnectionSource.mysql, "8.4"),
            },
        ),
        Topology(
            "clusterset",
            {
                "primary_node1": Server("cs-node1", 3311, ConnectionSource.mysql, "8.4"),
                "replica_node4": Server("cs-node4", 3314, ConnectionSource.mysql, "8.4"),
            },
        ),
    )
}


def selected_topology_ids() -> set[str]:
    """Topologies to run, from DOLPHIE_IT_TOPOLOGIES."""
    return _selected("DOLPHIE_IT_TOPOLOGIES", TOPOLOGIES)
