from __future__ import annotations

import hashlib
from dataclasses import dataclass, field, fields
from datetime import date, datetime, timedelta
from decimal import Decimal
from threading import Lock
from typing import TYPE_CHECKING, Final, Literal, TypedDict

from dolphie.Modules.Functions import coerce_float, coerce_int, coerce_str, format_query, format_time

if TYPE_CHECKING:
    from dolphie.Modules.MySQL import Database

ConnectionSourceType = Literal["MySQL", "ProxySQL", "MariaDB"]
ConnectionStatusType = Literal["CONNECTING", "CONNECTED", "DISCONNECTED", "R/W", "RO"]
DatabaseScalar = str | int | float | Decimal | date | datetime | timedelta | None
DatabaseRow = dict[str, DatabaseScalar]


class ConnectionSource:
    mysql: Final[Literal["MySQL"]] = "MySQL"
    proxysql: Final[Literal["ProxySQL"]] = "ProxySQL"
    mariadb: Final[Literal["MariaDB"]] = "MariaDB"


class ConnectionStatus:
    connecting: Final[Literal["CONNECTING"]] = "CONNECTING"
    connected: Final[Literal["CONNECTED"]] = "CONNECTED"
    disconnected: Final[Literal["DISCONNECTED"]] = "DISCONNECTED"
    read_write: Final[Literal["R/W"]] = "R/W"
    read_only: Final[Literal["RO"]] = "RO"


class ReplicaRow(TypedDict, total=False):
    id: int
    user: str
    host: str
    replica_uuid: str
    identity: str
    report_host: str
    port: int | None


@dataclass
class Replica:
    identity: str
    row_key: str
    host: str
    user: str = ""
    thread_id: int | None = None
    port: int | None = None
    host_distro: str | None = None
    connection: Database | None = None
    connection_source_alt: ConnectionSourceType | None = None
    replication_status: DatabaseRow = field(default_factory=dict)
    replication_source_uuids: set[str] = field(default_factory=set)
    group_replication_view_change_uuid: str = ""
    mysql_version: str | None = None
    last_error: str | None = None
    consecutive_errors: int = 0
    next_poll_at: float = 0
    errant_transactions: str | None = None
    errant_check_error: str | None = None
    next_errant_check_at: float = 0
    mariadb_gtid_slave_pos: str = ""
    # The address MariaDB's own SHOW SLAVE HOSTS advertises for this replica's
    # server_id, resolved once the replica's own @@server_id is known. Purely
    # informational — connections keep using host/port so a report_host that's
    # identical across several replicas (e.g. all published via 127.0.0.1 with
    # distinct ports) never causes reconnect churn, since discovery can't tell
    # those replicas apart from the primary side alone.
    reported_host: str | None = None
    reported_port: int | None = None

    @property
    def host_with_port(self) -> str:
        host = self.reported_host or self.host
        port = self.reported_port if self.reported_port is not None else self.port
        display_host = f"[{host}]" if ":" in host and not host.startswith("[") else host
        return f"{display_host}:{port}" if port is not None else display_host


class ReplicaManager:
    def __init__(self):
        self._lock = Lock()
        self._available_replicas: tuple[ReplicaRow, ...] = ()
        self._replicas: dict[str, Replica] = {}
        # Cached SHOW REPLICAS/SHOW SLAVE HOSTS rows keyed by the processlist
        # discovery signature that produced them. Only touched by the main
        # worker thread's discovery cycle, and discarded with the manager on
        # reconnect.
        self.reported_replica_signature: object | None = None
        self.reported_replicas: list[DatabaseRow] = []
        # Server_id -> (Host, Port) from the latest SHOW SLAVE HOSTS; see
        # Replica.reported_host for why this exists. Written only by the main
        # worker thread's discovery cycle and replaced wholesale (never
        # mutated in place), so the replicas worker thread can read it without
        # a lock while polling.
        self.mariadb_reported_ports: dict[int, tuple[str, int]] = {}

    @property
    def available_replicas(self) -> list[ReplicaRow]:
        """Return a detached discovery snapshot safe for another worker to consume."""
        with self._lock:
            return [row.copy() for row in self._available_replicas]

    @available_replicas.setter
    def available_replicas(self, replicas: list[ReplicaRow]) -> None:
        self.replace_discovery(replicas)

    def replace_discovery(self, replicas: list[ReplicaRow]) -> None:
        """Atomically publish a complete replica discovery cycle."""
        snapshot = tuple(row.copy() for row in replicas)
        with self._lock:
            self._available_replicas = snapshot

    @property
    def discovery_count(self) -> int:
        with self._lock:
            return len(self._available_replicas)

    @property
    def active_count(self) -> int:
        with self._lock:
            return len(self._replicas)

    @staticmethod
    def create_replica_row_key(identity: str) -> str:
        """Create a stable, Textual-safe widget key from a replica identity."""
        digest = hashlib.blake2s(identity.encode(), digest_size=8).hexdigest()
        return f"replica-{digest}"

    def upsert_replica(self, identity: str, thread_id: int, host: str, port: int, user: str = "") -> Replica:
        row_key = self.create_replica_row_key(identity)
        # Closed outside the lock (like remove_replica) so a wedged socket
        # can't block UI-thread readers of this manager.
        stale_connection: Database | None = None
        with self._lock:
            replica = self._replicas.get(row_key)
            if replica is None:
                replica = Replica(
                    identity=identity,
                    row_key=row_key,
                    user=user,
                    thread_id=thread_id,
                    host=host,
                    port=port,
                )
                self._replicas[row_key] = replica
                return replica

            if replica.host != host or replica.port != port:
                stale_connection = replica.connection
                # Reset every field to its dataclass default (driven by the
                # dataclass itself, not a hand-kept list, so newly added fields
                # can't be forgotten here) while keeping this the same object,
                # since callers rely on identity surviving an upsert.
                defaults = Replica(
                    identity=identity, row_key=row_key, user=user, thread_id=thread_id, host=host, port=port
                )
                for f in fields(Replica):
                    setattr(replica, f.name, getattr(defaults, f.name))
            else:
                replica.thread_id = thread_id
                replica.host = host
                replica.port = port
                replica.user = user
        if stale_connection:
            stale_connection.close()
        return replica

    def remove_replica(self, row_key: str):
        with self._lock:
            replica = self._replicas.pop(row_key, None)
        if replica and replica.connection:
            replica.connection.close()

    def remove_missing_replicas(self, active_row_keys: set[str]) -> None:
        with self._lock:
            stale_row_keys = set(self._replicas) - active_row_keys
        for row_key in stale_row_keys:
            self.remove_replica(row_key)

    def remove_all_replicas(self):
        with self._lock:
            replicas = list(self._replicas.values())
            self._replicas = {}
        for replica in replicas:
            if replica.connection:
                replica.connection.close()

    def get_sorted_replicas(self) -> list[Replica]:
        with self._lock:
            return sorted(self._replicas.values(), key=lambda x: x.host)


@dataclass
class Panel:
    name: str
    display_name: str
    key: str | None = None
    visible: bool = False
    daemon_supported: bool = True

    @property
    def formatted_key(self) -> str:
        # Rich markup — used in Rich Table titles (Dashboard, ProxySQL)
        return f"[$b_highlight]{self.key}[/$b_highlight]"

    @property
    def content_key(self) -> str:
        return self.formatted_key

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
                f"Panel(s) [$red2]{', '.join(invalid_panels)}[/$red2] are not valid (see --help for more information)"
            )

        return panels

    def get_all_daemon_panel_names(self) -> list[str]:
        return [panel.name for panel in self._registry.values() if panel.daemon_supported]

    def get_all_panels(self) -> list[Panel]:
        return list(self._registry.values())

    def all(self) -> list[str]:
        return list(self._registry.keys())


class BaseProcesslistThread:
    def __init__(self, thread_data: DatabaseRow):
        self.thread_data = thread_data
        self.id = str(thread_data.get("id", ""))
        self.user = coerce_str(thread_data.get("user"))
        self.db = coerce_str(thread_data.get("db"))

    @staticmethod
    def _get_time_color(time: float, query_code: str) -> str:
        if not query_code:
            return ""
        if time >= 10:
            return "$red"
        elif time >= 5:
            return "$yellow"
        return "$green"

    @staticmethod
    def _format_time_with_color(time: float, color: str) -> str:
        return f"[{color}]{format_time(time)}[/{color}]" if color else format_time(time)

    @staticmethod
    def _format_command(command: str) -> str:
        return "[$red]Killed[/$red]" if command == "Killed" else command

    @staticmethod
    def _format_string(string: str) -> str:
        return string if string else "[$dark_gray]N/A"

    @staticmethod
    def _format_number(number: DatabaseScalar) -> str | int:
        value = coerce_int(number)
        return "[$dark_gray]0" if value == 0 else value


class ProcesslistThread(BaseProcesslistThread):
    def __init__(self, thread_data: DatabaseRow):
        super().__init__(thread_data)

        mysql_thread_id = thread_data.get("mysql_thread_id")
        self.mysql_thread_id = coerce_int(mysql_thread_id) if mysql_thread_id is not None else None
        self.host = coerce_str(thread_data.get("host"))
        self.time = coerce_int(thread_data.get("time"))
        self.protocol = self._format_string(coerce_str(thread_data.get("connection_type")))
        self.formatted_query = format_query(coerce_str(thread_data.get("query")))
        self.formatted_time = self._format_time_with_color(self.time, self._mysql_time_color())
        self.command = self._format_command(coerce_str(thread_data.get("command")))
        self.state = self._format_string(coerce_str(thread_data.get("state")))
        self.trx_state = self._format_string(coerce_str(thread_data.get("trx_state")))
        self.trx_operation_state = self._format_string(coerce_str(thread_data.get("trx_operation_state")))
        self.trx_rows_locked = self._format_number(thread_data.get("trx_rows_locked", 0))
        self.trx_rows_modified = self._format_number(thread_data.get("trx_rows_modified", 0))
        self.trx_concurrency_tickets = self._format_number(thread_data.get("trx_concurrency_tickets", 0))
        trx_time = thread_data.get("trx_time", "")
        self.trx_time = format_time(coerce_int(trx_time)) if trx_time else "[$dark_gray]N/A"

    def _mysql_time_color(self) -> str:
        if "Group replication" in self.formatted_query.code:  # Don't color GR threads
            return ""
        if "SELECT /*!40001 SQL_NO_CACHE */ *" in self.formatted_query.code:
            return "purple"
        return self._get_time_color(self.time, self.formatted_query.code)


class ProxySQLProcesslistThread(BaseProcesslistThread):
    def __init__(self, thread_data: DatabaseRow):
        super().__init__(thread_data)

        self.hostgroup = coerce_int(thread_data.get("hostgroup"))
        self.frontend_host = self._format_string(coerce_str(thread_data.get("frontend_host")))
        self.host = self._format_string(coerce_str(thread_data.get("backend_host")))
        self.time = coerce_float(thread_data.get("time")) / 1000
        self.formatted_query = format_query(coerce_str(thread_data.get("query")).strip())
        color = self._get_time_color(self.time, self.formatted_query.code)
        self.formatted_time = self._format_time_with_color(self.time, color)
        self.command = self._format_command(coerce_str(thread_data.get("command")))
        self.extended_info = coerce_str(thread_data.get("extended_info"))


class HotkeyCommands:
    show_thread = "show_thread"
    thread_filter = "thread_filter"
    thread_kill_by_parameter = "thread_kill_by_parameter"
    variable_search = "variable_search"
    rename_tab = "rename_tab"
    refresh_interval = "refresh_interval"
    replay_seek = "replay_seek"
    maximize_panel = "maximize_panel"
