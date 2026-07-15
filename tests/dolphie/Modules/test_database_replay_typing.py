from __future__ import annotations

import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from dolphie.DataTypes import ConnectionSource, DatabaseRow, ProcesslistThread, ProxySQLProcesslistThread
from dolphie.Modules.MySQL import Database
from dolphie.Modules.PerformanceSchemaMetrics import PerformanceSchemaMetrics
from dolphie.Modules.ReplayManager import ReplayManager


class StatusDatabase(Database):
    def __init__(self, rows: list[DatabaseRow]) -> None:
        self.source = ConnectionSource.mysql
        self.rows = rows

    def execute(self, query: str, values: object = None, ignore_error: bool = False) -> int | None:
        return 0

    def fetchall(self) -> list[DatabaseRow]:
        return self.rows


def test_database_decodes_binary_scalars() -> None:
    database = Database.__new__(Database)
    database.non_printable_regex = re.compile(r"[^\x20-\x7e]")

    assert database._decode_value(b"plain text") == "plain text"
    assert database._decode_value(bytearray("café", "utf-8")) == "caf?"
    assert database._decode_value(42) == 42
    timestamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert database._decode_value(timestamp) is timestamp


def test_status_values_are_normalized_at_database_boundary() -> None:
    database = StatusDatabase(
        [
            {"Variable_name": "Threads_connected", "Value": "12"},
            {"Variable_name": "version", "Value": "8.4.0"},
            {"Variable_name": None, "Value": "ignored"},
        ]
    )

    assert database.fetch_status_and_variables("status") == {
        "Threads_connected": 12,
        "version": "8.4.0",
    }


def test_replay_payloads_use_integer_thread_ids_and_list_defaults() -> None:
    manager = ReplayManager.__new__(ReplayManager)

    mysql_data = manager._create_mysql_replay_data(
        "2026-01-01 00:00:00",
        {"processlist": [{"id": "7", "user": None, "query": "SELECT 1"}]},
    )
    proxysql_data = manager._create_proxysql_replay_data(
        "2026-01-01 00:00:00",
        {"processlist": [{"id": "8", "hostgroup": None, "time": None, "query": None}]},
    )

    assert isinstance(mysql_data.processlist[7], ProcesslistThread)
    assert mysql_data.replica_manager == []
    assert mysql_data.metadata_locks == []
    assert mysql_data.group_replication_members == []
    assert mysql_data.clusterset_instances == []
    assert isinstance(proxysql_data.processlist[8], ProxySQLProcesslistThread)
    assert proxysql_data.command_stats == []
    assert proxysql_data.hostgroup_summary == []


def test_replay_payload_preserves_clusterset_instances() -> None:
    manager = ReplayManager.__new__(ReplayManager)
    clusterset_instances = [
        {
            "clusterset_name": "production",
            "cluster_name": "replica-cluster",
            "cluster_role": "REPLICA",
        }
    ]

    mysql_data = manager._create_mysql_replay_data(
        "2026-01-01 00:00:00",
        {
            "processlist": [],
            "clusterset_instances": clusterset_instances,
        },
    )

    assert mysql_data.clusterset_instances == clusterset_instances


def test_replay_metadata_refreshes_after_external_head_purge(tmp_path: Path) -> None:
    replay_path = tmp_path / "replay.db"
    writer = sqlite3.connect(replay_path)
    reader = sqlite3.connect(replay_path)
    try:
        writer.execute("CREATE TABLE replay_data (id INTEGER PRIMARY KEY, timestamp TEXT)")
        writer.executemany(
            "INSERT INTO replay_data VALUES (?, ?)",
            [(1, "2026-01-01 00:00:01"), (2, "2026-01-01 00:00:02"), (3, "2026-01-01 00:00:03")],
        )
        writer.commit()

        manager = ReplayManager.__new__(ReplayManager)
        manager.connection = reader
        manager.min_replay_id = 0
        manager.max_replay_id = 0
        manager.min_replay_timestamp = None
        manager.max_replay_timestamp = None
        manager.total_replay_rows = 0
        manager._replay_metadata_change_token = None

        assert manager._update_replay_metadata_cache()
        assert (manager.min_replay_id, manager.max_replay_id, manager.total_replay_rows) == (1, 3, 3)

        writer.execute("DELETE FROM replay_data WHERE id = 1")
        writer.commit()

        assert manager._update_replay_metadata_cache()
        assert (manager.min_replay_id, manager.max_replay_id, manager.total_replay_rows) == (2, 3, 2)
    finally:
        reader.close()
        writer.close()


def test_performance_schema_metrics_skip_null_keys_and_normalize_names() -> None:
    metrics = PerformanceSchemaMetrics(
        [
            {"FILE_NAME": None, "COUNT_READ": 1},
            {"FILE_NAME": 123, "COUNT_READ": 2},
        ],
        "file_io",
        "FILE_NAME",
    )

    assert list(metrics.internal_data) == ["123"]
