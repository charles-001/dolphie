from __future__ import annotations

import hashlib
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import orjson
import pytest
import zstandard as zstd

from dolphie.DataTypes import ConnectionSource, ProcesslistThread, ProxySQLProcesslistThread
from dolphie.Dolphie import Dolphie
from dolphie.Modules.Functions import coerce_int
from dolphie.Modules.ReplayManager import MySQLReplayData, ReplayManager

SCHEMA = """
CREATE TABLE replay_data (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp DATETIME, data BLOB);
CREATE INDEX idx_replay_data_timestamp ON replay_data (timestamp);
CREATE TABLE metadata (schema_version INTEGER DEFAULT 1, host VARCHAR(255), port INTEGER, host_distro VARCHAR(255),
    connection_source VARCHAR(255), dolphie_version VARCHAR(255), compression_dict BLOB);
CREATE TABLE variable_changes (id INTEGER PRIMARY KEY AUTOINCREMENT, replay_id INTEGER, timestamp DATETIME,
    variable_name VARCHAR(255), old_value VARCHAR(255), new_value VARCHAR(255));
"""


def make_row(index: int) -> dict[str, Any]:
    return {
        "global_status": {"Uptime": 1000 + index, "replay_polling_latency": 0.1},
        "global_variables": {"version": "8.4.7", "server_uuid": "abc"},
        "processlist": [{"id": 7, "user": "app", "query": f"SELECT {index}"}],
        "metric_manager": {"datetimes": [f"row-{index}"], "_delta": True},
        "replication_status": [{"Source_Host": "primary", "Source_UUID": "uuid"}],
    }


def write_replay_file(
    path: Path,
    row_count: int,
    *,
    dolphie_version: str = "6.15.0",
    start: datetime = datetime(2026, 9, 1, 12, 0, 0, tzinfo=timezone.utc),
    spacing: timedelta = timedelta(seconds=2),
) -> None:
    """Writes rows the way the recorder does: samples without a dictionary, then a raw-content dictionary."""
    samples = [orjson.dumps(make_row(index)) for index in range(ReplayManager.COMPRESSION_DICT_SAMPLES)]
    dictionary = zstd.ZstdCompressionDict(b"".join(samples), dict_type=zstd.DICT_TYPE_RAWCONTENT)
    plain = zstd.ZstdCompressor(level=ReplayManager.COMPRESSION_LEVEL)
    with_dict = zstd.ZstdCompressor(level=ReplayManager.COMPRESSION_LEVEL, dict_data=dictionary)

    connection = sqlite3.connect(path)
    try:
        connection.executescript(SCHEMA)
        connection.execute(
            "INSERT INTO metadata VALUES (?, ?, ?, ?, ?, ?, ?)",
            (2, "db1", 3306, "MySQL", ConnectionSource.mysql, dolphie_version, dictionary.as_bytes()),
        )
        for index in range(row_count):
            payload = orjson.dumps(make_row(index))
            compressor = plain if index < len(samples) else with_dict
            connection.execute(
                "INSERT INTO replay_data (timestamp, data) VALUES (?, ?)",
                ((start + spacing * index).strftime("%Y-%m-%d %H:%M:%S"), compressor.compress(payload)),
            )
        connection.commit()
    finally:
        connection.close()


def make_dolphie(replay_file: Path | None, **overrides: Any) -> tuple[Dolphie, list[tuple[str, dict[str, Any]]]]:
    notifications: list[tuple[str, dict[str, Any]]] = []
    app = SimpleNamespace(notify=lambda message, **kwargs: notifications.append((message, kwargs)))
    dolphie = SimpleNamespace(
        host="ignored",
        port=1,
        host_distro="MySQL",
        connection_source=ConnectionSource.mysql,
        replay_file=str(replay_file) if replay_file else None,
        replay_dir=None,
        daemon_mode=False,
        record_for_replay=False,
        replay_retention_hours=48,
        app_version="6.16.0",
        app=app,
    )
    for key, value in overrides.items():
        setattr(dolphie, key, value)
    return cast(Dolphie, dolphie), notifications


def test_playback_reads_the_file_without_modifying_it(tmp_path: Path) -> None:
    replay_file = tmp_path / "daemon.db"
    write_replay_file(replay_file, row_count=5)
    replay_file.chmod(0o640)
    before = hashlib.sha256(replay_file.read_bytes()).hexdigest()
    dolphie, notifications = make_dolphie(replay_file)

    manager = ReplayManager(dolphie)
    try:
        assert manager.verify_replay_file()
        assert (dolphie.host, dolphie.port, dolphie.host_with_port) == ("db1", 3306, "db1:3306")

        # The first rows were compressed before the dictionary existed and the later rows with it
        for index in range(5):
            data = manager.get_next_refresh_interval()
            assert isinstance(data, MySQLReplayData)
            assert data.global_status["Uptime"] == 1000 + index
            assert data.replication_status == [{"Source_Host": "primary", "Source_UUID": "uuid"}]
        assert notifications == []

        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            manager._execute_modify("DELETE FROM replay_data")
    finally:
        manager.close()

    assert hashlib.sha256(replay_file.read_bytes()).hexdigest() == before
    assert replay_file.stat().st_mode & 0o777 == 0o640
    assert not (tmp_path / "daemon.db-journal").exists()


def test_playback_of_a_file_that_is_not_sqlite_fails_with_a_notification(tmp_path: Path) -> None:
    replay_file = tmp_path / "broken.db"
    replay_file.write_bytes(b"this is not a sqlite database")
    dolphie, notifications = make_dolphie(replay_file)

    manager = ReplayManager(dolphie)
    try:
        assert manager.verify_replay_file() is False
    finally:
        manager.close()

    assert [kwargs.get("severity") for _, kwargs in notifications] == ["error"]


def test_playback_warns_when_the_file_was_recorded_by_a_newer_dolphie(tmp_path: Path) -> None:
    replay_file = tmp_path / "daemon.db"
    write_replay_file(replay_file, row_count=1, dolphie_version="99.0.0")
    dolphie, notifications = make_dolphie(replay_file)

    manager = ReplayManager(dolphie)
    try:
        assert manager.verify_replay_file()
    finally:
        manager.close()

    assert [kwargs.get("title") for _, kwargs in notifications] == ["Replay recorded by a newer Dolphie"]
    assert "99.0.0" in notifications[0][0]


@pytest.mark.parametrize("dolphie_version", ["6.15.0", "6.16.0", "snapshot"])
def test_playback_does_not_warn_for_older_or_unparseable_versions(tmp_path: Path, dolphie_version: str) -> None:
    replay_file = tmp_path / "daemon.db"
    write_replay_file(replay_file, row_count=1, dolphie_version=dolphie_version)
    dolphie, notifications = make_dolphie(replay_file)

    manager = ReplayManager(dolphie)
    try:
        assert manager.verify_replay_file()
    finally:
        manager.close()

    assert notifications == []


def test_daemon_records_its_version_in_the_metadata(tmp_path: Path) -> None:
    replay_file = tmp_path / "db1_3306" / "daemon.db"
    replay_file.parent.mkdir()
    write_replay_file(replay_file, row_count=3, dolphie_version="6.9.1")
    dolphie, _ = make_dolphie(
        None,
        host="db1",
        port=3306,
        replay_dir=str(tmp_path),
        daemon_mode=True,
        record_for_replay=True,
    )

    manager = ReplayManager(dolphie)
    manager.close()

    connection = sqlite3.connect(replay_file)
    try:
        assert connection.execute("SELECT dolphie_version FROM metadata").fetchone() == ("6.16.0",)
    finally:
        connection.close()


def test_daemon_creates_new_files_with_large_pages(tmp_path: Path) -> None:
    dolphie, _ = make_dolphie(
        None,
        host="db1",
        port=3306,
        replay_dir=str(tmp_path),
        daemon_mode=True,
        record_for_replay=True,
    )

    manager = ReplayManager(dolphie)
    try:
        assert manager._execute_select_one("PRAGMA page_size") == (ReplayManager.PAGE_SIZE,)
        assert manager._execute_select_one("PRAGMA auto_vacuum") == (1,)
    finally:
        manager.close()


def test_playback_skips_rows_it_cannot_read(tmp_path: Path) -> None:
    replay_file = tmp_path / "daemon.db"
    write_replay_file(replay_file, row_count=6)
    connection = sqlite3.connect(replay_file)
    try:
        # A row a daemon never finished writing, and a row with valid compression but no JSON object
        connection.execute("UPDATE replay_data SET data = ? WHERE id = 2", (b"\x28\xb5\x2f\xfdtruncated",))
        connection.execute(
            "UPDATE replay_data SET data = ? WHERE id = 4",
            (zstd.ZstdCompressor(level=ReplayManager.COMPRESSION_LEVEL).compress(b"[1, 2, 3]"),),
        )
        connection.commit()
    finally:
        connection.close()
    dolphie, notifications = make_dolphie(replay_file)

    manager = ReplayManager(dolphie)
    try:
        assert manager.verify_replay_file()
        uptimes = []
        while (data := manager.get_next_refresh_interval()) is not None:
            uptimes.append(data.global_status["Uptime"])
    finally:
        manager.close()

    assert uptimes == [1000, 1002, 1004, 1005]
    assert [kwargs.get("title") for _, kwargs in notifications] == ["Unreadable replay data"] * 2
    assert "Row 2 at" in notifications[0][0]
    assert "Row 4 at" in notifications[1][0]


def test_playback_gives_up_after_too_many_unreadable_rows(tmp_path: Path) -> None:
    replay_file = tmp_path / "daemon.db"
    write_replay_file(replay_file, row_count=ReplayManager.MAX_SKIPPED_ROWS + 5)
    connection = sqlite3.connect(replay_file)
    try:
        connection.execute("UPDATE replay_data SET data = ? WHERE id > 1", (b"garbage",))
        connection.commit()
    finally:
        connection.close()
    dolphie, notifications = make_dolphie(replay_file)

    manager = ReplayManager(dolphie)
    try:
        assert manager.verify_replay_file()
        assert manager.get_next_refresh_interval() is not None
        assert manager.get_next_refresh_interval() is None
        assert manager.current_replay_id == ReplayManager.MAX_SKIPPED_ROWS + 1
    finally:
        manager.close()

    assert len(notifications) == 1


def test_payload_fields_are_coerced_to_the_containers_panels_expect() -> None:
    manager = ReplayManager.__new__(ReplayManager)
    foreign = {
        "processlist": {"id": 1},
        "global_status": [],
        "global_variables": None,
        "metric_manager": "delta",
        "replica_manager": {},
        "replication_status": "none",
        "replication_applier_status": [],
        "metadata_locks": {"a": 1},
        "group_replication_data": [],
        "group_replication_members": {},
        "clusterset_instances": None,
        "galera_cluster_members": 3,
        "file_io_data": [],
        "command_stats": {},
        "hostgroup_summary": None,
    }

    mysql_data = manager._create_mysql_replay_data("2026-01-01 00:00:00", foreign)
    proxysql_data = manager._create_proxysql_replay_data("2026-01-01 00:00:00", foreign)

    assert mysql_data.processlist == {}
    assert mysql_data.global_status == {}
    assert mysql_data.global_variables == {}
    assert mysql_data.metric_manager == {}
    assert mysql_data.replica_manager == []
    assert mysql_data.replication_status == []
    assert mysql_data.replication_applier_status == {}
    assert mysql_data.metadata_locks == []
    assert mysql_data.group_replication_data == {}
    assert mysql_data.group_replication_members == []
    assert mysql_data.clusterset_instances == []
    assert mysql_data.galera_cluster_members == []
    assert mysql_data.file_io_data.filtered_data == {}
    assert proxysql_data.command_stats == []
    assert proxysql_data.hostgroup_summary == []

    # Thread ids are keyed as integers however the file spelled them. Threads without an id, or
    # that are not objects, are dropped rather than raising
    partial = manager._create_mysql_replay_data(
        "2026-01-01 00:00:00",
        {"processlist": [{"id": "7", "query": "SELECT 1"}, {"query": "SELECT 2"}, "SELECT 3", None]},
    )
    assert list(partial.processlist) == [7]
    assert isinstance(partial.processlist[7], ProcesslistThread)
    proxysql_partial = manager._create_proxysql_replay_data(
        "2026-01-01 00:00:00", {"processlist": [{"id": "8", "hostgroup": None, "time": None, "query": None}]}
    )
    assert isinstance(proxysql_partial.processlist[8], ProxySQLProcesslistThread)

    empty = manager._create_mysql_replay_data("2026-01-01 00:00:00", {})
    assert empty.processlist == {}


def test_metadata_refreshes_after_the_daemon_purges_the_head_of_the_file(tmp_path: Path) -> None:
    replay_file = tmp_path / "daemon.db"
    write_replay_file(replay_file, row_count=3)
    dolphie, _ = make_dolphie(replay_file)
    manager = ReplayManager(dolphie)
    try:
        assert manager._update_replay_metadata_cache()
        assert (manager.min_replay_id, manager.max_replay_id, manager.total_replay_rows) == (1, 3, 3)

        writer = sqlite3.connect(replay_file)
        try:
            writer.execute("DELETE FROM replay_data WHERE id = 1")
            writer.commit()
        finally:
            writer.close()

        assert manager._update_replay_metadata_cache()
        assert (manager.min_replay_id, manager.max_replay_id, manager.total_replay_rows) == (2, 3, 2)
    finally:
        manager.close()


def test_insert_failure_surfaces_the_original_error(tmp_path: Path) -> None:
    replay_file = tmp_path / "daemon.db"
    write_replay_file(replay_file, row_count=1)
    dolphie, _ = make_dolphie(None)
    manager = ReplayManager(dolphie)
    manager.connection = sqlite3.connect(replay_file, isolation_level=None, timeout=0.05)
    writer = sqlite3.connect(replay_file, isolation_level=None)
    try:
        writer.execute("BEGIN IMMEDIATE")
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            manager._insert_replay_data("2026-01-01 00:00:00", b"{}")
    finally:
        writer.execute("ROLLBACK")
        writer.close()
        manager.connection.close()


def test_raw_content_dictionary_reads_rows_written_before_and_after_it(tmp_path: Path) -> None:
    dolphie, _ = make_dolphie(None)
    manager = ReplayManager(dolphie)
    manager.connection = sqlite3.connect(tmp_path / "dict.db", isolation_level=None)
    try:
        manager.connection.executescript(SCHEMA)
        manager.connection.execute("INSERT INTO metadata (schema_version) VALUES (2)")

        rows = [orjson.dumps(make_row(index)) for index in range(ReplayManager.COMPRESSION_DICT_SAMPLES + 3)]
        blobs: list[bytes] = []
        for payload in rows:
            manager._handle_compression_training(payload)
            blobs.append(manager._compressor.compress(payload))

        assert manager.compression_dict is not None
        assert len(blobs[-1]) < len(blobs[0]) / 3

        stored = manager.connection.execute("SELECT compression_dict FROM metadata").fetchone()[0]
        # Readers, including older Dolphie versions, load the stored bytes with type auto-detection
        reader = zstd.ZstdDecompressor(dict_data=zstd.ZstdCompressionDict(stored))
        assert [reader.decompress(blob) for blob in blobs] == rows
    finally:
        manager.connection.close()


def test_delta_metric_window_covers_exactly_the_rows_up_to_the_target(tmp_path: Path) -> None:
    replay_file = tmp_path / "daemon.db"
    write_replay_file(replay_file, row_count=20, spacing=timedelta(minutes=1))
    dolphie, _ = make_dolphie(replay_file)

    manager = ReplayManager(dolphie)
    try:
        assert manager.verify_replay_file()
        manager.current_replay_id = 15
        manager.current_replay_timestamp = "2026-09-01 12:14:00"

        window = manager.fetch_delta_metrics_for_window(15, window_minutes=5)
        assert [entry["datetimes"] for entry in window] == [[f"row-{index}"] for index in range(9, 15)]

        whole_history = manager.fetch_delta_metrics_for_window(15, window_minutes=0)
        assert [entry["datetimes"] for entry in whole_history] == [[f"row-{index}"] for index in range(15)]
    finally:
        manager.close()


def test_delta_metric_window_ignores_later_rows_written_after_the_clock_stepped_back(tmp_path: Path) -> None:
    replay_file = tmp_path / "daemon.db"
    write_replay_file(replay_file, row_count=20, spacing=timedelta(minutes=1))
    connection = sqlite3.connect(replay_file)
    try:
        # Rows 11 to 20 carry timestamps from 11:55 onwards, as after a DST fall-back
        stepped_back = datetime(2026, 9, 1, 11, 55, tzinfo=timezone.utc)
        for replay_id in range(11, 21):
            stamp = (stepped_back + timedelta(minutes=replay_id - 11)).strftime("%Y-%m-%d %H:%M:%S")
            connection.execute("UPDATE replay_data SET timestamp = ? WHERE id = ?", (stamp, replay_id))
        connection.commit()
    finally:
        connection.close()
    dolphie, _ = make_dolphie(replay_file)

    manager = ReplayManager(dolphie)
    try:
        assert manager.verify_replay_file()
        manager.current_replay_id = 10
        manager.current_replay_timestamp = "2026-09-01 12:09:00"

        window = manager.fetch_delta_metrics_for_window(10, window_minutes=10)
        assert [entry["datetimes"] for entry in window] == [[f"row-{index}"] for index in range(10)]
    finally:
        manager.close()


def uptime_of_next_frame(manager: ReplayManager) -> int:
    data = manager.get_next_refresh_interval()
    assert isinstance(data, MySQLReplayData)
    return coerce_int(data.global_status["Uptime"])


def test_seek_relative_steps_over_gaps_and_clamps_at_both_ends(tmp_path: Path) -> None:
    replay_file = tmp_path / "daemon.db"
    write_replay_file(replay_file, row_count=10)
    connection = sqlite3.connect(replay_file)
    try:
        # A retention purge leaves holes in the id sequence
        connection.execute("DELETE FROM replay_data WHERE id IN (4, 5)")
        connection.commit()
    finally:
        connection.close()
    dolphie, _ = make_dolphie(replay_file)

    manager = ReplayManager(dolphie)
    try:
        assert manager.verify_replay_file()
        assert [uptime_of_next_frame(manager) for _ in range(3)] == [1000, 1001, 1002]

        assert manager.seek_relative(1)
        assert manager.current_replay_timestamp == "2026-09-01 12:00:10"
        assert uptime_of_next_frame(manager) == 1005

        assert manager.seek_relative(-1)
        assert uptime_of_next_frame(manager) == 1002

        assert manager.seek_relative(100)
        assert uptime_of_next_frame(manager) == 1009
        assert not manager.seek_relative(1)

        assert manager.seek_relative(-100)
        assert uptime_of_next_frame(manager) == 1000
        assert not manager.seek_relative(-1)
        assert not manager.seek_relative(0)
        assert manager.current_replay_id == 1
    finally:
        manager.close()


def test_seek_to_timestamp_lands_on_the_exact_or_closest_earlier_frame(tmp_path: Path) -> None:
    replay_file = tmp_path / "daemon.db"
    write_replay_file(replay_file, row_count=10)
    dolphie, notifications = make_dolphie(replay_file)

    manager = ReplayManager(dolphie)
    try:
        assert manager.verify_replay_file()

        assert manager.seek_to_timestamp("2026-09-01 12:00:06")
        assert uptime_of_next_frame(manager) == 1003
        assert "Seeking to timestamp" in notifications[-1][0]

        assert manager.seek_to_timestamp("2026-09-01 12:00:07")
        assert uptime_of_next_frame(manager) == 1003
        assert "closest timestamp" in notifications[-1][0]

        assert not manager.seek_to_timestamp("2026-09-01 11:00:00")
        assert notifications[-1][1]["severity"] == "error"
        assert uptime_of_next_frame(manager) == 1004
    finally:
        manager.close()
