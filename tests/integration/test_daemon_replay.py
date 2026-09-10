"""Daemon mode through the real CLI, then playback of the recorded file through the real app."""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

from dolphie.DataTypes import ConnectionSource
from tests.integration.cli import daemon, daemon_replay_file, replay_row_count, run_daemon, wait_for_rows
from tests.integration.harness import frontend_traffic, make_config, query, run_dolphie
from tests.integration.servers import Server


def test_daemon_mode_records_polls_to_a_replay_file(server: Server, tmp_path: Path) -> None:
    replay_file = run_daemon(server, tmp_path)
    log = (tmp_path / "daemon.log").read_text()

    assert "in daemon mode" in log
    assert "Shutting down" in log
    assert "ERROR" not in log, log
    assert "CRITICAL" not in log, log

    with sqlite3.connect(replay_file) as connection:
        metadata = connection.execute(
            "SELECT schema_version, host, port, host_distro, connection_source FROM metadata"
        ).fetchone()
        rows = connection.execute("SELECT COUNT(*), MIN(timestamp), MAX(timestamp) FROM replay_data").fetchone()

    assert metadata[1:3] == (server.host, server.port)
    assert metadata[3] == server.distro
    assert metadata[4] == ConnectionSource.mysql
    assert rows[0] >= 5
    assert rows[1] < rows[2]


def test_daemon_records_global_variable_changes(server: Server, tmp_path: Path) -> None:
    """A variable flipped while the daemon runs lands in variable_changes with old and new values."""
    original = int(query(server, "SELECT @@GLOBAL.max_connections AS v")[0]["v"])
    changed = original + 7
    try:
        with daemon(server, tmp_path) as (process, replay_file):
            wait_for_rows(process, replay_file, 3)
            query(server, f"SET GLOBAL max_connections = {changed}")
            wait_for_rows(process, replay_file, replay_row_count(replay_file) + 3)
    finally:
        query(server, f"SET GLOBAL max_connections = {original}")

    with sqlite3.connect(replay_file) as connection:
        changes = connection.execute(
            "SELECT old_value, new_value FROM variable_changes WHERE variable_name = 'max_connections'"
        ).fetchall()
    assert (str(original), str(changed)) in {(old, new) for old, new in changes}, changes


def test_daemon_replaces_a_file_with_an_old_schema(server: Server, tmp_path: Path) -> None:
    replay_file = daemon_replay_file(server, tmp_path)
    replay_file.parent.mkdir(parents=True)
    with sqlite3.connect(replay_file) as connection:
        connection.execute(
            "CREATE TABLE replay_data (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp DATETIME, data BLOB)"
        )
        connection.execute(
            "CREATE TABLE metadata (schema_version INTEGER DEFAULT 1, host VARCHAR(255), port INTEGER, "
            "host_distro VARCHAR(255), connection_source VARCHAR(255), dolphie_version VARCHAR(255), "
            "compression_dict BLOB)"
        )
        connection.execute(
            "INSERT INTO metadata (schema_version, host, port, host_distro, connection_source, dolphie_version) "
            "VALUES (1, ?, ?, 'MySQL', 'MySQL', '0.0.0')",
            (server.host, server.port),
        )

    run_daemon(server, tmp_path)

    renamed = replay_file.parent / "daemon.db_old_schema_v1"
    assert renamed.exists(), sorted(p.name for p in replay_file.parent.iterdir())
    log = (tmp_path / "daemon.log").read_text()
    assert "schema version of the replay file (1) differs" in log
    assert replay_row_count(replay_file) >= 5


async def test_daemon_recording_plays_back_in_the_tui(server: Server, tmp_path: Path) -> None:
    replay_file = run_daemon(server, tmp_path, min_rows=8)

    config = make_config(server, tmp_path, host="replay-ignored", port=1, replay_file=str(replay_file), replay_dir=None)
    async with run_dolphie(config) as harness:
        await harness.wait_for_replay_frame()
        replay_manager = harness.replay_manager
        dolphie = harness.dolphie

        # Metadata from the file replaces whatever the config said.
        assert dolphie.host == server.host
        assert dolphie.port == server.port
        assert dolphie.connection_source == ConnectionSource.mysql
        assert dolphie.global_status.get("Uptime")
        assert replay_manager.min_replay_timestamp
        assert replay_manager.max_replay_timestamp
        assert replay_manager.max_replay_id >= 8

        await harness.click_button("#pause_button")
        assert dolphie.pause_refresh
        current = replay_manager.current_replay_id
        await harness.press("right_square_bracket")
        await harness.wait_for(lambda: replay_manager.current_replay_id == current + 1, message="step forward")
        await harness.press("left_square_bracket")
        await harness.wait_for(lambda: replay_manager.current_replay_id == current, message="step back")

        # Presses this close together count as a held key, which runs off the start silently.
        # A tap after the key rests warns instead of failing.
        for _ in range(replay_manager.max_replay_id + 1):
            await harness.press("left_square_bracket")
        await asyncio.sleep(harness.app.key_event_manager.replay_release_threshold.total_seconds() + 0.1)
        await harness.press("left_square_bracket")
        assert harness.notifications_with("already at the beginning")

        # Every MySQL panel toggles on in replay mode, including metadata locks.
        for key in ("3", "5", "7", "8"):
            await harness.press(key)
        assert dolphie.panels.graphs.visible
        assert dolphie.panels.metadata_locks.visible
        assert dolphie.panels.pfs_metrics.visible
        assert dolphie.panels.statements_summary.visible

        await harness.click_button("#pause_button")
        assert not dolphie.pause_refresh
        resumed_from = replay_manager.current_replay_id
        await harness.wait_for(
            lambda: replay_manager.current_replay_id > resumed_from, message="replay to advance after resume"
        )

        assert not dolphie.main_db_connection.is_connected()


async def test_live_recording_round_trip(server: Server, tmp_path: Path) -> None:
    """Record from the TUI, then play the file back and find the same metric history."""
    async with run_dolphie(make_config(server, tmp_path, record_for_replay=True)) as harness:
        await harness.wait_for_polls(5)
        assert harness.notifications_with("Recording data")
        replay_file = Path(harness.replay_manager.replay_file)
        polls = harness.poll_count

    assert replay_row_count(replay_file) >= 5

    config = make_config(server, tmp_path, replay_file=str(replay_file), replay_dir=None)
    async with run_dolphie(config) as harness:
        await harness.wait_for_replay_frame()
        replay_manager = harness.replay_manager
        await harness.wait_for(
            lambda: replay_manager.current_replay_id >= replay_manager.max_replay_id,
            message="replay to reach the last frame",
        )
        assert replay_manager.max_replay_id >= polls
        assert replay_manager.min_replay_timestamp is not None
        assert replay_manager.max_replay_timestamp is not None
        assert replay_manager.min_replay_timestamp < replay_manager.max_replay_timestamp
        # Live recordings store the full history, so the graph has every recorded point.
        assert harness.poll_count >= polls


async def test_proxysql_daemon_recording_plays_back_in_the_tui(proxysql_server: Server, tmp_path: Path) -> None:
    """Daemon mode against ProxySQL records its own panels, and playback restores the ProxySQL source."""
    with frontend_traffic():
        replay_file = run_daemon(
            proxysql_server, tmp_path, "--daemon-panels", "processlist,proxysql_hostgroup_summary", min_rows=8
        )

    log = (tmp_path / "daemon.log").read_text()
    assert "ERROR" not in log, log
    assert "CRITICAL" not in log, log
    with sqlite3.connect(replay_file) as connection:
        metadata = connection.execute("SELECT host, port, host_distro, connection_source FROM metadata").fetchone()
    assert metadata == (
        proxysql_server.host,
        proxysql_server.port,
        ConnectionSource.proxysql,
        ConnectionSource.proxysql,
    )

    config = make_config(
        proxysql_server, tmp_path, host="replay-ignored", port=1, replay_file=str(replay_file), replay_dir=None
    )
    async with run_dolphie(config) as harness:
        await harness.wait_for_replay_frame()
        replay_manager = harness.replay_manager
        dolphie = harness.dolphie

        assert dolphie.connection_source == ConnectionSource.proxysql
        assert dolphie.host == proxysql_server.host
        assert dolphie.port == proxysql_server.port
        assert "Queries" in dolphie.global_status
        assert replay_manager.max_replay_id >= 8

        await harness.click_button("#pause_button")
        current = replay_manager.current_replay_id
        await harness.press("right_square_bracket")
        await harness.wait_for(lambda: replay_manager.current_replay_id == current + 1, message="step forward")

        # Hostgroup Summary is the one ProxySQL panel a replay can show. Query Rules and Command Stats
        # are live-only, so CommandManager's proxysql_replay catalog ignores their keys.
        for key in ("4", "5", "6"):
            await harness.press(key)
        assert dolphie.panels.proxysql_hostgroup_summary.visible
        assert not dolphie.panels.proxysql_mysql_query_rules.visible
        assert not dolphie.panels.proxysql_command_stats.visible
        await harness.press("right_square_bracket")
        await harness.wait_for(lambda: replay_manager.current_replay_id == current + 2, message="step forward")
        assert any(row.get("srv_host") == "mysql84" for row in dolphie.proxysql_hostgroup_summary)
        assert harness.tab.proxysql_hostgroup_summary_datatable.row_count >= 1
        assert any(row.get("Command") == "SELECT" for row in dolphie.proxysql_command_stats)
        assert not dolphie.main_db_connection.is_connected()


async def test_proxysql_live_recording_round_trip(proxysql_server: Server, tmp_path: Path) -> None:
    async with run_dolphie(make_config(proxysql_server, tmp_path, record_for_replay=True)) as harness:
        await harness.wait_for_polls(5)
        assert harness.notifications_with("Recording data")
        replay_file = Path(harness.replay_manager.replay_file)
        polls = harness.poll_count

    assert replay_row_count(replay_file) >= 5

    config = make_config(
        proxysql_server, tmp_path, host="replay-ignored", port=1, replay_file=str(replay_file), replay_dir=None
    )
    async with run_dolphie(config) as harness:
        await harness.wait_for_replay_frame()
        replay_manager = harness.replay_manager
        assert harness.dolphie.connection_source == ConnectionSource.proxysql
        assert harness.dolphie.host == proxysql_server.host
        await harness.wait_for(
            lambda: replay_manager.current_replay_id >= replay_manager.max_replay_id,
            message="replay to reach the last frame",
        )
        assert replay_manager.max_replay_id >= polls
        assert harness.poll_count >= polls
