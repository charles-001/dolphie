"""Every MySQL and MariaDB panel and display command, rendered by the real app against a live server."""

from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pymysql

from dolphie.DataTypes import ConnectionSource
from tests.integration.harness import DolphieHarness, connect, make_config, query, run_dolphie, traffic
from tests.integration.servers import Server

SLEEP_MARKER = "dolphie_integration_sleep"


@contextmanager
def long_running_query(server: Server, seconds: int = 60) -> Iterator[None]:
    """Hold one `SELECT SLEEP()` open on its own connection so the processlist has a live row."""
    connection = connect(server)

    def sleep() -> None:
        try:
            connection.cursor().execute(f"SELECT SLEEP({seconds}) AS {SLEEP_MARKER}")
        except pymysql.Error:
            # The KILL below interrupts the statement on purpose.
            pass

    thread = threading.Thread(target=sleep, daemon=True)
    thread.start()
    try:
        yield
    finally:
        # Killing the sleeping thread from another connection ends the query immediately.
        # KILL fails with "Unknown thread id" if the sleep already ran to completion.
        try:
            query(server, f"KILL {connection.thread_id()}")
        except pymysql.Error:
            pass
        thread.join(timeout=5)
        connection.close()


def has_sleep_thread(harness: DolphieHarness) -> bool:
    return any(SLEEP_MARKER in thread.formatted_query.code for thread in harness.dolphie.processlist_threads.values())


async def test_connects_and_identifies_the_server(server: Server, tmp_path: Path) -> None:
    async with run_dolphie(make_config(server, tmp_path)) as harness:
        await harness.wait_for_polls(2)
        dolphie = harness.dolphie

        assert dolphie.connection_source == ConnectionSource.mysql
        assert dolphie.host_version is not None
        assert dolphie.host_version.startswith(server.version)
        assert dolphie.connection_source_alt == server.flavor
        assert dolphie.host_distro == server.distro
        assert dolphie.performance_schema_enabled
        assert dolphie.global_status["Uptime"]
        assert dolphie.binlog_status.get("File")


async def test_runs_with_every_panel_open(server: Server, tmp_path: Path) -> None:
    """The smoke test: five seconds of polling with every panel showing, so each render path runs every poll."""
    async with run_dolphie(make_config(server, tmp_path)) as harness:
        await harness.wait_for_polls(1)
        # Metadata Locks and DDL stay hidden with a notification when the server lacks their instruments.
        await harness.press("3", "5", "6", "7", "8")
        assert harness.dolphie.panels.graphs.visible
        assert harness.dolphie.panels.pfs_metrics.visible
        assert harness.dolphie.panels.statements_summary.visible

        assert await harness.run_for(5) >= 5


async def test_processlist_shows_a_live_query(server: Server, tmp_path: Path) -> None:
    async with run_dolphie(make_config(server, tmp_path)) as harness:
        await harness.wait_for_polls(1)
        with long_running_query(server):
            await harness.wait_for(lambda: has_sleep_thread(harness), message="sleep thread in processlist")
            await harness.next_poll()
            assert harness.tab.processlist_datatable.row_count >= 1
            assert harness.dolphie.panels.dashboard.visible
            assert harness.tab.panel_dashboard.display

        await harness.wait_for(lambda: not has_sleep_thread(harness), message="sleep thread to disappear")


async def test_graph_panel_accumulates_history(server: Server, tmp_path: Path) -> None:
    async with run_dolphie(make_config(server, tmp_path)) as harness:
        await harness.wait_for_polls(1)
        await harness.press("3")
        assert harness.dolphie.panels.graphs.visible
        assert harness.tab.panel_graphs.display

        await harness.wait_for_polls(3)
        values = harness.dolphie.metric_manager.metrics.dml.Queries.values_snapshot()
        assert len(values) >= 2
        assert all(value >= 0 for value in values)

        await harness.press("3")
        assert not harness.dolphie.panels.graphs.visible


async def test_metadata_locks_panel(server: Server, tmp_path: Path) -> None:
    database = "dolphie_it_locks"
    query(server, f"CREATE DATABASE IF NOT EXISTS {database}")
    query(server, f"CREATE TABLE IF NOT EXISTS {database}.locked (id INT PRIMARY KEY)")

    try:
        async with run_dolphie(make_config(server, tmp_path)) as harness:
            await harness.wait_for_polls(1)
            await harness.press("5")

            if not harness.dolphie.metadata_locks_enabled:
                assert harness.notifications_with("Metadata Locks panel requires")
                return

            assert harness.dolphie.panels.metadata_locks.visible
            with connect(server) as holder:
                holder.cursor().execute(f"LOCK TABLES {database}.locked WRITE")
                await harness.wait_for(
                    lambda: any(lock.get("OBJECT_NAME") == "locked" for lock in harness.dolphie.metadata_locks),
                    message="metadata lock row",
                )
                await harness.next_poll()
                assert harness.tab.metadata_locks_datatable.row_count >= 1
    finally:
        query(server, f"DROP DATABASE IF EXISTS {database}")


async def test_ddl_panel_toggles_when_stage_instruments_are_enabled(server: Server, tmp_path: Path) -> None:
    async with run_dolphie(make_config(server, tmp_path)) as harness:
        await harness.wait_for_polls(1)
        await harness.press("6")

        if harness.notifications_with("DDL panel requires"):
            assert not harness.dolphie.panels.ddl.visible
            return

        assert harness.dolphie.panels.ddl.visible
        await harness.next_poll()
        assert harness.dolphie.ddl == [] or all("processlist_id" in row for row in harness.dolphie.ddl)


async def test_performance_schema_metrics_panel(server: Server, tmp_path: Path) -> None:
    database = "dolphie_it_pfs"
    query(server, f"CREATE DATABASE IF NOT EXISTS {database}")
    query(server, f"CREATE TABLE {database}.io (id INT PRIMARY KEY AUTO_INCREMENT, v CHAR(36))")

    try:
        async with run_dolphie(make_config(server, tmp_path)) as harness:
            await harness.wait_for_polls(1)
            await harness.press("7")
            assert harness.dolphie.panels.pfs_metrics.visible

            # Rows appear only for instances whose counters moved between two polls, so keep writing.
            # MySQL 9.7 has no MD5() and needs a default database for a VALUES insert.
            with traffic(server, "INSERT INTO io (v) VALUES " + ", ".join(["(UUID())"] * 50), database):
                await harness.wait_for(
                    lambda: (
                        harness.dolphie.file_io_data is not None and bool(harness.dolphie.file_io_data.filtered_data)
                    ),
                    message="file io deltas",
                )
                await harness.next_poll()
                assert harness.tab.pfs_metrics_file_io_datatable.row_count >= 1
    finally:
        query(server, f"DROP DATABASE IF EXISTS {database}")


async def test_statements_summary_panel(server: Server, tmp_path: Path) -> None:
    async with run_dolphie(make_config(server, tmp_path)) as harness:
        await harness.wait_for_polls(1)
        await harness.press("8")
        assert harness.dolphie.panels.statements_summary.visible

        # Rows appear for digests whose counters moved between two polls, so keep one query running.
        with traffic(server, "SELECT COUNT(*) AS n FROM information_schema.tables WHERE table_name LIKE 'dolphie%'"):
            await harness.wait_for(
                lambda: (
                    harness.dolphie.statements_summary_data is not None
                    and bool(harness.dolphie.statements_summary_data.filtered_data)
                ),
                message="statement digest deltas",
            )
            await harness.next_poll()
            assert harness.tab.statements_summary_datatable.row_count >= 1


async def test_replication_panel_on_a_standalone_reports_no_replication(server: Server, tmp_path: Path) -> None:
    async with run_dolphie(make_config(server, tmp_path)) as harness:
        await harness.wait_for_polls(2)
        await harness.press("4")
        assert not harness.dolphie.panels.replication.visible
        assert harness.notifications_with("Replication panel has no data")


async def test_display_commands_open_a_screen(server: Server, tmp_path: Path) -> None:
    """Each display command runs its SQL on the secondary connection and pushes a screen."""

    async with run_dolphie(make_config(server, tmp_path)) as harness:
        await harness.wait_for_polls(2)
        dolphie = harness.dolphie
        # The error log screen reads performance_schema.error_log, which only MySQL 8.0+ has.
        has_error_log = dolphie.connection_source_alt == ConnectionSource.mysql and dolphie.is_mysql_version_at_least(
            "8.0"
        )
        for key in ("d", "o", "u", "Z", *(["e"] if has_error_log else [])):
            await harness.open_command_screen(key)
            await harness.press("escape")
            assert len(harness.app.screen_stack) == 1, key

        if not has_error_log:
            before = len(harness.notifications)
            await harness.press("e")
            await harness.wait_for(lambda: len(harness.notifications) > before, message="error log refusal")
            assert len(harness.app.screen_stack) == 1
            # MariaDB gets "only available for MySQL connections", MySQL 5.7 gets "requires MySQL 8+".
            refused = harness.notifications[before:]
            assert any("Error log command" in n.message or "only available" in n.message for n in refused)


async def test_processlist_toggles_and_filters(server: Server, tmp_path: Path) -> None:
    async with run_dolphie(make_config(server, tmp_path)) as harness:
        await harness.wait_for_polls(1)
        with long_running_query(server):
            await harness.wait_for(lambda: has_sleep_thread(harness), message="sleep thread")

            # Idle threads: the holder connection is asleep between statements, so more rows appear.
            await harness.press("i")
            assert harness.dolphie.show_idle_threads
            await harness.next_poll()
            await harness.press("i")
            assert not harness.dolphie.show_idle_threads

            await harness.press("a")
            assert harness.dolphie.show_additional_query_columns
            await harness.press("s")
            assert not harness.dolphie.sort_by_time_descending

            await harness.press("p")
            assert harness.dolphie.pause_refresh
            await harness.press("p")
            assert not harness.dolphie.pause_refresh
            await harness.next_poll()
