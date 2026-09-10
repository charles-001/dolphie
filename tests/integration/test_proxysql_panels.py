"""ProxySQL detection and every ProxySQL panel, rendered against a live ProxySQL admin interface."""

from __future__ import annotations

from pathlib import Path

from dolphie.DataTypes import ConnectionSource
from tests.integration.harness import frontend_traffic, make_config, run_dolphie
from tests.integration.servers import Server


async def test_detects_proxysql_from_the_admin_port(proxysql_server: Server, tmp_path: Path) -> None:
    async with run_dolphie(make_config(proxysql_server, tmp_path)) as harness:
        await harness.wait_for_polls(2)
        dolphie = harness.dolphie

        assert dolphie.connection_source == ConnectionSource.proxysql
        assert dolphie.host_distro == ConnectionSource.proxysql
        assert dolphie.host_version is not None
        assert dolphie.host_version.startswith(proxysql_server.version)
        assert "Queries" in dolphie.global_status
        with frontend_traffic():
            await harness.wait_for(lambda: bool(dolphie.proxysql_command_stats), message="command stats")


async def test_runs_with_every_panel_open(proxysql_server: Server, tmp_path: Path) -> None:
    """The smoke test: five seconds of polling under traffic with every panel showing."""
    with frontend_traffic():
        async with run_dolphie(make_config(proxysql_server, tmp_path)) as harness:
            await harness.wait_for_polls(1)
            await harness.press("3", "4", "5", "6")
            for panel in (
                "graphs",
                "proxysql_hostgroup_summary",
                "proxysql_mysql_query_rules",
                "proxysql_command_stats",
            ):
                assert getattr(harness.dolphie.panels, panel).visible, panel

            # The MySQL-only panel keys are refused
            await harness.press("7", "8")
            assert not harness.dolphie.panels.pfs_metrics.visible
            assert not harness.dolphie.panels.statements_summary.visible

            assert await harness.run_for(5) >= 5


async def test_hostgroup_summary_panel_shows_the_backend(proxysql_server: Server, tmp_path: Path) -> None:
    async with run_dolphie(make_config(proxysql_server, tmp_path)) as harness:
        await harness.wait_for_polls(1)
        await harness.press("4")
        assert harness.dolphie.panels.proxysql_hostgroup_summary.visible

        with frontend_traffic():
            await harness.wait_for(
                lambda: any(row.get("srv_host") == "mysql84" for row in harness.dolphie.proxysql_hostgroup_summary),
                message="mysql84 backend in hostgroup summary",
            )
            await harness.next_poll()
            assert harness.tab.proxysql_hostgroup_summary_datatable.row_count >= 1

            await harness.wait_for(
                lambda: harness.dolphie.metric_manager.metrics.dml.Queries.latest_value() not in (None, 0),
                message="frontend traffic in the Queries metric",
            )


async def test_query_rules_panel_lists_configured_rules(proxysql_server: Server, tmp_path: Path) -> None:
    async with run_dolphie(make_config(proxysql_server, tmp_path)) as harness:
        await harness.wait_for_polls(1)
        await harness.press("5")
        assert harness.dolphie.panels.proxysql_mysql_query_rules.visible

        with frontend_traffic():
            await harness.wait_for(lambda: len(harness.dolphie.proxysql_mysql_query_rules) >= 2, message="query rules")
            await harness.next_poll()
            assert harness.tab.proxysql_mysql_query_rules_datatable.row_count >= 2


async def test_command_stats_panel(proxysql_server: Server, tmp_path: Path) -> None:
    async with run_dolphie(make_config(proxysql_server, tmp_path)) as harness:
        await harness.wait_for_polls(1)
        await harness.press("6")
        assert harness.dolphie.panels.proxysql_command_stats.visible

        with frontend_traffic():
            await harness.wait_for_polls(3)
            assert harness.tab.proxysql_command_stats_datatable.row_count >= 1
            select_rows = [r for r in harness.dolphie.proxysql_command_stats if r.get("Command") == "SELECT"]
            assert select_rows
            assert int(select_rows[0]["Total_cnt"]) > 0


async def test_processlist_shows_frontend_sessions(proxysql_server: Server, tmp_path: Path) -> None:
    async with run_dolphie(make_config(proxysql_server, tmp_path)) as harness:
        await harness.wait_for_polls(1)
        await harness.press("i")
        assert harness.dolphie.show_idle_threads

        with frontend_traffic():
            await harness.wait_for(lambda: len(harness.dolphie.processlist_threads) >= 1, message="frontend session")
            await harness.next_poll()
            assert harness.tab.processlist_datatable.row_count >= 1


async def test_display_commands_open_a_screen(proxysql_server: Server, tmp_path: Path) -> None:
    async with run_dolphie(make_config(proxysql_server, tmp_path)) as harness:
        await harness.wait_for_polls(2)
        with frontend_traffic():
            for key in ("u", "e"):
                await harness.open_command_screen(key)
                await harness.press("escape")
                assert len(harness.app.screen_stack) == 1, key
