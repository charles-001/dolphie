import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import cast
from unittest.mock import patch

from rich.syntax import Syntax
from rich.text import Text
from textual.app import App, ComposeResult
from textual.containers import Horizontal
from textual.widgets import Button, Footer, RadioButton, Tabs

from dolphie.App import DolphieApp
from dolphie.DataTypes import ConnectionStatus, DatabaseRow
from dolphie.Modules.ArgumentParser import Config
from dolphie.Modules.TabManager import Tab, TabManager
from dolphie.Modules.Theme import DARK_GRAY, DOLPHIE_THEME, RED, YELLOW, ThemedDataTable
from dolphie.Widgets.CommandScreen import CommandScreen
from dolphie.Widgets.DolphieScreen import ScreenContext
from dolphie.Widgets.EventLogScreen import EventLog
from dolphie.Widgets.MetricGraphDashboard import MetricGraphDashboard
from dolphie.Widgets.MetricSeriesControl import MetricSeriesControl
from dolphie.Widgets.TabSetupModal import TabSetupModal
from dolphie.Widgets.ThreadScreen import ThreadScreen
from dolphie.Widgets.TopBar import TopBar

CSS_PATH = Path(__file__).parents[2] / "dolphie" / "Dolphie.tcss"
CONTEXT = ScreenContext("CONNECTED", "test", "db.example:3306")


class ScreenTestApp(App):
    CSS_PATH = CSS_PATH

    def __init__(self, screen):
        super().__init__()
        self.register_theme(DOLPHIE_THEME)
        self.theme = DOLPHIE_THEME.name
        self.test_screen = screen

    def on_mount(self):
        self.push_screen(self.test_screen)


class MainChromeTestApp(DolphieApp):
    CSS_PATH = CSS_PATH

    async def on_mount(self) -> None:
        pass


class DaemonLifecycleTestApp(DolphieApp):
    CSS_PATH = CSS_PATH

    async def on_mount(self) -> None:
        self.tab_manager = TabManager(app=self, config=self.config)
        await self.tab_manager.create_ui_widgets()
        await self.tab_manager.create_tab(tab_name="daemon")


class GraphDashboardLifecycleTestApp(App[None]):
    CSS_PATH = CSS_PATH

    def __init__(self) -> None:
        super().__init__()
        self.config = Config(app_version="test")
        self.register_theme(DOLPHIE_THEME)
        self.theme = DOLPHIE_THEME.name

    def compose(self) -> ComposeResult:
        yield TopBar(host="", app_version="test", help="")
        yield Tabs(id="host_tabs")

    async def on_mount(self) -> None:
        self.tab_manager = TabManager(app=cast(DolphieApp, self), config=self.config)
        await self.tab_manager.create_ui_widgets()


class FakeDatabase:
    def execute(self, query: str) -> int:
        return 0

    def fetchall(self) -> list[DatabaseRow]:
        return []


def test_main_screen_keeps_help_in_topbar_without_footer():
    async def run_test():
        app = MainChromeTestApp(Config(app_version="test"))

        async with app.run_test(size=(100, 30)) as pilot:
            await pilot.pause()

            assert app.query_one(Tabs)
            assert not app.query(Footer)
            assert str(app.query_one("#topbar_help").render()) == "press ? for help"

            topbar = app.query_one(TopBar)
            topbar.host = "db.example:3306"
            topbar.connection_status = ConnectionStatus.connecting
            await pilot.pause()
            assert str(topbar.topbar_host.render()) == "[CONNECTING] db.example:3306"

            topbar.connection_status = ConnectionStatus.connected
            await pilot.pause()
            assert str(topbar.topbar_host.render()) == "[CONNECTED] db.example:3306"

    asyncio.run(run_test())


def test_daemon_initializes_without_graph_dashboard():
    async def run_test():
        app = DaemonLifecycleTestApp(Config(app_version="test", daemon_mode=True))

        async with app.run_test(size=(100, 30)) as pilot:
            await pilot.pause()

            assert app.tab_manager.metric_graph_dashboard is None
            assert app.tab_manager.active_tab is not None

    asyncio.run(run_test())


def test_graph_dashboard_mounts_without_dynamic_tab_graph_attributes():
    async def run_test():
        app = GraphDashboardLifecycleTestApp()

        async with app.run_test(size=(120, 50)) as pilot:
            await pilot.pause()

            dashboard = app.query_one(MetricGraphDashboard)
            assert dashboard.graphs
            assert dashboard.controls
            assert app.query(MetricSeriesControl)
            assert not hasattr(Tab(id="test", name="test"), "graph_system_cpu")

    asyncio.run(run_test())


def test_command_screen_uses_shared_topbar_and_footer():
    async def run_test():
        app = ScreenTestApp(CommandScreen(CONTEXT, "command output"))

        async with app.run_test(size=(100, 30)) as pilot:
            await pilot.pause()

            topbar = app.screen.query_one(TopBar)
            footer = app.screen.query_one(Footer)

            assert str(topbar.topbar_host.render()) == "[CONNECTED] db.example:3306"
            assert str(topbar.topbar_help.render()) == ""
            assert footer.show_command_palette is False
            assert footer.styles.background.hex == "#192036"
            assert "q" in app.screen.active_bindings

            await pilot.press("q")
            await pilot.pause()

            assert not isinstance(app.screen, CommandScreen)

    asyncio.run(run_test())


def test_event_log_moves_inline_help_to_contextual_footer():
    async def run_test():
        event_log = EventLog(CONTEXT, FakeDatabase())
        app = ScreenTestApp(event_log)

        async with app.run_test(size=(120, 40)) as pilot:
            await pilot.pause()

            footer = app.screen.query_one(Footer)
            assert not app.screen.query("#help")
            assert {"q", "r", "1", "2"} <= app.screen.active_bindings.keys()
            assert app.screen.query_one(".switch_container", Horizontal).styles.margin.top == 1

            event_log._prepare_datatable()
            event_log._populate_datatable(
                3,
                [
                    {
                        "timestamp": datetime(2026, 7, 14, 1, 2, 3, tzinfo=timezone.utc),
                        "subsystem": "Server",
                        "level": level,
                        "error_code": 100,
                        "message": "Event message",
                    }
                    for level in ("Error", "Warning", "Note")
                ],
            )
            datatable = event_log.query_one(ThemedDataTable)
            for row_index, expected_color in enumerate((RED, YELLOW, DARK_GRAY)):
                timestamp, _, level, error_code, _ = datatable.get_row_at(row_index)
                assert isinstance(timestamp, Text)
                assert timestamp.plain == "2026-07-14 01:02:03"
                assert str(timestamp.spans[0].style) == DARK_GRAY
                assert isinstance(level, Text)
                assert str(level.spans[0].style) == expected_color
                assert isinstance(error_code, Text)

            app.screen.query_one("#search").focus()
            await pilot.pause()
            footer_text = " ".join(str(child.render()) for child in footer.query("*"))
            assert "Apply" in footer_text

    asyncio.run(run_test())


def test_thread_screen_exposes_copy_actions_instead_of_buttons():
    async def run_test():
        query = Syntax("SELECT 1", "sql")
        screen = ThreadScreen(
            CONTEXT,
            thread_table="thread details",
            user_thread_attributes_table="",
            query=query,
            explain_data=[{"id": 1, "select_type": "SIMPLE", "table": "events", "Extra": "Using where"}],
            explain_json_data='{"plan": 1}',
            explain_failure="",
            transaction_history_table="",
        )
        app = ScreenTestApp(screen)

        async with app.run_test(size=(120, 40)) as pilot:
            await pilot.pause()

            assert not app.screen.query(Button)
            assert {"q", "c", "j"} <= app.screen.active_bindings.keys()
            assert app.screen.query_one(Footer).size.width == app.size.width
            explain_table = app.screen.query_one("#explain_table", ThemedDataTable)
            for label, column in zip(
                ("id", "select_type", "table", "Extra"),
                explain_table.columns.values(),
                strict=True,
            ):
                assert column.content_width >= len(label)

            with patch.object(app, "copy_to_clipboard") as copy_to_clipboard:
                await pilot.press("c")
                copy_to_clipboard.assert_called_once_with("SELECT 1")

                copy_to_clipboard.reset_mock()
                await pilot.press("j")
                copy_to_clipboard.assert_called_once_with('{"plan": 1}')

    asyncio.run(run_test())


def test_tab_setup_defaults_unknown_ssl_configuration_to_required():
    async def run_test():
        screen = TabSetupModal(
            credential_profile=None,
            credential_profiles={},
            host=None,
            port=None,
            username=None,
            password=None,
            ssl={"ca": "ca.pem"},
            socket_file=None,
            available_hosts=[],
            hostgroups=[],
            replay_files=[],
            replay_directory=None,
            record_for_replay=False,
        )
        app = ScreenTestApp(screen)

        async with app.run_test(size=(100, 50)) as pilot:
            await pilot.pause()

            assert app.screen.query_one("#REQUIRED", RadioButton).value is True

    asyncio.run(run_test())
