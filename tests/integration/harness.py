from __future__ import annotations

import asyncio
import functools
import os
import threading
import time
import traceback
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pymysql
import pymysql.cursors
from loguru import logger
from textual import events
from textual.app import App
from textual.keys import key_to_character
from textual.notifications import SeverityLevel
from textual.pilot import Pilot
from textual.widgets import Button

from dolphie.App import DolphieApp
from dolphie.Dolphie import Dolphie
from dolphie.Modules.ArgumentParser import Config
from dolphie.Modules.ReplayManager import ReplayManager
from dolphie.Modules.TabManager import Tab
from tests.integration.servers import PROXYSQL_FRONTEND, Server

DEFAULT_TIMEOUT = 60.0
# An unreachable local port makes the PyPI version check fail instantly.
UNREACHABLE_PYPI = "http://127.0.0.1:1/"
# Committed daemon recordings, eight frames each, two seconds apart
REPLAYS = Path(__file__).parents[1] / "dolphie" / "replays"

# Worker failures and replay write errors are logged rather than raised, and the TUI installs no
# log sink of its own. One process-wide sink collects them; each app remembers where its own start.
LOG_ERRORS: list[str] = []
logger.add(lambda message: LOG_ERRORS.append(str(message)), level="ERROR", format="{level}: {message}")


@dataclass
class Notification:
    title: str
    message: str
    severity: SeverityLevel


class HarnessApp(DolphieApp):
    """The real app, with notifications and unhandled errors captured instead of shown."""

    SHOW_TOASTS = True

    def __init__(self, config: Config) -> None:
        super().__init__(config)
        self.notifications: list[Notification] = []
        self.errors: list[str] = []
        self._log_errors_start = len(LOG_ERRORS)

    @property
    def log_errors(self) -> list[str]:
        return LOG_ERRORS[self._log_errors_start :]

    def notify(
        self,
        message: str,
        *,
        title: str = "",
        severity: SeverityLevel = "information",
        timeout: float | None = None,
        markup: bool = True,
    ) -> None:
        self.notifications.append(Notification(title, message, severity))
        if self.SHOW_TOASTS:
            super().notify(message, title=title, severity=severity, timeout=timeout, markup=markup)

    def _handle_exception(self, error: Exception) -> None:
        cause = error.__cause__ or error.__context__ or error
        self.errors.append("".join(traceback.format_exception(type(cause), cause, cause.__traceback__)))
        # Textual's handler stores the error so run_test() re-raises it when the app exits.
        App._handle_exception(self, error)


class DolphieHarness:
    def __init__(self, app: HarnessApp, pilot: Pilot[Any]) -> None:
        self.app = app
        self.pilot = pilot

    @property
    def tab(self) -> Tab:
        tab = self.app.tab_manager.active_tab
        assert tab is not None, "no active tab"
        return tab

    @property
    def dolphie(self) -> Dolphie:
        return self.tab.dolphie

    @property
    def replay_manager(self) -> ReplayManager:
        replay_manager = self.tab.replay_manager
        assert replay_manager is not None, "no replay manager on the active tab"
        return replay_manager

    @property
    def poll_count(self) -> int:
        tab = self.app.tab_manager.active_tab
        return 0 if tab is None else len(tab.dolphie.metric_manager.snapshot_datetimes())

    @property
    def notifications(self) -> list[Notification]:
        return self.app.notifications

    @property
    def error_notifications(self) -> list[Notification]:
        return [n for n in self.notifications if n.severity == "error"]

    def notifications_with(self, text: str) -> list[Notification]:
        return [n for n in self.notifications if text in n.message or text in n.title]

    def fail_on_errors(self) -> None:
        """Fail on anything Dolphie treats as an error, however it chose to report it."""
        if self.app.errors:
            raise AssertionError("Dolphie raised inside the app:\n" + "\n".join(self.app.errors))
        if self.app.log_errors:
            raise AssertionError("Dolphie logged an error:\n" + "".join(self.app.log_errors))
        if self.error_notifications:
            raise AssertionError(f"Dolphie showed an error notification: {self.error_notifications}")

    async def wait_for(
        self,
        predicate: Callable[[], bool],
        *,
        timeout: float = DEFAULT_TIMEOUT,
        message: str = "condition",
        expect_errors: bool = False,
    ) -> None:
        """Poll ``predicate``. With ``expect_errors`` only a crash fails the wait, not a reported error."""
        deadline = time.monotonic() + timeout
        while True:
            if expect_errors:
                assert self.app.errors == [], "Dolphie raised inside the app:\n" + "\n".join(self.app.errors)
            else:
                self.fail_on_errors()
            if predicate():
                return
            if time.monotonic() > deadline:
                raise AssertionError(f"Timed out after {timeout:.0f}s waiting for {message}")
            # A plain sleep lets the app's message loop run. Pilot.pause also waits for the whole UI
            # to settle, which is slow while a spinner or loading indicator animates.
            await asyncio.sleep(0.05)

    async def wait_for_polls(self, count: int) -> None:
        """Wait until the active tab has completed at least ``count`` polls with metric history."""
        await self.wait_for(lambda: self.poll_count >= count, message=f"{count} polls")

    async def wait_for_replay_frame(self) -> None:
        """Wait until replay playback has applied at least one frame to the tab."""

        def applied() -> bool:
            tab = self.app.tab_manager.active_tab
            if tab is None or tab.replay_manager is None:
                return False
            return tab.replay_manager.max_replay_id >= 1 and bool(tab.dolphie.global_status)

        await self.wait_for(applied, message="first replay frame")

    async def wait_for_worker_idle(self) -> None:
        """Wait until the active tab's worker finished. A replay step while one runs is dropped."""
        tab = self.tab
        await self.wait_for(lambda: tab.worker is None or not tab.worker.is_running, message="worker")

    async def press(self, *keys: str) -> None:
        """Press keys and wait until the app has processed them.

        Pilot.press and Pilot.pause() wait for the process to go CPU-idle after every key, up to
        one second twice over. Dolphie polls and renders continuously, so that wait always hits
        the cap. Posting the key event directly and pausing with an explicit delay drains the
        screen's message queues over the same delivery path without the idle heuristic.
        """
        for key in keys:
            self.app.post_message(events.Key(key, key_to_character(key)))
            await self.pilot.pause(0)
        await self._settle()

    async def click_button(self, selector: str) -> None:
        """Activate a Button the way a click does, through its Pressed message."""
        self.app.query_one(selector, Button).press()
        await self._settle()

    async def _settle(self) -> None:
        await asyncio.sleep(0.05)
        await self.pilot.pause(0)
        self.fail_on_errors()

    async def open_command_screen(self, key: str) -> None:
        """Press a display command key and require that its SQL ran and a screen was pushed."""
        await self.press(key)
        await self.wait_for(
            lambda: len(self.app.screen_stack) > 1 or bool(self.notifications_with("Error running command")),
            message=f"screen for command {key}",
        )
        failures = self.notifications_with("Error running command")
        assert failures == [], (key, failures)
        assert len(self.app.screen_stack) > 1, key

    async def next_poll(self) -> None:
        """Wait for one more main worker cycle to complete."""
        before = self.poll_count
        await self.wait_for(lambda: self.poll_count > before, message="next poll")

    async def run_for(self, seconds: float) -> int:
        """Let the app keep polling for ``seconds``, failing fast on any error. Returns the polls completed."""
        before = self.poll_count
        deadline = time.monotonic() + seconds
        await self.wait_for(lambda: time.monotonic() >= deadline, timeout=seconds + 1, message="the clock")
        return self.poll_count - before


def shared_options(tmp_path: Path) -> dict[str, Any]:
    """Config values every test run shares, whether passed as a Config or written to a config file."""
    return {
        # Half-second polls keep every wait short. The CLI accepts fractional intervals.
        "refresh_interval": 0.5,
        "pypi_repository": UNREACHABLE_PYPI,
        "host_cache_file": str(tmp_path / "host_cache"),
        "tab_setup_file": str(tmp_path / "tab_setup_hosts"),
        "replay_dir": str(tmp_path / "replays"),
        "daemon_mode_log_file": str(tmp_path / "daemon.log"),
    }


def make_config(server: Server, tmp_path: Path, **overrides: Any) -> Config:
    """A Config that talks to ``server`` and writes every side file under ``tmp_path``."""
    values: dict[str, Any] = {
        "host": server.host,
        "port": server.port,
        "user": server.user,
        "password": server.password,
        **shared_options(tmp_path),
        **overrides,
    }
    return Config(app_version="integration", **values)


def replay_config(replay_file: Path, **overrides: Any) -> Config:
    """A Config that plays ``replay_file`` back with no server."""
    values: dict[str, Any] = {
        "app_version": "test",
        "replay_file": str(replay_file),
        "pypi_repository": UNREACHABLE_PYPI,
        "refresh_interval": 0.2,
        # The default is ~/dolphie_host_cache, and an entry there would rename hosts in the render
        "host_cache_file": os.devnull,
        **overrides,
    }
    return Config(**values)


def playback_config(server: Server, tmp_path: Path, replay_file: Path) -> Config:
    """A Config that plays back a file recorded from ``server``. The file's metadata wins over the host."""
    return make_config(server, tmp_path, host="replay-ignored", port=1, replay_file=str(replay_file), replay_dir=None)


@asynccontextmanager
async def run_dolphie(config: Config) -> AsyncIterator[DolphieHarness]:
    app = HarnessApp(config)
    async with app.run_test(size=(220, 60)) as pilot:
        harness = DolphieHarness(app, pilot)
        yield harness
        harness.fail_on_errors()


def connect(server: Server, database: str | None = None) -> pymysql.Connection[pymysql.cursors.DictCursor]:
    """A direct client connection for inducing conditions the panels must then show."""
    return pymysql.connect(
        host=server.host,
        port=server.port,
        user=server.user,
        password=server.password,
        database=database,
        autocommit=True,
        connect_timeout=5,
        cursorclass=pymysql.cursors.DictCursor,
    )


def query(server: Server, sql: str, database: str | None = None) -> list[dict[str, Any]]:
    with connect(server, database) as connection, connection.cursor() as cursor:
        cursor.execute(sql)
        return list(cursor.fetchall())


@contextmanager
def traffic(server: Server, sql: str, database: str | None = None) -> Iterator[None]:
    """Repeat ``sql`` on its own connection in a thread, so the app's event loop never blocks on it."""
    stop = threading.Event()
    failure: list[pymysql.Error] = []

    def run() -> None:
        try:
            with connect(server, database) as connection, connection.cursor() as cursor:
                while not stop.wait(0.2):
                    cursor.execute(sql)
                    cursor.fetchall()
        except pymysql.Error as error:
            failure.append(error)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join(timeout=5)
    if failure:
        raise failure[0]


# Queries through the frontend land on the mysql84 backend and move the ProxySQL counters.
frontend_traffic = functools.partial(traffic, PROXYSQL_FRONTEND, "SELECT 1")
