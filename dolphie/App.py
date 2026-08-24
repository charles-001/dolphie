#!/usr/bin/env python3
"""Dolphie - Your single pane of glass for real-time analytics into MySQL/MariaDB & ProxySQL.

Author: Charles Thompson
License: GPL-3.0
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from importlib import metadata

import requests
from loguru import logger
from packaging.version import parse as parse_version
from rich.emoji import Emoji
from rich.traceback import Traceback
from textual import events, on, work
from textual.app import App
from textual.binding import Binding
from textual.widgets import Footer, RadioSet, Tabs
from textual.worker import Worker

from dolphie.DataTypes import ConnectionSource, ConnectionStatus, HotkeyCommands
from dolphie.Modules.ArgumentParser import ArgumentParser, Config
from dolphie.Modules.CommandManager import CommandManager
from dolphie.Modules.CommandPalette import CommandPaletteCommands
from dolphie.Modules.KeyEventManager import KeyEventManager
from dolphie.Modules.ReplayManager import ReplayManager
from dolphie.Modules.TabManager import Tab, TabManager
from dolphie.Modules.Theme import DOLPHIE_THEME
from dolphie.Modules.WorkerDataProcessor import WorkerDataProcessor
from dolphie.Modules.WorkerManager import WorkerManager
from dolphie.Panels import DDL as DDLPanel
from dolphie.Panels import Dashboard as DashboardPanel
from dolphie.Panels import MetadataLocks as MetadataLocksPanel
from dolphie.Panels import PerformanceSchemaMetrics as PerformanceSchemaMetricsPanel
from dolphie.Panels import Processlist as ProcesslistPanel
from dolphie.Panels import ProxySQLCommandStats as ProxySQLCommandStatsPanel
from dolphie.Panels import ProxySQLDashboard as ProxySQLDashboardPanel
from dolphie.Panels import ProxySQLHostgroupSummary as ProxySQLHostgroupSummaryPanel
from dolphie.Panels import ProxySQLProcesslist as ProxySQLProcesslistPanel
from dolphie.Panels import ProxySQLQueryRules as ProxySQLQueryRulesPanel
from dolphie.Panels import Replication as ReplicationPanel
from dolphie.Panels import StatementsSummaryMetrics as StatementsSummaryPanel
from dolphie.Widgets.CommandModal import CommandModal
from dolphie.Widgets.TopBar import TopBar

try:
    __package_name__ = metadata.metadata(__package__ or __name__)["Name"]
    __version__ = metadata.version(__package__ or __name__)
except Exception:
    __package_name__ = "Dolphie"
    __version__ = "N/A"


class DolphieApp(App):
    TITLE = "Dolphie"
    CSS_PATH = "Dolphie.tcss"
    COMMANDS = {CommandPaletteCommands}
    COMMAND_PALETTE_BINDING = "question_mark"
    BINDINGS = [Binding("escape", "exit_maximized_panel", "Exit maximized panel", show=True)]
    PANEL_MAPPING = {
        "replication": {ConnectionSource.mysql: ReplicationPanel},
        "dashboard": {
            ConnectionSource.mysql: DashboardPanel,
            ConnectionSource.proxysql: ProxySQLDashboardPanel,
        },
        "processlist": {
            ConnectionSource.mysql: ProcesslistPanel,
            ConnectionSource.proxysql: ProxySQLProcesslistPanel,
        },
        "metadata_locks": {ConnectionSource.mysql: MetadataLocksPanel},
        "ddl": {ConnectionSource.mysql: DDLPanel},
        "pfs_metrics": {ConnectionSource.mysql: PerformanceSchemaMetricsPanel},
        "statements_summary": {ConnectionSource.mysql: StatementsSummaryPanel},
        "proxysql_hostgroup_summary": {ConnectionSource.proxysql: ProxySQLHostgroupSummaryPanel},
        "proxysql_mysql_query_rules": {ConnectionSource.proxysql: ProxySQLQueryRulesPanel},
        "proxysql_command_stats": {ConnectionSource.proxysql: ProxySQLCommandStatsPanel},
    }

    def __init__(self, config: Config):
        super().__init__()

        self.config = config
        self.command_manager = CommandManager()
        self.key_event_manager: KeyEventManager
        self.worker_manager: WorkerManager
        self.worker_data_processor: WorkerDataProcessor
        self.tab_manager: TabManager

        self._has_tty = sys.stdin.isatty()

        # Replay Back/Forward acceleration: while [ or ] is held (detected via key
        # auto-repeat in KeyEventManager), the step ramps up so scrubbing covers ground
        # without a render per row. Manual clicks/taps stay at a single row.
        self._replay_nav_streak = 0

        self.console.set_window_title(self.TITLE or "Dolphie")

        self.register_theme(DOLPHIE_THEME)
        self.theme = DOLPHIE_THEME.name

        if config.daemon_mode:
            logger.info(
                f"Starting Dolphie v{__version__} in daemon mode with a refresh interval of {config.refresh_interval}s"
            )
            logger.info(f"Log file: {config.daemon_mode_log_file}")

    @work(thread=True, group="replay", exclusive=True)
    async def run_worker_replay(self, tab_id: str, manual_control: bool = False):
        """Execute replay worker in a worker thread.

        This is a wrapper that uses the @work decorator (which requires a DOMNode)
        and delegates to WorkerManager for actual worker execution.

        Args:
            tab_id: The tab ID to run the worker for
            manual_control: Whether this is manual control
        """
        await self.worker_manager.run_worker_replay(tab_id, manual_control)

    @work(thread=True, group="main")
    async def run_worker_main(self, tab_id: str):
        """Execute main worker in a worker thread.

        This is a wrapper that uses the @work decorator (which requires a DOMNode)
        and delegates to WorkerManager for actual worker execution.

        Args:
            tab_id: The tab ID to run the worker for
        """
        await self.worker_manager.run_worker_main(tab_id)

    @work(thread=True, group="replicas")
    def run_worker_replicas(self, tab_id: str):
        """Execute replicas worker in a worker thread.

        This is a wrapper that uses the @work decorator (which requires a DOMNode)
        and delegates to WorkerManager for actual worker execution.

        Args:
            tab_id: The tab ID to run the worker for
        """
        self.worker_manager.run_worker_replicas(tab_id)

    def on_worker_state_changed(self, event: Worker.StateChanged):
        """Delegate worker state changes to the WorkerManager.

        This method was extracted into a separate handler class for better
        code organization and maintainability.

        Args:
            event: The worker state changed event
        """
        self.worker_manager.on_worker_state_changed(event)

    async def on_key(self, event: events.Key):
        """Handle key events and delegate to KeyEventManager.

        Args:
            event: The key event
        """
        if len(self.screen_stack) > 1:
            return

        await self.key_event_manager.process_key_event(event.key)

    @work()
    async def connect_as_hostgroup(self, hostgroup: str):
        self.tab_manager.loading_hostgroups = True
        self.notify(
            f"Connecting to hosts in hostgroup [$highlight]{hostgroup}",
            severity="information",
        )

        for hostgroup_member in self.config.hostgroup_hosts.get(hostgroup, []):
            # We only want to switch if it's the first tab created
            switch_tab = bool(not self.tab_manager.active_tab)

            tab = await self.tab_manager.create_tab(hostgroup_member=hostgroup_member, switch_tab=switch_tab)

            self.run_worker_main(tab.id)

            if not self.config.daemon_mode:
                self.run_worker_replicas(tab.id)

        # Wait for all workers to finish before notifying the user
        await asyncio.sleep(0.2)
        for tab in self.tab_manager.tabs.values():
            while tab.worker and tab.worker.is_running:
                await asyncio.sleep(0.1)

        self.tab_manager.loading_hostgroups = False
        self.notify(
            f"Finished connecting to hosts in hostgroup [$highlight]{hostgroup}",
            severity="information",
        )

    # Replay playback actions. Both the ReplayControls buttons and the keyboard
    # shortcuts in KeyEventManager route through these so there's a single code path.
    def _replay_nav_step(self, accelerate: bool) -> int:
        """Returns how many rows a single Back/Forward should move.

        Returns 1 unless [ or ] is being held (``accelerate``), in which case the step
        ramps up the longer it's held so fast scrubbing covers ground without a render
        per row. Manual clicks/taps and the ReplayControls buttons always move one row.
        """
        if not accelerate:
            self._replay_nav_streak = 0
            return 1

        self._replay_nav_streak += 1
        return min(1 + self._replay_nav_streak // 2, 25)

    def action_replay_back(self, accelerate: bool = False):
        tab = self.tab_manager.active_tab
        if not tab or not tab.dolphie.replay_file or tab.replay_manager is None:
            return

        if tab.replay_manager.seek_relative(-self._replay_nav_step(accelerate)):
            self.force_refresh_for_replay()
        else:
            self.notify("You're already at the beginning of the replay", severity="warning")

    def action_replay_forward(self, accelerate: bool = False):
        tab = self.tab_manager.active_tab
        if not tab or not tab.dolphie.replay_file or tab.replay_manager is None:
            return

        if tab.replay_manager.current_replay_id >= tab.replay_manager.max_replay_id:
            self.notify("You're already at the end of the replay", severity="warning")
            return

        tab.replay_manager.seek_relative(self._replay_nav_step(accelerate))
        self.force_refresh_for_replay()

    def action_replay_pause(self):
        tab = self.tab_manager.active_tab
        if not tab or not tab.dolphie.replay_file:
            return

        tab.dolphie.pause_refresh = not tab.dolphie.pause_refresh
        tab.replay_controls.paused = tab.dolphie.pause_refresh

        if tab.dolphie.pause_refresh:
            self.notify("Replay is paused")
        else:
            self.notify("Replay has resumed", severity="information")

    def action_replay_seek(self):
        tab = self.tab_manager.active_tab
        if not tab or not tab.dolphie.replay_file or tab.replay_manager is None:
            return

        replay_manager = tab.replay_manager

        def command_get_input(timestamp: object | None) -> None:
            if isinstance(timestamp, str) and timestamp and replay_manager.seek_to_timestamp(timestamp):
                self.force_refresh_for_replay()

        self.push_screen(
            CommandModal(
                command=HotkeyCommands.replay_seek,
                message="What time would you like to seek to?",
                max_replay_timestamp=replay_manager.max_replay_timestamp,
            ),
            command_get_input,
        )

    @on(RadioSet.Changed, "#pfs_metrics_radio_set")
    def replay_pfs_metrics_radio_set_changed(self, event: RadioSet.Changed):
        tab = self.tab_manager.active_tab

        if tab:
            self.refresh_panel(tab, tab.dolphie.panels.pfs_metrics.name)

    @on(Tabs.TabActivated, "#host_tabs")
    def host_tab_changed(self, event: Tabs.TabActivated):
        previous_tab = self.tab_manager.active_tab

        # If the previous tab is the same as the current tab, return
        if previous_tab and event.tab.id == previous_tab.id:
            return

        # If the previous tab is a replay file, cancel its worker and timer
        if previous_tab and previous_tab.dolphie.replay_file and previous_tab.worker:
            previous_tab.worker.cancel()
            if previous_tab.worker_timer:
                previous_tab.worker_timer.stop()

        if event.tab.id is None:
            return
        self.tab_manager.switch_tab(event.tab.id, set_active=False)

        tab = self.tab_manager.active_tab
        # Sync the (shared) replay controls to the newly-active tab's pause state
        # immediately so the button label doesn't briefly show the previous tab's state.
        if tab and tab.dolphie.replay_file:
            tab.replay_controls.paused = tab.dolphie.pause_refresh
        if (
            tab
            and tab.worker
            and (
                (tab.dolphie.main_db_connection.is_connected() and tab.dolphie.worker_processing_time)
                or tab.dolphie.replay_file
            )
        ):
            # Set each panel's display status based on the tab's panel visibility
            for panel in tab.dolphie.panels.get_all_panels():
                tab.get_panel_widget(panel.name).display = panel.visible

            tab.sync_shared_ui()
            tab.toggle_entities_displays()

            self.worker_data_processor.refresh_screen(tab)
            if tab.dolphie.connection_source == ConnectionSource.mysql:
                ReplicationPanel.create_replica_panel(tab)
                tab.toggle_replication_panel_components()

            self.force_refresh_for_replay(need_current_data=True)

    def update_graphs(self) -> None:
        """Refresh the shared dashboard from the active host."""
        tab = self.tab_manager.active_tab
        if not tab or not tab.panel_graphs.display:
            return

        with self.batch_update():
            tab.graph_dashboard.bind_host(tab.dolphie)

    def toggle_panel(self, panel_name: str):
        tab = self.tab_manager.active_tab
        if tab is None:
            return

        # We store the panel objects in the tab object (i.e. tab.panel_dashboard, tab.panel_processlist, etc.)
        panel = tab.get_panel_widget(panel_name)

        new_display_status = not panel.display

        getattr(tab.dolphie.panels, panel_name).visible = new_display_status

        if panel_name != tab.dolphie.panels.graphs.name:
            self.refresh_panel(tab, panel_name, toggled=True)

        panel.display = new_display_status

        self.force_refresh_for_replay(need_current_data=True)

    def force_refresh_for_replay(self, need_current_data: bool = False):
        # This function lets us force a refresh of the worker thread when we're in a replay
        tab = self.tab_manager.active_tab

        if (
            tab is not None
            and tab.dolphie.replay_file
            and tab.replay_manager is not None
            and (not tab.worker or not tab.worker.is_running)
        ):
            if tab.worker:
                tab.worker.cancel()
            if tab.worker_timer:
                tab.worker_timer.stop()

            if need_current_data:
                tab.replay_manager.current_replay_id -= 1

            self.run_worker_replay(tab.id, manual_control=True)

    def refresh_panel(self, tab: Tab, panel_name: str, toggled: bool = False):
        connection_sources = self.PANEL_MAPPING.get(panel_name)
        if not connection_sources:
            return

        panel_module = connection_sources.get(tab.dolphie.connection_source)
        if not panel_module:
            tab.get_panel_widget(panel_name).display = False
            return

        panel_module.create_panel(tab)

        if panel_name == tab.dolphie.panels.replication.name and toggled and tab.dolphie.replication_status:
            # When replication panel status is changed, we need to refresh the dashboard panel as well since
            # it adds/removes it from there
            DashboardPanel.create_panel(tab)

    def sync_replication_ui(self, tab: Tab) -> None:
        """Render a host's cached data into the shared replication panel."""
        if tab.dolphie.connection_source != ConnectionSource.mysql or not tab.dolphie.panels.replication.visible:
            return

        ReplicationPanel.create_panel(tab)
        ReplicationPanel.create_replica_panel(tab)
        tab.toggle_replication_panel_components()

    def check_for_new_version(self):
        # Query PyPI API to get the latest version
        try:
            url = self.config.pypi_repository
            response = requests.get(url, timeout=3)

            if response.status_code == 200:
                data = response.json()

                # Extract the latest version from the response
                latest_version = data["info"]["version"]

                # Compare the current version with the latest version
                if parse_version(latest_version) > parse_version(__version__):
                    self.notify(
                        f"{Emoji('tada')}  [b]New version [$highlight]{latest_version}[/$highlight] is available![/b] "
                        f"{Emoji('tada')}\n\nPlease update at your earliest convenience\n"
                        f"[$dark_gray]Find more details at https://github.com/charles-001/dolphie",
                        title="",
                        severity="information",
                        timeout=20,
                    )

                    logger.info(
                        f"New version {latest_version} is available! Please update at your earliest convenience. "
                        "Find more details at https://github.com/charles-001/dolphie"
                    )
        except Exception:
            pass

    def _monitor_terminal_disconnect(self):
        """Periodically check if we still have a valid TTY connection.
        If TTY is lost, gracefully shut down to prevent CPU spikes.
        """
        if not self._has_tty or self.config.daemon_mode:
            return

        try:
            current_tty = sys.stdin.isatty()

            # If TTY is lost, exit the application
            if not current_tty:
                self.exit()
                return
        except (OSError, ValueError):
            self.exit()
            return
        except Exception:
            pass

        # If we're still running, schedule the next check (every 5 seconds)
        self.set_timer(5.0, self._monitor_terminal_disconnect)

    async def on_mount(self):
        self.tab_manager = TabManager(app=self, config=self.config)
        await self.tab_manager.create_ui_widgets()

        self.key_event_manager = KeyEventManager(app=self)
        self.worker_manager = WorkerManager(app=self)
        self.worker_data_processor = WorkerDataProcessor(app=self)

        if self.config.hostgroup:
            self.connect_as_hostgroup(self.config.hostgroup)
        else:
            tab = await self.tab_manager.create_tab(tab_name="Initial Tab")

            if self.config.tab_setup:
                self.tab_manager.setup_host_tab(tab)
            elif tab.dolphie.replay_file:
                tab.replay_manager = ReplayManager(tab.dolphie)
                if not tab.replay_manager.verify_replay_file():
                    tab.replay_manager = None
                    self.tab_manager.setup_host_tab(tab)
                    return

                self.tab_manager.rename_tab(tab)
                self.tab_manager.update_connection_status(tab=tab, connection_status=ConnectionStatus.connected)
                self.run_worker_replay(tab.id)
            else:
                self.run_worker_main(tab.id)

                if not self.config.daemon_mode:
                    self.run_worker_replicas(tab.id)

        self.check_for_new_version()

        self.set_timer(5.0, self._monitor_terminal_disconnect)

        self.watch(self.screen, "maximized", self._update_maximized_footer)

    def _update_maximized_footer(self) -> None:
        is_maximized = self.screen.maximized is not None
        footer = self.screen.query(Footer)

        if is_maximized and not footer:
            self.screen.mount(Footer(compact=True, show_command_palette=False))
            self.screen.call_after_refresh(self.screen.refresh_bindings)
        elif not is_maximized and footer:
            footer.first().remove()

    def check_action(self, action: str, parameters: tuple[object, ...]) -> bool | None:
        if action == "exit_maximized_panel":
            return self.screen.maximized is not None

        return True

    def action_exit_maximized_panel(self) -> None:
        if self.screen.maximized is not None:
            self.screen.minimize()

    def compose(self):
        yield TopBar(
            host="",
            app_version=__version__,
            help="press [$b_highlight]?[/] for help",
        )
        yield Tabs(id="host_tabs")

    def _handle_exception(self, error: Exception) -> None:
        self.bell()
        self.exit(message=Traceback(show_locals=True, width=None, locals_max_length=5))


def setup_logger(config: Config):
    logger.remove()

    # If we're not using daemon mode, we want to essentially disable logging
    if not config.daemon_mode:
        return

    logger.level("DEBUG", color="<magenta>")
    logger.level("INFO", color="<blue>")
    logger.level("WARNING", color="<yellow>")
    logger.level("ERROR", color="<red>")
    log_format = "<dim>{time:MM-DD-YYYY HH:mm:ss}</dim> <b><level>[{level}]</level></b> {message}"

    log_level = "INFO"

    # Add terminal & file logging
    logger.add(sys.stdout, format=log_format, backtrace=True, colorize=True, level=log_level)
    logger.add(config.daemon_mode_log_file, format=log_format, backtrace=True, level=log_level)

    # Exit when critical is used
    logger.add(lambda _: sys.exit(1), level="CRITICAL")


def detect_iterm2() -> None:
    """Detect iTerm2 by probing the terminal directly and set LC_TERMINAL if found.

    iTerm2 advertises itself via TERM_PROGRAM (set locally) and LC_TERMINAL, but
    SSH usually strips both: TERM_PROGRAM is never forwarded, and LC_TERMINAL is
    dropped unless the server's sshd happens to have `AcceptEnv` for it (most
    don't). Without that signal, Textual's IS_ITERM is False and it negotiates the
    in-band window resize protocol (DEC private mode 2048), which iTerm2 implements
    buggily and which corrupts mouse input over SSH.

    Querying the terminal with XTVERSION asks iTerm2 directly over the live PTY, so
    detection works regardless of env forwarding. We set LC_TERMINAL so Textual's
    IS_ITERM picks it up (it's read lazily, after this runs, when the input parser
    is first imported during app.run()).
    """
    # Already detectable by Textual, or no terminal to probe.
    if os.environ.get("LC_TERMINAL") or os.environ.get("TERM_PROGRAM") == "iTerm.app":
        return
    if os.environ.get("TERM", "") in ("", "dumb"):
        return
    if not (sys.stdin.isatty() and sys.stdout.isatty()):
        return

    try:
        import select
        import termios
        import tty
    except ImportError:
        return  # Non-POSIX (e.g. Windows); nothing to do.

    fd = sys.stdin.fileno()
    try:
        old_attrs = termios.tcgetattr(fd)
    except termios.error:
        return

    response = ""
    try:
        tty.setraw(fd)
        # XTVERSION request, followed by a Primary Device Attributes request as a
        # terminator: every terminal answers DA ("\x1b[?...c"), so we know when the
        # reply is complete instead of relying on a fixed sleep.
        sys.stdout.write("\x1b[>q\x1b[c")
        sys.stdout.flush()

        deadline = time.monotonic() + 0.3
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            readable, _, _ = select.select([fd], [], [], remaining)
            if not readable:
                break
            chunk = os.read(fd, 1024)
            if not chunk:
                break
            response += chunk.decode("latin-1", errors="replace")
            # Stop once the Primary DA reply (ends with "c") has arrived.
            if "\x1b[?" in response and response.rstrip().endswith("c"):
                break
    except (OSError, termios.error):
        return
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_attrs)

    if "iTerm2" in response:
        os.environ["LC_TERMINAL"] = "iTerm2"


def main():
    # Set environment variables for better color support
    os.environ["TERM"] = "xterm-256color"
    os.environ["COLORTERM"] = "truecolor"

    # Identify iTerm2 over SSH so Textual avoids a mouse-breaking protocol (see fn docstring).
    detect_iterm2()

    arg_parser = ArgumentParser(__version__)

    setup_logger(arg_parser.config)

    app = DolphieApp(arg_parser.config)
    try:
        app.run(headless=arg_parser.config.daemon_mode)
    except KeyboardInterrupt:
        pass
    finally:
        if arg_parser.config.daemon_mode:
            logger.info("Shutting down")
            for tab in app.tab_manager.tabs.values():
                tab.dolphie.main_db_connection.close()
                tab.dolphie.secondary_db_connection.close()
                if tab.replay_manager:
                    tab.replay_manager.close()


if __name__ == "__main__":
    main()
