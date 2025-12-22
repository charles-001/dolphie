#!/usr/bin/env python3
"""Dolphie - Your single pane of glass for real-time analytics into MySQL/MariaDB & ProxySQL.

Author: Charles Thompson
License: GPL-3.0
"""

import asyncio
import os
import sys
from importlib import metadata

import requests
from loguru import logger
from packaging.version import parse as parse_version
from rich.emoji import Emoji
from rich.theme import Theme as RichTheme
from rich.traceback import Traceback
from textual import events, on, work
from textual.app import App
from textual.theme import Theme as TextualTheme
from textual.widgets import Button, RadioSet, Switch, TabbedContent, Tabs
from textual.worker import Worker

import dolphie.Modules.MetricManager as MetricManager
from dolphie.DataTypes import ConnectionSource, ConnectionStatus, HotkeyCommands
from dolphie.Modules.ArgumentParser import ArgumentParser, Config
from dolphie.Modules.CommandManager import CommandManager
from dolphie.Modules.CommandPalette import CommandPaletteCommands
from dolphie.Modules.KeyEventManager import KeyEventManager
from dolphie.Modules.ReplayManager import ReplayManager
from dolphie.Modules.TabManager import Tab, TabManager
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

    def __init__(self, config: Config):
        super().__init__()

        self.config = config
        self.command_manager = CommandManager()
        self.key_event_manager: KeyEventManager = None
        self.worker_manager: WorkerManager = None
        self.worker_data_processor: WorkerDataProcessor = None

        self._has_tty = sys.stdin.isatty()

        theme = RichTheme(
            {
                "white": "#e9e9e9",
                "green": "#54efae",
                "yellow": "#f6ff8f",
                "dark_yellow": "#e6d733",
                "red": "#fd8383",
                "purple": "#b565f3",
                "dark_gray": "#969aad",
                "b dark_gray": "b #969aad",
                "highlight": "#91abec",
                "label": "#c5c7d2",
                "b label": "b #c5c7d2",
                "light_blue": "#bbc8e8",
                "b white": "b #e9e9e9",
                "b highlight": "b #91abec",
                "b light_blue": "b #bbc8e8",
                "recording": "#ff5e5e",
                "b recording": "b #ff5e5e",
                "panel_border": "#6171a6",
                "table_border": "#333f62",
            }
        )
        self.console.push_theme(theme)
        self.console.set_window_title(self.TITLE)

        theme = TextualTheme(
            name="custom",
            primary="white",
            variables={
                "white": "#e9e9e9",
                "green": "#54efae",
                "yellow": "#f6ff8f",
                "dark_yellow": "#e6d733",
                "red": "#fd8383",
                "purple": "#b565f3",
                "dark_gray": "#969aad",
                "b_dark_gray": "b #969aad",
                "highlight": "#91abec",
                "label": "#c5c7d2",
                "b_label": "b #c5c7d2",
                "light_blue": "#bbc8e8",
                "b_white": "b #e9e9e9",
                "b_highlight": "b #91abec",
                "b_light_blue": "b #bbc8e8",
                "recording": "#ff5e5e",
                "b_recording": "b #ff5e5e",
                "panel_border": "#6171a6",
                "table_border": "#333f62",
            },
        )
        self.register_theme(theme)
        self.theme = "custom"

        if config.daemon_mode:
            logger.info(
                f"Starting Dolphie v{__version__} in daemon mode with a refresh "
                f"interval of {config.refresh_interval}s"
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
            self.run_worker_replicas(tab.id)

        # Wait for all workers to finish before notifying the user
        await asyncio.sleep(0.2)
        for tab in self.tab_manager.tabs.values():
            while tab.worker and tab.worker.is_running:
                await asyncio.sleep(0.1)

        self.tab_manager.loading_hostgroups = False
        self.notify(
            f"Finished connecting to hosts in hostgroup [$highlight]{hostgroup}",
            severity="success",
        )

    @on(Button.Pressed, "#back_button")
    def replay_back(self):
        self.tab_manager.active_tab.replay_manager.current_replay_id -= 2
        self.force_refresh_for_replay()

    @on(Button.Pressed, "#forward_button")
    def replay_forward(self):
        self.force_refresh_for_replay()

    @on(Button.Pressed, "#pause_button")
    def replay_pause(self, event: Button.Pressed):
        tab = self.tab_manager.active_tab

        if not tab.dolphie.pause_refresh:
            tab.dolphie.pause_refresh = True
            self.notify("Replay is paused")
            event.button.label = "▶️  Resume"
        else:
            tab.dolphie.pause_refresh = False
            self.notify("Replay has resumed", severity="success")
            event.button.label = "⏸️  Pause"

    @on(Button.Pressed, "#seek_button")
    def replay_seek(self):
        def command_get_input(timestamp: str):
            if timestamp:
                found_timestamp = self.tab_manager.active_tab.replay_manager.seek_to_timestamp(timestamp)

                if found_timestamp:
                    self.force_refresh_for_replay()

        self.app.push_screen(
            CommandModal(
                command=HotkeyCommands.replay_seek,
                message="What time would you like to seek to?",
                max_replay_timestamp=self.tab_manager.active_tab.replay_manager.max_replay_timestamp,
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

        self.tab_manager.switch_tab(event.tab.id, set_active=False)

        tab = self.tab_manager.active_tab
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
                tab_panel = tab.get_panel_widget(panel.name)
                tab_panel.display = getattr(tab.dolphie.panels, panel.name).visible

            tab.toggle_metric_graph_tabs_display()
            tab.toggle_entities_displays()

            if tab.dolphie.connection_source == ConnectionSource.mysql:
                self.worker_data_processor.refresh_screen_mysql(tab)
                ReplicationPanel.create_replica_panel(tab)
                tab.toggle_replication_panel_components()
            elif tab.dolphie.connection_source == ConnectionSource.proxysql:
                self.worker_data_processor.refresh_screen_proxysql(tab)

            self.force_refresh_for_replay(need_current_data=True)

    @on(TabbedContent.TabActivated, "#metric_graph_tabs")
    def metric_graph_tab_changed(self, event: TabbedContent.TabActivated):
        metric_tab_name = event.pane.name

        if metric_tab_name:
            self.update_graphs(metric_tab_name)

    def update_graphs(self, metric_tab_name: str):
        tab = self.tab_manager.active_tab
        if not tab or not tab.panel_graphs.display:
            return

        for metric_instance in tab.dolphie.metric_manager.metrics.__dict__.values():
            if metric_tab_name == metric_instance.tab_name:
                # Batch all graph and stats label updates into a single rendering cycle
                with self.batch_update():
                    for graph_name in metric_instance.graphs:
                        getattr(tab, graph_name).render_graph(metric_instance, tab.dolphie.metric_manager.datetimes)
                    self.update_stats_label(metric_tab_name)

    def update_stats_label(self, metric_tab_name: str):
        stat_data = {}

        for metric_instance in self.tab_manager.active_tab.dolphie.metric_manager.metrics.__dict__.values():
            if metric_tab_name == metric_instance.tab_name:
                number_format_func = MetricManager.get_number_format_function(metric_instance, color=True)
                for metric_name, metric_data in metric_instance.__dict__.items():
                    if isinstance(metric_data, MetricManager.MetricData) and metric_data.values and metric_data.visible:
                        if f"graph_{metric_name}" in metric_instance.graphs:
                            stat_data[metric_data.label] = round(metric_data.values[-1])
                        else:
                            stat_data[metric_data.label] = number_format_func(metric_data.values[-1])

        formatted_stat_data = "  ".join(
            f"[$b_light_blue]{label}[/$b_light_blue] {value}" for label, value in stat_data.items()
        )
        getattr(self.tab_manager.active_tab, metric_tab_name).update(formatted_stat_data)

    def toggle_panel(self, panel_name: str):
        # We store the panel objects in the tab object (i.e. tab.panel_dashboard, tab.panel_processlist, etc.)
        panel = self.tab_manager.active_tab.get_panel_widget(panel_name)

        new_display_status = not panel.display

        getattr(self.tab_manager.active_tab.dolphie.panels, panel_name).visible = new_display_status

        if panel_name not in [self.tab_manager.active_tab.dolphie.panels.graphs.name]:
            self.refresh_panel(self.tab_manager.active_tab, panel_name, toggled=True)

        panel.display = new_display_status

        self.force_refresh_for_replay(need_current_data=True)

    def force_refresh_for_replay(self, need_current_data: bool = False):
        # This function lets us force a refresh of the worker thread when we're in a replay
        tab = self.tab_manager.active_tab

        if tab.dolphie.replay_file and (not tab.worker or not tab.worker.is_running):
            if tab.worker:
                tab.worker.cancel()
            if tab.worker_timer:
                tab.worker_timer.stop()

            if need_current_data:
                # We subtract 1 because get_next_refresh_interval will increment the index
                tab.replay_manager.current_replay_id -= 1

            self.run_worker_replay(tab.id, manual_control=True)

    def refresh_panel(self, tab: Tab, panel_name: str, toggled: bool = False):
        panel_mapping = {
            tab.dolphie.panels.replication.name: {ConnectionSource.mysql: ReplicationPanel},
            tab.dolphie.panels.dashboard.name: {
                ConnectionSource.mysql: DashboardPanel,
                ConnectionSource.proxysql: ProxySQLDashboardPanel,
            },
            tab.dolphie.panels.processlist.name: {
                ConnectionSource.mysql: ProcesslistPanel,
                ConnectionSource.proxysql: ProxySQLProcesslistPanel,
            },
            tab.dolphie.panels.metadata_locks.name: {ConnectionSource.mysql: MetadataLocksPanel},
            tab.dolphie.panels.ddl.name: {ConnectionSource.mysql: DDLPanel},
            tab.dolphie.panels.pfs_metrics.name: {ConnectionSource.mysql: PerformanceSchemaMetricsPanel},
            tab.dolphie.panels.statements_summary.name: {ConnectionSource.mysql: StatementsSummaryPanel},
            tab.dolphie.panels.proxysql_hostgroup_summary.name: {
                ConnectionSource.proxysql: ProxySQLHostgroupSummaryPanel
            },
            tab.dolphie.panels.proxysql_mysql_query_rules.name: {ConnectionSource.proxysql: ProxySQLQueryRulesPanel},
            tab.dolphie.panels.proxysql_command_stats.name: {ConnectionSource.proxysql: ProxySQLCommandStatsPanel},
        }

        for panel_map_name, panel_map_connection_sources in panel_mapping.items():
            panel_map_obj = panel_map_connection_sources.get(tab.dolphie.connection_source)

            if not panel_map_obj:
                tab.get_panel_widget(panel_map_name).display = False
                continue

            if panel_name == panel_map_name:
                panel_map_obj.create_panel(tab)

            if panel_name == tab.dolphie.panels.replication.name and toggled and tab.dolphie.replication_status:
                # When replication panel status is changed, we need to refresh the dashboard panel as well since
                # it adds/removes it from there
                DashboardPanel.create_panel(tab)

    @on(Switch.Changed)
    def switch_changed(self, event: Switch.Changed):
        if len(self.screen_stack) > 1 or not self.tab_manager.active_tab:
            return

        metric_tab_name = event.switch.name

        # The switch id is in the format of metric_instance_name-metric
        metric_split = event.switch.id.split("-")
        metric_instance_name = metric_split[0]
        metric = metric_split[1]

        # Set the visible boolean of the metric data to the switch value
        metric_instance = getattr(
            self.tab_manager.active_tab.dolphie.metric_manager.metrics,
            metric_instance_name,
        )
        metric_data: MetricManager.MetricData = getattr(metric_instance, metric)
        metric_data.visible = event.value

        self.update_graphs(metric_tab_name)

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
        self.tab_manager = TabManager(app=self.app, config=self.config)
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
            elif self.tab_manager.active_tab.dolphie.replay_file:
                self.tab_manager.active_tab.replay_manager = ReplayManager(tab.dolphie)
                if not tab.replay_manager.verify_replay_file():
                    tab.replay_manager = None
                    self.tab_manager.setup_host_tab(tab)
                    return

                self.tab_manager.rename_tab(tab)
                self.tab_manager.update_connection_status(tab=tab, connection_status=ConnectionStatus.connected)
                self.run_worker_replay(self.tab_manager.active_tab.id)
            else:
                self.run_worker_main(self.tab_manager.active_tab.id)

                if not self.config.daemon_mode:
                    self.run_worker_replicas(self.tab_manager.active_tab.id)

        self.check_for_new_version()

        self.set_timer(5.0, self._monitor_terminal_disconnect)

    def compose(self):
        yield TopBar(
            host="",
            app_version=__version__,
            help="press [b highlight]?[/b highlight] for commands",
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


def main():
    # Set environment variables for better color support
    os.environ["TERM"] = "xterm-256color"
    os.environ["COLORTERM"] = "truecolor"

    arg_parser = ArgumentParser(__version__)

    setup_logger(arg_parser.config)

    app = DolphieApp(arg_parser.config)
    app.run(headless=arg_parser.config.daemon_mode)


if __name__ == "__main__":
    main()
