#!/usr/bin/env python3

# ****************************
# *        Dolphie           *
# * Author: Charles Thompson *
# ****************************


import asyncio
import csv
import os
import re
import sys
from datetime import datetime, timedelta
from functools import partial
from importlib import metadata

import requests
from loguru import logger
from packaging.version import parse as parse_version
from rich import box
from rich.align import Align
from rich.console import Group
from rich.style import Style
from rich.table import Table
from rich.theme import Theme
from rich.traceback import Traceback
from sqlparse import format as sqlformat
from textual import events, on, work
from textual.app import App
from textual.command import DiscoveryHit, Hit, Provider
from textual.widgets import Button, Switch, TabbedContent, TabPane, Tabs
from textual.worker import Worker, WorkerState, get_current_worker

import dolphie.Modules.MetricManager as MetricManager
from dolphie.DataTypes import (
    ConnectionSource,
    ConnectionStatus,
    HotkeyCommands,
    ProcesslistThread,
    ProxySQLProcesslistThread,
)
from dolphie.Dolphie import Dolphie
from dolphie.Modules.ArgumentParser import ArgumentParser, Config
from dolphie.Modules.CommandManager import CommandManager
from dolphie.Modules.Functions import (
    format_bytes,
    format_number,
    format_query,
    format_sys_table_memory,
)
from dolphie.Modules.ManualException import ManualException
from dolphie.Modules.Queries import MySQLQueries, ProxySQLQueries
from dolphie.Modules.ReplayManager import ReplayManager
from dolphie.Modules.TabManager import Tab, TabManager
from dolphie.Panels import (
    dashboard_panel,
    ddl_panel,
    metadata_locks_panel,
    processlist_panel,
    proxysql_command_stats_panel,
    proxysql_dashboard_panel,
    proxysql_hostgroup_summary_panel,
    proxysql_mysql_query_rules_panel,
    proxysql_processlist_panel,
    replication_panel,
)
from dolphie.Widgets.command_screen import CommandScreen
from dolphie.Widgets.event_log_screen import EventLog
from dolphie.Widgets.modal import CommandModal
from dolphie.Widgets.proxysql_thread_screen import ProxySQLThreadScreen
from dolphie.Widgets.thread_screen import ThreadScreen
from dolphie.Widgets.topbar import TopBar

try:
    __package_name__ = metadata.metadata(__package__ or __name__)["Name"]
    __version__ = metadata.version(__package__ or __name__)
except Exception:
    __package_name__ = "Dolphie"
    __version__ = "N/A"


class CommandPaletteCommands(Provider):
    """Command palette commands for Dolphie based on connection source."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dolphie_app: DolphieApp = self.app

    def async_command(self, key: str):
        """Helper function to call the capture_key command asynchronously."""
        self.app.call_later(self.dolphie_app.capture_key, key)

    def get_command_hits(self):
        """Helper function to get all commands and format them for discovery or search."""
        commands = self.dolphie_app.command_manager.get_commands(
            self.dolphie_app.tab_manager.active_tab.dolphie.replay_file,
            self.dolphie_app.tab_manager.active_tab.dolphie.connection_source,
        )

        # Find the longest human_key length
        max_key_length = max(len(data["human_key"]) for data in commands.values())

        # Format the commands with dynamic spacing
        return {
            key: {
                # Center the human_key based on the max length and pad spaces after it
                "display": (
                    f"[b highlight]{data['human_key'].center(max_key_length)}[/b highlight]  {data['description']}"
                ),
                "text": f"{data['human_key']} {data['description']}",
                "command": partial(self.async_command, key),
                "human_key": data["human_key"],
            }
            for key, data in commands.items()
        }

    async def discover(self):
        for data in self.get_command_hits().values():
            yield DiscoveryHit(
                display=data["display"],
                text=data["text"],
                command=data["command"],
            )

    async def search(self, query: str):
        hits = []

        # Gather all hits and calculate their scores
        for data in self.get_command_hits().values():
            score = self.matcher(query).match(data["text"])
            if score > 0:
                hits.append(
                    Hit(
                        score=score,
                        match_display=data["display"],
                        text=data["text"],
                        command=data["command"],
                    )
                )

        # Sort the hits by score, descending order
        hits.sort(key=lambda hit: hit.score, reverse=True)

        for hit in hits:
            yield hit


class DolphieApp(App):
    TITLE = "Dolphie"
    CSS_PATH = "Dolphie.css"
    COMMANDS = {CommandPaletteCommands}
    COMMAND_PALETTE_BINDING = "question_mark"

    def __init__(self, config: Config):
        super().__init__()

        self.config = config
        self.command_manager = CommandManager()
        self.loading_hostgroups: bool = False

        theme = Theme(
            {
                "white": "#e9e9e9",
                "green": "#54efae",
                "yellow": "#f6ff8f",
                "dark_yellow": "#cad45f",
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
                "bold red": "b #fd8383",
                "b light_blue": "b #bbc8e8",
                "recording": "#ff5e5e",
                "b recording": "b #ff5e5e",
                "panel_border": "#6171a6",
                "table_border": "#333f62",
            }
        )
        self.console.push_theme(theme)
        self.console.set_window_title(self.TITLE)

        if config.daemon_mode:
            logger.info(
                f"Starting Dolphie v{__version__} in daemon mode with a refresh "
                f"interval of {config.refresh_interval}s"
            )
            logger.info(f"Log file: {config.daemon_mode_log_file}")

    @work(thread=True, group="replay")
    async def run_worker_replay(self, tab_id: int, manual_control: bool = False):
        tab = self.tab_manager.get_tab(tab_id)

        # Get our worker thread
        tab.worker = get_current_worker()
        tab.worker.name = tab_id
        tab.worker_running = True

        dolphie = tab.dolphie

        tab.replay_manual_control = manual_control
        if (
            len(self.screen_stack) > 1
            or (dolphie.pause_refresh and not manual_control)
            or tab.id != self.tab_manager.active_tab.id
        ):
            return

        # Get the next event from the replay file
        replay_event_data = tab.replay_manager.get_next_refresh_interval()
        # If there's no more events, cancel the worker
        if not replay_event_data:
            tab.worker.cancel()

        min_timestamp = tab.replay_manager.min_timestamp
        max_timestamp = tab.replay_manager.max_timestamp
        current_timestamp = replay_event_data.timestamp

        # Highlight if the min or max timestamp matches the current timestamp
        min_timestamp = (
            f"[b light_blue]{min_timestamp}[/b light_blue]" if min_timestamp == current_timestamp else min_timestamp
        )
        max_timestamp = (
            f"[b light_blue]{max_timestamp}[/b light_blue]" if max_timestamp == current_timestamp else max_timestamp
        )

        # Update the dashboard title with the timestamp of the replay event
        tab.dashboard_replay.update(f"[b]Replay[/b] ([dark_gray]{os.path.basename(dolphie.replay_file)}[/dark_gray])")
        tab.dashboard_replay_start_end.update(
            f"{min_timestamp} [b highlight]←[/b highlight] "
            f"[b light_blue]{current_timestamp}[/b light_blue] [b highlight]→[/b highlight] "
            f"{max_timestamp}"
        )

        dolphie.detect_global_variable_change(
            old_data=dolphie.global_variables, new_data=replay_event_data.global_variables
        )

        # Common data for refreshing
        dolphie.global_variables = replay_event_data.global_variables
        dolphie.global_status = replay_event_data.global_status
        common_metrics = {"global_variables": dolphie.global_variables, "global_status": dolphie.global_status}

        if dolphie.connection_source == ConnectionSource.mysql:
            dolphie.set_host_version(dolphie.global_variables.get("version"))

            dolphie.binlog_status = replay_event_data.binlog_status
            dolphie.innodb_metrics = replay_event_data.innodb_metrics
            dolphie.replica_manager.available_replicas = replay_event_data.replica_manager
            dolphie.processlist_threads = replay_event_data.processlist
            dolphie.replication_status = replay_event_data.replication_status
            dolphie.metadata_locks = replay_event_data.metadata_locks
            dolphie.group_replication_members = replay_event_data.group_replication_members
            dolphie.group_replication_data = replay_event_data.group_replication_data

            connection_source_metrics = {
                "innodb_metrics": dolphie.innodb_metrics,
                "replication_status": dolphie.replication_status,
            }

            replication_panel.fetch_replicas(tab)

            if not dolphie.server_uuid:
                dolphie.configure_mysql_variables()
        elif dolphie.connection_source == ConnectionSource.proxysql:
            dolphie.set_host_version(dolphie.global_variables.get("admin-version"))

            dolphie.proxysql_command_stats = replay_event_data.command_stats
            dolphie.proxysql_hostgroup_summary = replay_event_data.hostgroup_summary
            dolphie.processlist_threads = replay_event_data.processlist

            connection_source_metrics = {"proxysql_command_stats": dolphie.proxysql_command_stats}

        # Refresh the metric manager metrics to the state of the replay event
        dolphie.metric_manager.refresh_data(**common_metrics, **connection_source_metrics)

        # Metrics data is already calculated in the replay event data so we just need to update the values
        dolphie.metric_manager.datetimes = replay_event_data.metric_manager.get("datetimes")
        for metric_name, metric_data in replay_event_data.metric_manager.items():
            metric_instance = dolphie.metric_manager.metrics.__dict__.get(metric_name)
            if metric_instance:
                for metric_name, metric_values in metric_data.items():
                    metric_instance.__dict__[metric_name].values = metric_values

    @work(thread=True, group="main")
    async def run_worker_main(self, tab_id: int):
        tab = self.tab_manager.get_tab(tab_id)
        if not tab:
            return

        # Get our worker thread
        tab.worker = get_current_worker()
        tab.worker.name = tab_id
        tab.worker_running = True

        dolphie = tab.dolphie
        try:
            if not dolphie.main_db_connection.is_connected():
                self.tab_manager.update_connection_status(tab=tab, connection_status=ConnectionStatus.connecting)

                tab.replay_manager = None
                if not dolphie.daemon_mode and tab == self.tab_manager.active_tab:
                    tab.loading_indicator.display = True

                dolphie.db_connect()

            worker_start_time = datetime.now()
            dolphie.polling_latency = (worker_start_time - dolphie.worker_previous_start_time).total_seconds()
            dolphie.worker_previous_start_time = worker_start_time

            if dolphie.connection_source == ConnectionSource.mysql:
                self.process_mysql_data(tab)
            elif dolphie.connection_source == ConnectionSource.proxysql:
                self.process_proxysql_data(tab)

            dolphie.worker_processing_time = (datetime.now() - worker_start_time).total_seconds()

            dolphie.metric_manager.refresh_data(
                worker_start_time=worker_start_time,
                polling_latency=dolphie.polling_latency,
                global_variables=dolphie.global_variables,
                global_status=dolphie.global_status,
                innodb_metrics=dolphie.innodb_metrics,
                disk_io_metrics=dolphie.disk_io_metrics,
                metadata_lock_metrics=dolphie.metadata_locks,
                replication_status=dolphie.replication_status,
                proxysql_command_stats=dolphie.proxysql_command_stats,
            )

            # We initalize this here so we have the host version from process_{mysql,proxysql}_data
            if not tab.replay_manager:
                tab.replay_manager = ReplayManager(dolphie)

            tab.replay_manager.capture_state()
        except ManualException as exception:
            # This will set up the worker state change function below to trigger the
            # tab setup modal with the error
            tab.worker_cancel_error = exception

            await self.tab_manager.disconnect_tab(tab)

        tab.worker_running = False

    @work(thread=True, group="replicas")
    def run_worker_replicas(self, tab_id: int):
        tab = self.tab_manager.get_tab(tab_id)
        if not tab:
            return

        # Get our worker thread
        tab.replicas_worker = get_current_worker()
        tab.replicas_worker.name = tab_id

        dolphie = tab.dolphie

        if dolphie.panels.replication.visible:
            if tab.id != self.tab_manager.active_tab.id:
                return

            if dolphie.replica_manager.available_replicas:
                if not dolphie.replica_manager.replicas:
                    tab.replicas_container.display = True
                    tab.replicas_loading_indicator.display = True
                    tab.replicas_title.update(
                        f"[b]Loading [highlight]{len(dolphie.replica_manager.available_replicas)}[/highlight]"
                        " replicas...\n"
                    )

                tab.replicas_worker_running = True
                replication_panel.fetch_replicas(tab)
                tab.replicas_worker_running = False
            else:
                tab.replicas_container.display = False
        else:
            # If we're not displaying the replication panel, close all replica connections
            dolphie.replica_manager.disconnect_all()

    def on_worker_state_changed(self, event: Worker.StateChanged):
        tab = self.tab_manager.get_tab(event.worker.name)
        if not tab:
            return

        dolphie = tab.dolphie

        if event.worker.group == "main":
            tab.worker_running = False

            if event.state == WorkerState.SUCCESS:
                self.monitor_read_only_change(tab)

                refresh_interval = dolphie.refresh_interval
                if dolphie.connection_source == ConnectionSource.proxysql:
                    refresh_interval = dolphie.determine_proxysql_refresh_interval()

                # Skip this if the conditions are right
                if (
                    len(self.screen_stack) > 1
                    or dolphie.pause_refresh
                    or not dolphie.main_db_connection.is_connected()
                    or dolphie.daemon_mode
                    or tab.id != self.tab_manager.active_tab.id
                ):
                    tab.worker_timer = self.set_timer(refresh_interval, partial(self.run_worker_main, tab.id))

                    return

                if not tab.main_container.display:
                    self.refresh_tab_properties()

                if dolphie.connection_source == ConnectionSource.mysql:
                    self.refresh_screen_mysql(tab)
                elif dolphie.connection_source == ConnectionSource.proxysql:
                    self.refresh_screen_proxysql(tab)

                # Update the topbar with the latest replay file size
                if dolphie.record_for_replay:
                    self.tab_manager.update_topbar(tab=tab)

                tab.worker_timer = self.set_timer(refresh_interval, partial(self.run_worker_main, tab.id))
            elif event.state == WorkerState.CANCELLED:
                # Only show the modal if there's a worker cancel error
                if tab.worker_cancel_error:
                    logger.critical(tab.worker_cancel_error)

                    if self.tab_manager.active_tab.id != tab.id or self.loading_hostgroups:
                        self.notify(
                            (
                                f"[b light_blue]{dolphie.host}:{dolphie.port}[/b light_blue]: "
                                f"{tab.worker_cancel_error.reason}"
                            ),
                            title="Connection Error",
                            severity="error",
                            timeout=10,
                        )

                    if not self.loading_hostgroups:
                        self.tab_manager.switch_tab(tab.id)

                        self.tab_manager.setup_host_tab(tab)
                        self.bell()
        elif event.worker.group == "replicas":
            tab.replicas_worker_running = False

            if event.state == WorkerState.SUCCESS:
                # Skip this if the conditions are right
                if len(self.screen_stack) > 1 or dolphie.pause_refresh or tab.id != self.tab_manager.active_tab.id:
                    tab.replicas_worker_timer = self.set_timer(
                        dolphie.refresh_interval, partial(self.run_worker_replicas, tab.id)
                    )
                    return

                if dolphie.panels.replication.visible and dolphie.replica_manager.available_replicas:
                    replication_panel.create_replica_panel(tab)

                tab.replicas_worker_timer = self.set_timer(
                    dolphie.refresh_interval, partial(self.run_worker_replicas, tab.id)
                )
        elif event.worker.group == "replay":
            tab.worker_running = False

            if event.state == WorkerState.SUCCESS:
                self.monitor_read_only_change(tab)

                if (
                    len(self.screen_stack) > 1
                    or (dolphie.pause_refresh and not tab.replay_manual_control)
                    or tab.id != self.tab_manager.active_tab.id
                ):
                    tab.worker_timer = self.set_timer(dolphie.refresh_interval, partial(self.run_worker_replay, tab.id))

                    return

                if not tab.main_container.display:
                    self.refresh_tab_properties()

                if dolphie.connection_source == ConnectionSource.mysql:
                    self.refresh_screen_mysql(tab)
                    replication_panel.create_replica_panel(tab)
                elif dolphie.connection_source == ConnectionSource.proxysql:
                    self.refresh_screen_proxysql(tab)

                tab.worker_timer = self.set_timer(dolphie.refresh_interval, partial(self.run_worker_replay, tab.id))

    def process_mysql_data(self, tab: Tab):
        dolphie = tab.dolphie

        global_variables = dolphie.main_db_connection.fetch_status_and_variables("variables")
        dolphie.detect_global_variable_change(old_data=dolphie.global_variables, new_data=global_variables)
        dolphie.global_variables = global_variables

        # At this point, we're connected so we need to do a few things
        if dolphie.connection_status == ConnectionStatus.connecting:
            self.tab_manager.update_connection_status(tab=tab, connection_status=ConnectionStatus.connected)
            dolphie.set_host_version(dolphie.global_variables.get("version"))
            dolphie.get_group_replication_metadata()
            dolphie.configure_mysql_variables()
            dolphie.validate_metadata_locks_enabled()

        dolphie.global_status = dolphie.main_db_connection.fetch_status_and_variables("status")
        dolphie.innodb_metrics = dolphie.main_db_connection.fetch_status_and_variables("innodb_metrics")

        if dolphie.performance_schema_enabled and dolphie.is_mysql_version_at_least("5.7"):
            if dolphie.connection_source_alt == ConnectionSource.mariadb:
                find_replicas_query = MySQLQueries.mariadb_find_replicas
            else:
                find_replicas_query = MySQLQueries.ps_find_replicas
        else:
            find_replicas_query = MySQLQueries.pl_find_replicas

        dolphie.main_db_connection.execute(find_replicas_query)
        available_replicas = dolphie.main_db_connection.fetchall()

        if dolphie.daemon_mode:
            dolphie.replica_manager.available_replicas = available_replicas
        else:
            # We update the replica ports used if the number of replicas have changed
            if len(available_replicas) != len(dolphie.replica_manager.available_replicas):
                dolphie.replica_manager.ports = {}

                if (
                    dolphie.is_mysql_version_at_least("8.0.22")
                    and dolphie.connection_source_alt != ConnectionSource.mariadb
                ):
                    dolphie.main_db_connection.execute(MySQLQueries.show_replicas)
                else:
                    dolphie.main_db_connection.execute(MySQLQueries.show_slave_hosts)

                replica_data = dolphie.main_db_connection.fetchall()
                for row in replica_data:
                    if dolphie.connection_source_alt == ConnectionSource.mariadb:
                        key = "Server_id"
                    else:
                        if dolphie.is_mysql_version_at_least("8.0.22"):
                            key = "Replica_UUID"
                        else:
                            key = "Slave_UUID"

                    dolphie.replica_manager.ports[row.get(key)] = {"port": row.get("Port"), "in_use": False}

                dolphie.replica_manager.available_replicas = available_replicas

        dolphie.main_db_connection.execute(MySQLQueries.ps_disk_io)
        dolphie.disk_io_metrics = dolphie.main_db_connection.fetchone()

        dolphie.replication_status = replication_panel.fetch_replication_data(tab)

        # If using MySQL 8, fetch the replication applier status data
        if (
            dolphie.is_mysql_version_at_least("8.0")
            and dolphie.panels.replication.visible
            and dolphie.global_variables.get("replica_parallel_workers", 0) > 1
        ):
            dolphie.main_db_connection.execute(MySQLQueries.replication_applier_status)
            dolphie.replication_applier_status = dolphie.main_db_connection.fetchall()

        if (
            not dolphie.daemon_mode
            and dolphie.is_mysql_version_at_least("8.0.30")
            and dolphie.connection_source_alt != ConnectionSource.mariadb
        ):
            active_redo_logs_count = dolphie.main_db_connection.fetch_value_from_field(
                MySQLQueries.active_redo_logs, "count"
            )
            dolphie.global_status["Active_redo_log_count"] = active_redo_logs_count

        # If the server doesn't support Innodb_lsn_current, use Innodb_os_log_written instead
        # which has less precision, but it's good enough. Used for calculating the percentage of redo log used
        if not dolphie.global_status.get("Innodb_lsn_current"):
            dolphie.global_status["Innodb_lsn_current"] = dolphie.global_status["Innodb_os_log_written"]

        if dolphie.group_replication or dolphie.innodb_cluster:
            if dolphie.is_mysql_version_at_least("8.0.13"):
                dolphie.group_replication_data["write_concurrency"] = dolphie.main_db_connection.fetch_value_from_field(
                    MySQLQueries.group_replication_get_write_concurrency, "write_concurrency"
                )

            dolphie.main_db_connection.execute(MySQLQueries.get_group_replication_members)
            dolphie.group_replication_members = dolphie.main_db_connection.fetchall()

        if dolphie.panels.dashboard.visible or dolphie.record_for_replay:
            if dolphie.is_mysql_version_at_least("8.2.0") and dolphie.connection_source_alt != ConnectionSource.mariadb:
                dolphie.main_db_connection.execute(MySQLQueries.show_binary_log_status)
            else:
                dolphie.main_db_connection.execute(MySQLQueries.show_master_status)

            previous_position = dolphie.binlog_status.get("Position")
            dolphie.binlog_status = dolphie.main_db_connection.fetchone()

            if previous_position is None:
                dolphie.binlog_status["Diff_Position"] = 0
            elif previous_position > dolphie.binlog_status["Position"]:
                dolphie.binlog_status["Diff_Position"] = "Binlog Rotated"
            else:
                dolphie.binlog_status["Diff_Position"] = dolphie.binlog_status["Position"] - previous_position

        if dolphie.panels.processlist.visible or dolphie.record_for_replay:
            dolphie.processlist_threads = processlist_panel.fetch_data(tab)

        if dolphie.is_mysql_version_at_least("5.7"):
            dolphie.metadata_locks = {}
            if dolphie.metadata_locks_enabled and (dolphie.panels.metadata_locks.visible or dolphie.record_for_replay):
                dolphie.metadata_locks = metadata_locks_panel.fetch_data(tab)

            if dolphie.panels.ddl.visible:
                dolphie.main_db_connection.execute(MySQLQueries.ddls)
                dolphie.ddl = dolphie.main_db_connection.fetchall()

    def process_proxysql_data(self, tab: Tab):
        dolphie = tab.dolphie

        global_variables = dolphie.main_db_connection.fetch_status_and_variables("variables")
        dolphie.detect_global_variable_change(old_data=dolphie.global_variables, new_data=global_variables)
        dolphie.global_variables = global_variables

        if dolphie.connection_status == ConnectionStatus.connecting:
            self.tab_manager.update_connection_status(tab=tab, connection_status=ConnectionStatus.connected)
            dolphie.set_host_version(dolphie.global_variables.get("admin-version"))

        dolphie.global_status = dolphie.main_db_connection.fetch_status_and_variables("mysql_stats")

        dolphie.main_db_connection.execute(ProxySQLQueries.command_stats)
        dolphie.proxysql_command_stats = dolphie.main_db_connection.fetchall()

        # Here, we're going to format the command stats to match the global status keys of
        # MySQL and get total count of queries
        total_queries_count = 0
        query_types_for_total = ["SELECT", "INSERT", "UPDATE", "DELETE", "REPLACE", "SET", "CALL"]
        for row in dolphie.proxysql_command_stats:
            total_cnt = 0
            if row["Command"] in query_types_for_total:
                total_cnt = int(row["Total_cnt"])
                total_queries_count += total_cnt

            dolphie.global_status[f"Com_{row['Command'].lower()}"] = total_cnt

        # Add the total queries to the global status
        dolphie.global_status["Queries"] = total_queries_count

        dolphie.main_db_connection.execute(ProxySQLQueries.connection_pool_data)
        data = dolphie.main_db_connection.fetchone()
        dolphie.global_status["proxysql_backend_host_average_latency"] = int(data.get("avg_latency", 0))

        if dolphie.global_status.get("Client_Connections_connected", 0):  # Don't divide by 0
            dolphie.global_status["proxysql_multiplex_efficiency_ratio"] = round(
                100
                - (
                    (
                        int(data.get("connection_pool_connections", 0))
                        / dolphie.global_status.get("Client_Connections_connected", 0)
                    )
                    * 100
                ),
                2,
            )
        else:
            dolphie.global_status["proxysql_multiplex_efficiency_ratio"] = 100

        if dolphie.panels.proxysql_hostgroup_summary.visible or dolphie.record_for_replay:
            dolphie.main_db_connection.execute(ProxySQLQueries.hostgroup_summary)

            previous_values = {}
            columns_to_calculate_per_sec = ["Queries", "Bytes_data_sent", "Bytes_data_recv"]

            # Store previous values for each row
            for row in dolphie.proxysql_hostgroup_summary:
                row_id = f"{row['hostgroup']}_{row['srv_host']}_{row['srv_port']}"

                for column_key in columns_to_calculate_per_sec:
                    previous_values.setdefault(row_id, {})[column_key] = int(row.get(column_key, 0))

            # Fetch the updated hostgroup summary
            dolphie.proxysql_hostgroup_summary = dolphie.main_db_connection.fetchall()

            # Calculate the values per second
            for row in dolphie.proxysql_hostgroup_summary:
                row_id = f"{row['hostgroup']}_{row['srv_host']}_{row['srv_port']}"

                if row_id in previous_values:  # Ensure we have previous values for this row_id
                    for column_key in columns_to_calculate_per_sec:
                        previous_value = previous_values[row_id].get(column_key, 0)
                        current_value = int(row.get(column_key, 0))

                        value_per_sec = (current_value - previous_value) / dolphie.polling_latency
                        row[f"{column_key}_per_sec"] = round(value_per_sec)

        if dolphie.panels.processlist.visible or dolphie.record_for_replay:
            dolphie.processlist_threads = proxysql_processlist_panel.fetch_data(tab)

        if dolphie.panels.proxysql_mysql_query_rules.visible:
            dolphie.main_db_connection.execute(ProxySQLQueries.query_rules_summary)
            dolphie.proxysql_mysql_query_rules = dolphie.main_db_connection.fetchall()

    def refresh_screen_proxysql(self, tab: Tab):
        dolphie = tab.dolphie

        if tab.loading_indicator.display or dolphie.replay_file:
            tab.loading_indicator.display = False

        # Loop each panel and refresh it
        for panel in dolphie.panels.get_all_panels():
            if panel.visible:
                # Skip the graphs panel since it's handled separately
                if panel.name == dolphie.panels.graphs.name:
                    continue

                self.refresh_panel(tab, panel.name)

                if panel.name == dolphie.panels.dashboard.name and dolphie.metric_manager.metrics.dml.Queries.values:
                    # Update the sparkline for queries per second
                    tab.sparkline.data = dolphie.metric_manager.metrics.dml.Queries.values
                    tab.sparkline.refresh()

        if dolphie.panels.graphs.visible:
            # Refresh the graph(s) for the selected tab
            self.update_graphs(tab.metric_graph_tabs.get_pane(tab.metric_graph_tabs.active).name)

        # We take a snapshot of the processlist to be used for commands
        # since the data can change after a key is pressed
        if not dolphie.daemon_mode:
            dolphie.processlist_threads_snapshot = dolphie.processlist_threads.copy()

    def refresh_screen_mysql(self, tab: Tab):
        dolphie = tab.dolphie

        if tab.loading_indicator.display or dolphie.replay_file:
            tab.loading_indicator.display = False
            self.layout_graphs(tab)

        # Loop each panel and refresh it
        for panel in dolphie.panels.get_all_panels():
            if panel.visible:
                # Skip the graphs panel since it's handled separately
                if panel.name == dolphie.panels.graphs.name:
                    continue

                self.refresh_panel(tab, panel.name)

                if panel.name == dolphie.panels.dashboard.name and dolphie.metric_manager.metrics.dml.Queries.values:
                    # Update the sparkline for queries per second
                    tab.sparkline.data = dolphie.metric_manager.metrics.dml.Queries.values
                    tab.sparkline.refresh()

        if dolphie.panels.graphs.visible:
            # Hide/show replication tab based on replication status
            if dolphie.replication_status:
                tab.metric_graph_tabs.show_tab("graph_tab_replication_lag")
            else:
                tab.metric_graph_tabs.hide_tab("graph_tab_replication_lag")

            if (dolphie.metadata_locks_enabled and dolphie.panels.metadata_locks.visible) or dolphie.replay_file:
                tab.metric_graph_tabs.show_tab("graph_tab_locks")
            else:
                tab.metric_graph_tabs.hide_tab("graph_tab_locks")

            # Refresh the graph(s) for the selected tab
            self.update_graphs(tab.metric_graph_tabs.get_pane(tab.metric_graph_tabs.active).name)

        # We take a snapshot of the processlist to be used for commands
        # since the data can change after a key is pressed
        if not dolphie.daemon_mode:
            dolphie.processlist_threads_snapshot = dolphie.processlist_threads.copy()

    def monitor_read_only_change(self, tab: Tab):
        dolphie = tab.dolphie

        if dolphie.connection_source == ConnectionSource.proxysql:
            return

        current_ro_status = dolphie.global_variables.get("read_only")
        formatted_ro_status = ConnectionStatus.read_only if current_ro_status == "ON" else ConnectionStatus.read_write
        status = "read-only" if current_ro_status == "ON" else "read/write"

        message = f"Host [highlight]{dolphie.host_with_port}[/highlight] is now [b highlight]{status}[/b highlight]"

        if current_ro_status == "ON" and not dolphie.replication_status and not dolphie.group_replication:
            message += " ([yellow]SHOULD BE READ/WRITE?[/yellow])"
        elif current_ro_status == "ON" and dolphie.group_replication and dolphie.is_group_replication_primary:
            message += " ([yellow]SHOULD BE READ/WRITE?[/yellow])"

        if (
            dolphie.connection_status in [ConnectionStatus.read_write, ConnectionStatus.read_only]
            and dolphie.connection_status != formatted_ro_status
        ):
            logger.warning(f"Read-only mode changed: {dolphie.connection_status} -> {formatted_ro_status}")
            self.app.notify(title="Read-only mode change", message=message, severity="warning", timeout=15)

            self.tab_manager.update_connection_status(tab=tab, connection_status=formatted_ro_status)
        elif dolphie.connection_status == ConnectionStatus.connected:
            self.tab_manager.update_connection_status(tab=tab, connection_status=formatted_ro_status)

        dolphie.connection_status = formatted_ro_status

    @work()
    async def connect_as_hostgroup(self, hostgroup: str):
        self.loading_hostgroups = True
        self.notify(f"Connecting to hosts in hostgroup [highlight]{hostgroup}", severity="information")

        for hostgroup_member in self.config.hostgroup_hosts.get(hostgroup, []):
            # We only want to switch if it's the first tab created
            switch_tab = True if not self.tab_manager.active_tab else False

            tab = await self.tab_manager.create_tab(hostgroup_member=hostgroup_member, switch_tab=switch_tab)

            self.run_worker_main(tab.id)
            self.run_worker_replicas(tab.id)

        # Wait for all workers to finish before notifying the user
        await asyncio.sleep(0.2)
        for tab in self.tab_manager.tabs.values():
            while tab.worker_running:
                await asyncio.sleep(0.1)

        self.loading_hostgroups = False
        self.notify(f"Finished connecting to hosts in hostgroup [highlight]{hostgroup}", severity="success")

    async def on_mount(self):
        self.tab_manager = TabManager(app=self.app, config=self.config)
        await self.tab_manager.create_ui_widgets()

        if self.config.hostgroup:
            self.connect_as_hostgroup(self.config.hostgroup)
        else:
            tab = await self.tab_manager.create_tab(tab_name="Initial Tab")

            if self.config.tab_setup:
                self.tab_manager.setup_host_tab(tab)
            elif self.tab_manager.active_tab.dolphie.replay_file:
                self.tab_manager.active_tab.replay_manager = ReplayManager(tab.dolphie)
                self.tab_manager.rename_tab(tab)
                self.tab_manager.update_connection_status(tab=tab, connection_status=ConnectionStatus.connected)
                self.run_worker_replay(self.tab_manager.active_tab.id)
            else:
                self.run_worker_main(self.tab_manager.active_tab.id)

                if not self.config.daemon_mode:
                    self.run_worker_replicas(self.tab_manager.active_tab.id)

        self.check_for_new_version()

    def _handle_exception(self, error: Exception) -> None:
        self.bell()
        self.exit(message=Traceback(show_locals=True, width=None, locals_max_length=5))

    @on(Button.Pressed, "#back_button")
    def replay_back(self):
        # Because of how get_next_refresh_interval works, we need to go back 2 to get the previous event
        self.tab_manager.active_tab.replay_manager.current_index -= 2
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
                max_replay_timestamp=self.tab_manager.active_tab.replay_manager.max_timestamp,
            ),
            command_get_input,
        )

    def refresh_tab_properties(self):
        tab = self.tab_manager.active_tab
        tab.main_container.display = True

        self.size_dashboard_sections(tab)

        # Hide all graph tabs so we can show the ones we want
        tabs = tab.metric_graph_tabs.query(TabPane)
        for graph_tab in tabs:
            tab.metric_graph_tabs.hide_tab(graph_tab.id)

        # Show the tabs that are for the current connection source
        for metric_instance in tab.dolphie.metric_manager.metrics.__dict__.values():
            if tab.dolphie.connection_source in metric_instance.connection_source:
                tab.metric_graph_tabs.show_tab(f"graph_tab_{metric_instance.tab_name}")

        if tab.dolphie.replay_file:
            tab.dashboard_replay_container.display = True
        else:
            tab.dashboard_replay_container.display = False

    @on(Tabs.TabActivated, "#host_tabs")
    def tab_changed(self, event: TabbedContent.TabActivated):
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

            if tab.dolphie.connection_source == ConnectionSource.mysql:
                self.refresh_screen_mysql(tab)
            elif tab.dolphie.connection_source == ConnectionSource.proxysql:
                self.refresh_screen_proxysql(tab)

            self.refresh_tab_properties()

            # Set the display state for the replica container based on whether there are replicas
            tab.replicas_container.display = bool(tab.dolphie.replica_manager.replicas)
            if tab.replicas_container.display:
                containers = self.query(".replica_container")
                for container in containers:
                    container.display = tab.id in container.id

                tab.replicas_title.update(
                    f"[b]Replicas ([highlight]{len(tab.dolphie.replica_manager.available_replicas)}[/highlight])\n"
                )

            # Set the display state for the group replication container based on whether there are members
            tab.group_replication_container.display = bool(tab.dolphie.group_replication_members)
            if tab.group_replication_container.display:
                containers = self.query(".member_container")
                for container in containers:
                    container.display = tab.id in container.id

    @on(TabbedContent.TabActivated, ".metrics_host_tabs")
    def metric_tab_changed(self, event: TabbedContent.TabActivated):
        metric_instance_name = event.pane.name

        if metric_instance_name:
            self.update_graphs(metric_instance_name)

    def update_graphs(self, tab_metric_instance_name: str):
        if not self.tab_manager.active_tab or not self.tab_manager.active_tab.panel_graphs.display:
            return

        for metric_instance in self.tab_manager.active_tab.dolphie.metric_manager.metrics.__dict__.values():
            if tab_metric_instance_name == metric_instance.tab_name:
                for graph_name in metric_instance.graphs:
                    getattr(self.tab_manager.active_tab, graph_name).render_graph(
                        metric_instance, self.tab_manager.active_tab.dolphie.metric_manager.datetimes
                    )

        self.update_stats_label(tab_metric_instance_name)

    def update_stats_label(self, tab_metric_instance_name: str):
        stat_data = {}

        for metric_instance in self.tab_manager.active_tab.dolphie.metric_manager.metrics.__dict__.values():
            if metric_instance.tab_name == tab_metric_instance_name:
                number_format_func = MetricManager.get_number_format_function(metric_instance, color=True)
                for metric_name, metric_data in metric_instance.__dict__.items():
                    if isinstance(metric_data, MetricManager.MetricData) and metric_data.values and metric_data.visible:
                        if f"graph_{metric_name}" in metric_instance.graphs:
                            stat_data[metric_data.label] = round(metric_data.values[-1])
                        else:
                            stat_data[metric_data.label] = number_format_func(metric_data.values[-1])

        formatted_stat_data = "  ".join(
            f"[b light_blue]{label}[/b light_blue] {value}" for label, value in stat_data.items()
        )
        getattr(self.tab_manager.active_tab, tab_metric_instance_name).update(formatted_stat_data)

    def toggle_panel(self, panel_name: str):
        # We store the panel objects in the tab object (i.e. tab.panel_dashboard, tab.panel_processlist, etc.)
        panel = self.tab_manager.active_tab.get_panel_widget(panel_name)

        new_display_status = not panel.display

        setattr(getattr(self.tab_manager.active_tab.dolphie.panels, panel_name), "visible", new_display_status)

        if panel_name not in [self.tab_manager.active_tab.dolphie.panels.graphs.name]:
            self.refresh_panel(self.tab_manager.active_tab, panel_name, toggled=True)

        panel.display = new_display_status

        self.force_refresh_for_replay(need_current_data=True)

    def force_refresh_for_replay(self, need_current_data: bool = False):
        # This function lets us force a refresh of the worker thread when we're in a replay
        tab = self.tab_manager.active_tab

        if tab.dolphie.replay_file:
            tab.worker.cancel()
            tab.worker_timer.stop()

            if need_current_data:
                # We subtract 1 because get_next_refresh_interval will increment the index
                tab.replay_manager.current_index -= 1

            self.run_worker_replay(tab.id, manual_control=True)

    def size_dashboard_sections(self, tab: Tab):
        if tab.dolphie.connection_source == ConnectionSource.mysql:
            # Update the sizes of the panels depending if replication container is visible or not
            if tab.dolphie.replication_status and not tab.dolphie.panels.replication.visible:
                tab.dashboard_section_1.styles.width = "25vw"
                tab.dashboard_section_2.styles.width = "17vw"
                tab.dashboard_section_3.styles.width = "21vw"
                tab.dashboard_section_4.styles.width = "12vw"
                tab.dashboard_section_5.styles.width = "25vw"

                tab.dashboard_section_5.display = True
            else:
                tab.dashboard_section_1.styles.width = "32vw"
                tab.dashboard_section_2.styles.width = "24vw"
                tab.dashboard_section_3.styles.width = "27vw"
                tab.dashboard_section_4.styles.width = "17vw"
                tab.dashboard_section_5.styles.width = "0"

                tab.dashboard_section_5.display = False

            tab.dashboard_section_1.styles.max_width = "45"
            tab.dashboard_section_2.styles.max_width = "32"
            tab.dashboard_section_3.styles.max_width = "38"
            tab.dashboard_section_4.styles.max_width = "22"
            tab.dashboard_section_5.styles.max_width = "55"
        elif tab.dolphie.connection_source == ConnectionSource.proxysql:
            tab.dashboard_section_1.styles.width = "24vw"
            tab.dashboard_section_2.styles.width = "20vw"
            tab.dashboard_section_3.styles.width = "22vw"
            tab.dashboard_section_4.styles.width = "13vw"

            tab.dashboard_section_5.display = False

            tab.dashboard_section_1.styles.max_width = "35"
            tab.dashboard_section_2.styles.max_width = "28"
            tab.dashboard_section_3.styles.max_width = "25"
            tab.dashboard_section_4.styles.max_width = "25"

    def refresh_panel(self, tab: Tab, panel_name: str, toggled: bool = False):
        panel_mapping = {
            tab.dolphie.panels.replication.name: {ConnectionSource.mysql: replication_panel},
            tab.dolphie.panels.dashboard.name: {
                ConnectionSource.mysql: dashboard_panel,
                ConnectionSource.proxysql: proxysql_dashboard_panel,
            },
            tab.dolphie.panels.processlist.name: {
                ConnectionSource.mysql: processlist_panel,
                ConnectionSource.proxysql: proxysql_processlist_panel,
            },
            tab.dolphie.panels.metadata_locks.name: {ConnectionSource.mysql: metadata_locks_panel},
            tab.dolphie.panels.ddl.name: {ConnectionSource.mysql: ddl_panel},
            tab.dolphie.panels.proxysql_hostgroup_summary.name: {
                ConnectionSource.proxysql: proxysql_hostgroup_summary_panel
            },
            tab.dolphie.panels.proxysql_mysql_query_rules.name: {
                ConnectionSource.proxysql: proxysql_mysql_query_rules_panel
            },
            tab.dolphie.panels.proxysql_command_stats.name: {ConnectionSource.proxysql: proxysql_command_stats_panel},
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
                dashboard_panel.create_panel(tab)

    def layout_graphs(self, tab: Tab):
        # These variables are dynamically created
        if tab.dolphie.is_mysql_version_at_least("8.0.30"):
            if tab.dolphie.replay_file:
                tab.graph_redo_log_data_written.styles.width = "88%"
                tab.graph_redo_log_bar.styles.width = "12%"
                tab.graph_redo_log_active_count.display = False
            else:
                tab.graph_redo_log_data_written.styles.width = "55%"
                tab.graph_redo_log_bar.styles.width = "12%"
                tab.graph_redo_log_active_count.styles.width = "33%"
                tab.graph_redo_log_active_count.display = True
                tab.dolphie.metric_manager.metrics.redo_log_active_count.Active_redo_log_count.visible = True
        else:
            tab.graph_redo_log_data_written.styles.width = "88%"
            tab.graph_redo_log_bar.styles.width = "12%"
            tab.graph_redo_log_active_count.display = False

        tab.graph_adaptive_hash_index.styles.width = "50%"
        tab.graph_adaptive_hash_index_hit_ratio.styles.width = "50%"

    @on(Switch.Changed)
    def switch_changed(self, event: Switch.Changed):
        if len(self.screen_stack) > 1 or not self.tab_manager.active_tab:
            return

        event_metric_instance_name = event.switch.name
        metric = event.switch.id

        # Loop all metric instances and set the visibility of the metric
        for tab in self.tab_manager.tabs.values():
            for metric_instance_name, metric_instance in tab.dolphie.metric_manager.metrics.__dict__.items():
                if (
                    tab.dolphie.connection_source in metric_instance.connection_source
                    and event_metric_instance_name == metric_instance_name
                ):
                    metric_data: MetricManager.MetricData = getattr(metric_instance, metric)
                    metric_data.visible = event.value

        self.update_graphs(event_metric_instance_name)

    async def on_key(self, event: events.Key):
        if len(self.screen_stack) > 1:
            return

        await self.capture_key(event.key)

    async def capture_key(self, key):
        tab = self.tab_manager.active_tab
        if not tab:
            return

        screen_data = None
        dolphie = tab.dolphie

        if key not in self.command_manager.exclude_keys:
            if not self.command_manager.get_commands(dolphie.replay_file, dolphie.connection_source).get(key):
                self.notify(f"Key [highlight]{key}[/highlight] is not a valid command", severity="warning")
                return

            # Prevent commands from being run if the secondary connection is processing a query already
            if dolphie.secondary_db_connection and dolphie.secondary_db_connection.is_running_query:
                self.notify("There's already a command running - please wait for it to finish")
                return

            if not dolphie.main_db_connection.is_connected() and not dolphie.replay_file:
                self.notify("You must be connected to a host to use commands")
                return

        if self.loading_hostgroups:
            self.notify("You can't run commands while hosts are connecting as a hostgroup")
            return

        if key == "1":
            self.toggle_panel(dolphie.panels.dashboard.name)

        elif key == "2":
            self.tab_manager.active_tab.processlist_datatable.clear()
            self.toggle_panel(dolphie.panels.processlist.name)

            tab.processlist_title.update("Processlist ([highlight]0[/highlight])")

        elif key == "3":
            self.toggle_panel(dolphie.panels.graphs.name)
            self.app.update_graphs("dml")

        elif key == "4":
            if dolphie.connection_source == ConnectionSource.proxysql:
                self.toggle_panel(dolphie.panels.proxysql_hostgroup_summary.name)
                dolphie.proxysql_per_second_data = {}
                self.tab_manager.active_tab.proxysql_hostgroup_summary_datatable.clear()

                return

            # If we're in replay mode and there's no replication status, replicas or group replication, stop here
            if dolphie.replay_file and (
                not dolphie.replication_status
                and not dolphie.replica_manager.replicas
                and not dolphie.group_replication_members
            ):
                self.notify("This replay file has no replication data")
                return

            self.toggle_panel(dolphie.panels.replication.name)
            self.size_dashboard_sections(tab)

            if dolphie.panels.replication.visible:
                if dolphie.replica_manager.available_replicas:
                    tab.replicas_container.display = True

                    # No loading animation necessary for replay mode
                    if not dolphie.replay_file:
                        tab.replicas_loading_indicator.display = True

                        tab.replicas_title.update(
                            f"[b]Loading [highlight]{len(dolphie.replica_manager.available_replicas)}[/highlight]"
                            " replicas...\n"
                        )

                    for container in dolphie.app.query(".replica_container"):
                        container.display = tab.id in container.id

                if dolphie.group_replication_members:
                    tab.group_replication_container.display = True
                    for container in dolphie.app.query(".member_container"):
                        container.display = tab.id in container.id
            else:
                queries = [f".replica_container_{dolphie.tab_id}", f".member_container_{dolphie.tab_id}"]
                for query in queries:
                    for container in dolphie.app.query(query):
                        container.remove()

        elif key == "5":
            if dolphie.connection_source == ConnectionSource.proxysql:
                self.toggle_panel(dolphie.panels.proxysql_mysql_query_rules.name)
                return

            if not dolphie.metadata_locks_enabled and not dolphie.replay_file:
                self.notify(
                    "Metadata Locks panel requires MySQL 5.7+ with Performance Schema enabled along with "
                    "[highlight]wait/lock/metadata/sql/mdl[/highlight] enabled in setup_instruments table"
                )
                return

            self.toggle_panel(dolphie.panels.metadata_locks.name)
            self.tab_manager.active_tab.metadata_locks_datatable.clear()
            tab.metadata_locks_title.update("Metadata Locks ([highlight]0[/highlight])")

        elif key == "6":
            if dolphie.connection_source == ConnectionSource.proxysql:
                self.toggle_panel(dolphie.panels.proxysql_command_stats.name)
            else:
                if not dolphie.is_mysql_version_at_least("5.7") or not dolphie.performance_schema_enabled:
                    self.notify("DDL panel requires MySQL 5.7+ with Performance Schema enabled")
                    return

                query = (
                    "SELECT enabled FROM performance_schema.setup_instruments WHERE name LIKE 'stage/innodb/alter%';"
                )
                dolphie.secondary_db_connection.execute(query)
                data = dolphie.secondary_db_connection.fetchall()
                for row in data:
                    if row.get("enabled") == "NO":
                        self.notify("DDL panel requires Performance Schema to have 'stage/innodb/alter%' enabled")
                        return

                self.toggle_panel(dolphie.panels.ddl.name)
                self.tab_manager.active_tab.ddl_datatable.clear()
                tab.ddl_title.update("DDL ([highlight]0[/highlight])")

        elif key == "grave_accent":
            self.tab_manager.setup_host_tab(tab)

        elif key == "space":
            if tab.worker.state != WorkerState.RUNNING:
                tab.worker_timer.stop()
                self.run_worker_main(tab.id)

        elif key == "plus":
            new_tab = await self.tab_manager.create_tab(tab_name="New Tab")
            self.tab_manager.switch_tab(new_tab.id)
            self.tab_manager.setup_host_tab(new_tab)

        elif key == "equals_sign":

            def command_get_input(tab_name):
                tab.manual_tab_name = tab_name
                self.tab_manager.rename_tab(tab, tab_name)

            self.app.push_screen(
                CommandModal(command=HotkeyCommands.rename_tab, message="What would you like to rename the tab to?"),
                command_get_input,
            )

        elif key == "minus":
            if len(self.tab_manager.tabs) == 1:
                self.notify("Removing all tabs is not permitted", severity="error")
            else:
                if not self.tab_manager.active_tab:
                    self.notify("No active tab to remove", severity="error")
                    return

                await self.tab_manager.remove_tab(tab)
                await self.tab_manager.disconnect_tab(tab=tab, update_topbar=False)

                self.notify(f"Tab [highlight]{tab.name}[/highlight] [white]has been removed", severity="success")
                self.tab_manager.tabs.pop(tab.id, None)
        elif key == "left_square_bracket":
            if dolphie.replay_file:
                self.query_one("#back_button", Button).press()

        elif key == "right_square_bracket":
            if dolphie.replay_file:
                self.query_one("#forward_button", Button).press()

        elif key == "ctrl+a" or key == "ctrl+d":
            if key == "ctrl+a":
                self.tab_manager.host_tabs.action_previous_tab()
            elif key == "ctrl+d":
                self.tab_manager.host_tabs.action_next_tab()

        elif key == "a":
            if dolphie.show_additional_query_columns:
                dolphie.show_additional_query_columns = False
                self.notify("Processlist will now hide additional columns")
            else:
                dolphie.show_additional_query_columns = True
                self.notify("Processlist will now show additional columns")

            self.force_refresh_for_replay(need_current_data=True)

        elif key == "c":
            dolphie.user_filter = ""
            dolphie.db_filter = ""
            dolphie.host_filter = ""
            dolphie.hostgroup_filter = ""
            dolphie.query_time_filter = ""
            dolphie.query_filter = ""

            self.force_refresh_for_replay(need_current_data=True)

            self.notify("Cleared all filters", severity="success")

        elif key == "C":
            if not dolphie.global_variables.get("innodb_thread_concurrency"):
                self.notify("InnoDB thread concurrency is not setup", severity="warning")
                return

            if dolphie.show_threads_with_concurrency_tickets:
                dolphie.show_threads_with_concurrency_tickets = False
                dolphie.show_idle_threads = False
                self.notify("Processlist will no longer only show threads with concurrency tickets")
            else:
                dolphie.show_threads_with_concurrency_tickets = True
                dolphie.show_idle_threads = True
                self.notify("Processlist will only show threads with concurrency tickets")

            self.force_refresh_for_replay(need_current_data=True)

        elif key == "d":
            self.run_command_in_worker(key=key, dolphie=dolphie)

        elif key == "D":
            await self.tab_manager.disconnect_tab(tab)

        elif key == "e":
            if dolphie.connection_source_alt == ConnectionSource.mariadb:
                self.notify(f"Command [highlight]{key}[/highlight] is only available for MySQL connections")
            elif dolphie.connection_source == ConnectionSource.proxysql:
                self.run_command_in_worker(key=key, dolphie=dolphie)
            else:
                if dolphie.is_mysql_version_at_least("8.0") and dolphie.performance_schema_enabled:
                    self.app.push_screen(
                        EventLog(
                            dolphie.connection_status,
                            dolphie.app_version,
                            dolphie.host_with_port,
                            dolphie.secondary_db_connection,
                        )
                    )
                else:
                    self.notify("Error log command requires MySQL 8+ with Performance Schema enabled")

        elif key == "E":
            processlist = dolphie.processlist_threads_snapshot or dolphie.processlist_threads
            if processlist:
                # Extract headers from the first entry's thread_data
                first_entry = next(iter(processlist.values()))
                headers = first_entry.thread_data.keys()

                # Generate the filename with a timestamp prefix
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"processlist-{timestamp}.csv"

                # Write the CSV to a file
                with open(filename, "w", newline="") as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=headers)

                    # Write the headers and rows
                    writer.writeheader()
                    for process_thread in processlist.values():
                        writer.writerow(process_thread.thread_data)

                self.notify(
                    f"Processlist has been exported to CSV file [highlight]{filename}", severity="success", timeout=10
                )
            else:
                self.notify("There's no processlist data to export", severity="warning")

        elif key == "f":

            def command_get_input(filter_data):
                # Unpack the data from the modal
                filters_mapping = {
                    "User": "user_filter",
                    "Host": "host_filter",
                    "Database": "db_filter",
                    "Hostgroup": "hostgroup_filter",
                    "Minimum Query Time": "query_time_filter",
                    "Partial Query Text": "query_filter",
                }

                filters = dict(zip(filters_mapping.keys(), filter_data))

                # Apply filters and notify the user for each valid input
                for filter_name, filter_value in filters.items():
                    if filter_value:
                        setattr(dolphie, filters_mapping[filter_name], filter_value)
                        self.notify(
                            f"[b]{filter_name}[/b]: [b highlight]{filter_value}[/b highlight]",
                            title="Filter applied",
                            severity="success",
                        )

                # Refresh data after applying filters
                self.force_refresh_for_replay(need_current_data=True)

            self.app.push_screen(
                CommandModal(
                    command=HotkeyCommands.thread_filter,
                    message="Filter threads by field(s)",
                    processlist_data=dolphie.processlist_threads_snapshot,
                    host_cache_data=dolphie.host_cache,
                    connection_source=dolphie.connection_source,
                ),
                command_get_input,
            )

        elif key == "i":
            if dolphie.show_idle_threads:
                dolphie.show_idle_threads = False
                dolphie.sort_by_time_descending = True

                self.notify("Processlist will now hide idle threads")
            else:
                dolphie.show_idle_threads = True
                dolphie.sort_by_time_descending = False

                self.notify("Processlist will now show idle threads")

        elif key == "k":

            def command_get_input(data):
                self.run_command_in_worker(key=key, dolphie=dolphie, additional_data=data)

            self.app.push_screen(
                CommandModal(
                    command=HotkeyCommands.thread_kill_by_id,
                    message="Kill Thread",
                    processlist_data=dolphie.processlist_threads_snapshot,
                ),
                command_get_input,
            )

        elif key == "K":

            def command_get_input(data):
                self.run_command_in_worker(key=key, dolphie=dolphie, additional_data=data)

            self.app.push_screen(
                CommandModal(
                    command=HotkeyCommands.thread_kill_by_parameter,
                    message="Kill threads by parameter(s)",
                    processlist_data=dolphie.processlist_threads_snapshot,
                ),
                command_get_input,
            )

        elif key == "l":
            self.run_command_in_worker(key=key, dolphie=dolphie)

        elif key == "o":
            self.run_command_in_worker(key=key, dolphie=dolphie)

        elif key == "m":
            if dolphie.connection_source == ConnectionSource.proxysql:
                self.run_command_in_worker(key=key, dolphie=dolphie)
                return

            if not dolphie.is_mysql_version_at_least("5.7") or not dolphie.performance_schema_enabled:
                self.notify("Memory usage command requires MySQL 5.7+ with Performance Schema enabled")
            else:
                self.run_command_in_worker(key=key, dolphie=dolphie)

        elif key == "p":
            if dolphie.replay_file:
                self.query_one("#pause_button", Button).press()
            else:
                if not dolphie.pause_refresh:
                    dolphie.pause_refresh = True
                    self.notify(f"Refresh is paused! Press [b highlight]{key}[/b highlight] again to resume")
                else:
                    dolphie.pause_refresh = False
                    self.notify("Refreshing has resumed", severity="success")

        if key == "P":
            if dolphie.use_performance_schema:
                dolphie.use_performance_schema = False
                self.notify("Switched to using [b highlight]Processlist")
            else:
                if dolphie.performance_schema_enabled:
                    dolphie.use_performance_schema = True
                    self.notify("Switched to using [b highlight]Performance Schema")
                else:
                    self.notify("You can't switch to Performance Schema because it isn't enabled")

        elif key == "q":
            self.app.exit()

        elif key == "r":

            def command_get_input(refresh_interval):
                dolphie.refresh_interval = refresh_interval

                self.notify(
                    f"Refresh interval set to [b highlight]{refresh_interval}[/b highlight] second(s)",
                    severity="success",
                )

            self.app.push_screen(
                CommandModal(HotkeyCommands.refresh_interval, message="Refresh Interval"),
                command_get_input,
            )

        elif key == "R":
            dolphie.metric_manager.reset()

            self.update_graphs(tab.metric_graph_tabs.get_pane(tab.metric_graph_tabs.active).name)
            dolphie.update_switches_after_reset()
            self.notify("Metrics have been reset", severity="success")

        elif key == "s":
            if dolphie.sort_by_time_descending:
                dolphie.sort_by_time_descending = False
                self.notify("Processlist will now sort threads by time in ascending order")
            else:
                dolphie.sort_by_time_descending = True
                self.notify("Processlist will now sort threads by time in descending order")

            self.force_refresh_for_replay(need_current_data=True)

        elif key == "S":
            if dolphie.replay_file:
                self.query_one("#seek_button", Button).press()

        elif key == "t":

            if dolphie.connection_source == ConnectionSource.proxysql:

                def command_get_input(data):
                    thread_table = Table(box=None, show_header=False)
                    thread_table.add_column("")
                    thread_table.add_column("", overflow="fold")

                    thread_id = data
                    thread_data: ProxySQLProcesslistThread = dolphie.processlist_threads_snapshot.get(thread_id)
                    if not thread_data:
                        self.notify(f"Thread ID [highlight]{thread_id}[/highlight] was not found", severity="error")
                        return

                    thread_table.add_row("[label]Process ID", thread_id)
                    thread_table.add_row("[label]Hostgroup", str(thread_data.hostgroup))
                    thread_table.add_row("[label]User", thread_data.user)
                    thread_table.add_row("[label]Frontend Host", thread_data.frontend_host)
                    thread_table.add_row("[label]Backend Host", thread_data.host)
                    thread_table.add_row("[label]Database", thread_data.db)
                    thread_table.add_row("[label]Command", thread_data.command)
                    thread_table.add_row("[label]Time", str(timedelta(seconds=thread_data.time)).zfill(8))

                    formatted_query = None
                    if thread_data.formatted_query.code:
                        query = sqlformat(thread_data.formatted_query.code, reindent_aligned=True)
                        formatted_query = format_query(query, minify=False)

                    self.app.push_screen(
                        ProxySQLThreadScreen(
                            connection_status=dolphie.connection_status,
                            app_version=dolphie.app_version,
                            host=dolphie.host_with_port,
                            thread_table=thread_table,
                            query=formatted_query,
                            extended_info=thread_data.extended_info,
                        )
                    )

            else:

                def command_get_input(data):
                    self.run_command_in_worker(key=key, dolphie=dolphie, additional_data=data)

            self.app.push_screen(
                CommandModal(
                    command=HotkeyCommands.show_thread,
                    message="Thread Details",
                    processlist_data=dolphie.processlist_threads_snapshot,
                ),
                command_get_input,
            )

        elif key == "T":
            if dolphie.show_trxs_only:
                dolphie.show_trxs_only = False
                dolphie.show_idle_threads = False
                self.notify("Processlist will no longer only show threads that have an active transaction")
            else:
                dolphie.show_trxs_only = True
                dolphie.show_idle_threads = True
                self.notify("Processlist will only show threads that have an active transaction")

            self.force_refresh_for_replay(need_current_data=True)
        elif key == "u":
            if not dolphie.performance_schema_enabled and dolphie.connection_source != ConnectionSource.proxysql:
                self.notify("User statistics command requires Performance Schema to be enabled")
                return

            self.run_command_in_worker(key=key, dolphie=dolphie)

        elif key == "v":

            def command_get_input(input_variable):
                table_grid = Table.grid()
                table_counter = 1
                variable_counter = 1
                row_counter = 1
                variable_num = 1
                all_tables = []
                tables = {}
                display_global_variables = {}

                for variable, value in dolphie.global_variables.items():
                    if input_variable == "all":
                        display_global_variables[variable] = dolphie.global_variables[variable]
                    else:
                        if input_variable:
                            if input_variable in variable:
                                display_global_variables[variable] = dolphie.global_variables[variable]

                max_num_tables = 1 if len(display_global_variables) <= 50 else 2

                # Create the number of tables we want
                while table_counter <= max_num_tables:
                    tables[table_counter] = Table(box=box.HORIZONTALS, show_header=False, style="table_border")
                    tables[table_counter].add_column("")
                    tables[table_counter].add_column("")

                    table_counter += 1

                # Calculate how many global_variables per table
                row_per_count = len(display_global_variables) // max_num_tables

                # Loop global_variables
                for variable, value in display_global_variables.items():
                    tables[variable_num].add_row("[label]%s" % variable, str(value))

                    if variable_counter == row_per_count and row_counter != max_num_tables:
                        row_counter += 1
                        variable_counter = 0
                        variable_num += 1

                    variable_counter += 1

                # Put all the variable data from dict into an array
                all_tables = [table_data for table_data in tables.values() if table_data]

                # Add the data into a single tuple for add_row
                if display_global_variables:
                    table_grid.add_row(*all_tables)
                    screen_data = Align.center(table_grid)

                    self.app.push_screen(
                        CommandScreen(
                            dolphie.connection_status, dolphie.app_version, dolphie.host_with_port, screen_data
                        )
                    )
                else:
                    if input_variable:
                        self.notify("No variable(s) found that match [b highlight]%s[/b highlight]" % input_variable)

            self.app.push_screen(
                CommandModal(HotkeyCommands.variable_search, message="Specify a variable to wildcard search"),
                command_get_input,
            )

        elif key == "z":
            if dolphie.host_cache:
                table = Table(
                    box=box.SIMPLE_HEAVY,
                    show_edge=False,
                    style="table_border",
                )
                table.add_column("Host/IP")
                table.add_column("Hostname (if resolved)")

                for ip, addr in dolphie.host_cache.items():
                    if ip:
                        table.add_row(ip, addr)

                screen_data = Group(
                    Align.center(
                        "[b light_blue]Host Cache[/b light_blue] ([b highlight]%s[/b highlight])\n"
                        % len(dolphie.host_cache)
                    ),
                    table,
                )
            else:
                self.notify("There are currently no hosts resolved")

        if screen_data:
            self.app.push_screen(
                CommandScreen(dolphie.connection_status, dolphie.app_version, dolphie.host_with_port, screen_data)
            )

    @work(thread=True)
    def run_command_in_worker(self, key: str, dolphie: Dolphie, additional_data=None):
        tab = self.tab_manager.active_tab

        # These are the screens to display we use for the commands
        def show_command_screen():
            self.app.push_screen(
                CommandScreen(dolphie.connection_status, dolphie.app_version, dolphie.host_with_port, screen_data)
            )

        def show_thread_screen():
            self.app.push_screen(
                ThreadScreen(
                    connection_status=dolphie.connection_status,
                    app_version=dolphie.app_version,
                    host=dolphie.host_with_port,
                    thread_table=thread_table,
                    user_thread_attributes_table=user_thread_attributes_table,
                    query=formatted_query,
                    explain_data=explain_data,
                    explain_json_data=explain_json_data,
                    explain_failure=explain_failure,
                    transaction_history_table=transaction_history_table,
                )
            )

        tab.spinner.show()

        try:
            if key == "d":
                tables = {}
                all_tables = []

                db_count = dolphie.secondary_db_connection.execute(MySQLQueries.databases)
                databases = dolphie.secondary_db_connection.fetchall()

                # Determine how many tables to provide data
                max_num_tables = 1 if db_count <= 20 else 3

                # Calculate how many databases per table
                row_per_count = db_count // max_num_tables

                # Create dictionary of tables
                for table_counter in range(1, max_num_tables + 1):
                    table_box = box.HORIZONTALS
                    if max_num_tables == 1:
                        table_box = None

                    tables[table_counter] = Table(box=table_box, show_header=False, style="table_border")
                    tables[table_counter].add_column("")

                # Loop over databases
                db_counter = 1
                table_counter = 1

                # Sort the databases by name
                for database in databases:
                    tables[table_counter].add_row(database["SCHEMA_NAME"])
                    db_counter += 1

                    if db_counter > row_per_count and table_counter < max_num_tables:
                        table_counter += 1
                        db_counter = 1

                # Collect table data into an array
                all_tables = [table_data for table_data in tables.values() if table_data]

                table_grid = Table.grid()
                table_grid.add_row(*all_tables)

                screen_data = Group(
                    Align.center("[b light_blue]Databases[/b light_blue] ([b highlight]%s[/b highlight])\n" % db_count),
                    Align.center(table_grid),
                )

                self.call_from_thread(show_command_screen)

            elif key == "e":
                header_style = Style(bold=True)
                table = Table(box=box.SIMPLE_HEAVY, style="table_border", show_edge=False)
                table.add_column("Hostgroup", header_style=header_style)
                table.add_column("Backend Host", max_width=35, header_style=header_style)
                table.add_column("Username", header_style=header_style)
                table.add_column("Schema", header_style=header_style)
                table.add_column("First Seen", header_style=header_style)
                table.add_column("Last Seen", header_style=header_style)
                table.add_column("Count", header_style=header_style)
                table.add_column("Error", header_style=header_style, overflow="fold")

                dolphie.secondary_db_connection.execute(ProxySQLQueries.query_errors)
                data = dolphie.secondary_db_connection.fetchall()

                for row in data:
                    table.add_row(
                        row.get("hostgroup"),
                        f"{dolphie.get_hostname(row.get('hostname'))}:{row.get('port')}",
                        row.get("username"),
                        row.get("schemaname"),
                        str(datetime.fromtimestamp(int(row.get("first_seen", 0)))),
                        str(datetime.fromtimestamp(int(row.get("last_seen", 0)))),
                        format_number(int(row.get("count_star", 0))),
                        "[b][highlight]%s[/b][/highlight]: %s" % (row.get("errno", 0), row.get("last_error")),
                    )

                screen_data = Group(
                    Align.center(f"[b light_blue]Query Errors ([highlight]{table.row_count}[/highlight])\n"),
                    Align.center(table),
                )

                self.call_from_thread(show_command_screen)
            elif key == "k":
                thread_id = additional_data
                try:
                    if dolphie.connection_source_alt == ConnectionSource.aws_rds:
                        dolphie.secondary_db_connection.execute("CALL mysql.rds_kill(%s)" % thread_id)
                    elif dolphie.connection_source_alt == ConnectionSource.azure_mysql:
                        dolphie.secondary_db_connection.execute("CALL mysql.az_kill(%s)" % thread_id)
                    elif dolphie.connection_source == ConnectionSource.proxysql:
                        dolphie.secondary_db_connection.execute("KILL CONNECTION %s" % thread_id)
                    else:
                        dolphie.secondary_db_connection.execute("KILL %s" % thread_id)

                    self.notify("Killed Thread ID [b highlight]%s[/b highlight]" % thread_id, severity="success")
                except ManualException as e:
                    self.notify(e.reason, title="Error killing Thread ID", severity="error")

            elif key == "K":

                # Unpack the data from the modal
                (
                    kill_by_username,
                    kill_by_host,
                    kill_by_age_range,
                    age_range_lower_limit,
                    age_range_upper_limit,
                    kill_by_query_text,
                    include_sleeping_queries,
                ) = additional_data

                threads_killed = 0
                commands_to_kill = ["Query", "Execute"]

                if include_sleeping_queries:
                    commands_to_kill.append("Sleep")

                # Make a copy of the threads snapshot to avoid modification during iteration
                threads = dolphie.processlist_threads_snapshot.copy()

                for thread_id, thread in threads.items():
                    thread: ProcesslistThread
                    try:
                        # Check if the thread matches all conditions
                        if (
                            thread.command in commands_to_kill
                            and (not kill_by_username or kill_by_username == thread.user)
                            and (not kill_by_host or kill_by_host == thread.host)
                            and (not kill_by_age_range or age_range_lower_limit <= thread.time <= age_range_upper_limit)
                            and (not kill_by_query_text or kill_by_query_text in thread.formatted_query.code)
                        ):
                            if dolphie.connection_source_alt == ConnectionSource.aws_rds:
                                query = "CALL mysql.rds_kill(%s)"
                            elif dolphie.connection_source_alt == ConnectionSource.azure_mysql:
                                query = "CALL mysql.az_kill(%s)"
                            elif dolphie.connection_source == ConnectionSource.proxysql:
                                query = "KILL CONNECTION %s"
                            else:
                                query = "KILL %s"

                            dolphie.secondary_db_connection.execute(query % thread_id)
                            threads_killed += 1
                    except ManualException as e:
                        self.notify(e.reason, title=f"Error Killing Thread ID {thread_id}", severity="error")

                if threads_killed:
                    self.notify(f"Killed [highlight]{threads_killed}[/highlight] thread(s)")
                else:
                    self.notify("No threads were killed")

            elif key == "l":
                deadlock = ""
                output = re.search(
                    r"------------------------\nLATEST\sDETECTED\sDEADLOCK\n------------------------"
                    "\n(.*?)------------\nTRANSACTIONS",
                    dolphie.secondary_db_connection.fetch_value_from_field(MySQLQueries.innodb_status, "Status"),
                    flags=re.S,
                )
                if output:
                    deadlock = output.group(1)

                    deadlock = deadlock.replace("***", "[yellow]*****[/yellow]")
                    screen_data = deadlock
                else:
                    screen_data = Align.center("No deadlock detected")

                self.call_from_thread(show_command_screen)

            elif key == "o":
                screen_data = dolphie.secondary_db_connection.fetch_value_from_field(
                    MySQLQueries.innodb_status, "Status"
                )

                self.call_from_thread(show_command_screen)

            elif key == "m":
                header_style = Style(bold=True)

                if dolphie.connection_source == ConnectionSource.proxysql:
                    table = Table(box=box.SIMPLE_HEAVY, style="table_border", show_edge=False)
                    table.add_column("Variable", header_style=header_style)
                    table.add_column("Value", header_style=header_style)

                    dolphie.secondary_db_connection.execute(ProxySQLQueries.memory_metrics)
                    data = dolphie.secondary_db_connection.fetchall()

                    for row in data:
                        if row["Variable_Name"]:
                            table.add_row(f"{row['Variable_Name']}", f"{format_bytes(int(row['Variable_Value']))}")

                    screen_data = Group(Align.center("[b light_blue]Memory Usage[/b light_blue]"), Align.center(table))

                    self.call_from_thread(show_command_screen)
                else:
                    table_grid = Table.grid()
                    table1 = Table(box=box.SIMPLE_HEAVY, style="table_border")

                    table1.add_column("User", header_style=header_style)
                    table1.add_column("Current", header_style=header_style)
                    table1.add_column("Total", header_style=header_style)

                    dolphie.secondary_db_connection.execute(MySQLQueries.memory_by_user)
                    data = dolphie.secondary_db_connection.fetchall()
                    for row in data:
                        table1.add_row(
                            row["user"],
                            format_sys_table_memory(row["current_allocated"]),
                            format_sys_table_memory(row["total_allocated"]),
                        )

                    table2 = Table(box=box.SIMPLE_HEAVY, style="table_border")
                    table2.add_column("Code Area", header_style=header_style)
                    table2.add_column("Current", header_style=header_style)

                    dolphie.secondary_db_connection.execute(MySQLQueries.memory_by_code_area)
                    data = dolphie.secondary_db_connection.fetchall()
                    for row in data:
                        table2.add_row(row["code_area"], format_sys_table_memory(row["current_allocated"]))

                    table3 = Table(box=box.SIMPLE_HEAVY, style="table_border")
                    table3.add_column("Host", header_style=header_style)
                    table3.add_column("Current", header_style=header_style)
                    table3.add_column("Total", header_style=header_style)

                    dolphie.secondary_db_connection.execute(MySQLQueries.memory_by_host)
                    data = dolphie.secondary_db_connection.fetchall()
                    for row in data:
                        table3.add_row(
                            dolphie.get_hostname(row["host"]),
                            format_sys_table_memory(row["current_allocated"]),
                            format_sys_table_memory(row["total_allocated"]),
                        )

                    table_grid.add_row("", Align.center("[b light_blue]Memory Allocation"), "")
                    table_grid.add_row(table1, table3, table2)

                    screen_data = Align.center(table_grid)

                    self.call_from_thread(show_command_screen)
            elif key == "t":
                formatted_query = ""
                explain_failure = ""
                explain_data = ""
                explain_json_data = ""

                thread_table = Table(box=None, show_header=False)
                thread_table.add_column("")
                thread_table.add_column("", overflow="fold")

                thread_id = additional_data
                thread_data: ProcesslistThread = dolphie.processlist_threads_snapshot.get(thread_id)
                if not thread_data:
                    self.notify(f"Thread ID [highlight]{thread_id}[/highlight] was not found", severity="error")
                    return

                thread_table.add_row("[label]Thread ID", thread_id)
                thread_table.add_row("[label]User", thread_data.user)
                thread_table.add_row("[label]Host", thread_data.host)
                thread_table.add_row("[label]Database", thread_data.db)
                thread_table.add_row("[label]Command", thread_data.command)
                thread_table.add_row("[label]State", thread_data.state)
                thread_table.add_row("[label]Time", str(timedelta(seconds=thread_data.time)).zfill(8))
                thread_table.add_row("[label]Rows Locked", format_number(thread_data.trx_rows_locked))
                thread_table.add_row("[label]Rows Modified", format_number(thread_data.trx_rows_modified))

                thread_table.add_row("", "")
                thread_table.add_row("[label]TRX Time", thread_data.trx_time)
                thread_table.add_row("[label]TRX State", thread_data.trx_state)
                thread_table.add_row("[label]TRX Operation", thread_data.trx_operation_state)

                if thread_data.formatted_query.code:
                    query = sqlformat(thread_data.formatted_query.code, reindent_aligned=True)
                    query_db = thread_data.db

                    formatted_query = format_query(query, minify=False)

                    if query_db:
                        try:
                            dolphie.secondary_db_connection.execute("USE %s" % query_db)

                            dolphie.secondary_db_connection.execute("EXPLAIN %s" % query)
                            explain_data = dolphie.secondary_db_connection.fetchall()

                            dolphie.secondary_db_connection.execute("EXPLAIN FORMAT=JSON %s" % query)
                            explain_fetched_json_data = dolphie.secondary_db_connection.fetchone()
                            if explain_fetched_json_data:
                                explain_json_data = explain_fetched_json_data.get("EXPLAIN")
                        except ManualException as e:
                            explain_failure = "[b indian_red]EXPLAIN ERROR:[/b indian_red] [indian_red]%s" % e.reason

                user_thread_attributes_table = None
                if dolphie.performance_schema_enabled:
                    user_thread_attributes_table = Table(box=None, show_header=False, expand=True)

                    dolphie.secondary_db_connection.execute(
                        MySQLQueries.user_thread_attributes.replace("$1", thread_id)
                    )

                    user_thread_attributes = dolphie.secondary_db_connection.fetchall()
                    if user_thread_attributes:
                        user_thread_attributes_table.add_column("")
                        user_thread_attributes_table.add_column("", overflow="fold")

                        for attribute in user_thread_attributes:
                            user_thread_attributes_table.add_row(
                                f"[label]{attribute['ATTR_NAME']}", attribute["ATTR_VALUE"]
                            )
                    else:
                        user_thread_attributes_table.add_column(justify="center")
                        user_thread_attributes_table.add_row("[b][label]None found")

                # Transaction history
                transaction_history_table = None
                if (
                    dolphie.is_mysql_version_at_least("5.7")
                    and dolphie.performance_schema_enabled
                    and thread_data.mysql_thread_id
                ):
                    query = MySQLQueries.thread_transaction_history.replace("$1", str(thread_data.mysql_thread_id))
                    dolphie.secondary_db_connection.execute(query)
                    transaction_history = dolphie.secondary_db_connection.fetchall()

                    if transaction_history:
                        transaction_history_table = Table(box=None)
                        transaction_history_table.add_column("Start Time")
                        transaction_history_table.add_column("Query", overflow="fold")

                        for query in transaction_history:
                            trx_history_formatted_query = ""
                            if query["sql_text"]:
                                trx_history_formatted_query = format_query(
                                    sqlformat(query["sql_text"], reindent_aligned=True), minify=False
                                )

                            transaction_history_table.add_row(
                                query["start_time"].strftime("%Y-%m-%d %H:%M:%S"), trx_history_formatted_query
                            )

                self.call_from_thread(show_thread_screen)

            elif key == "u":
                if dolphie.connection_source == ConnectionSource.proxysql:
                    title = "Frontend Users"

                    dolphie.secondary_db_connection.execute(ProxySQLQueries.user_stats)
                    users = dolphie.secondary_db_connection.fetchall()

                    columns = {
                        "User": {"field": "username", "format_number": False},
                        "Active": {"field": "frontend_connections", "format_number": True},
                        "Max": {"field": "frontend_max_connections", "format_number": True},
                        "Default HG": {"field": "default_hostgroup", "format_number": False},
                        "Default Schema": {"field": "default_schema", "format_number": False},
                        "SSL": {"field": "use_ssl", "format_number": False},
                    }

                    table = Table(
                        header_style="b",
                        box=box.SIMPLE_HEAVY,
                        show_edge=False,
                        style="table_border",
                    )
                    for column, data in columns.items():
                        table.add_column(column, no_wrap=True)

                    for user in users:
                        row_values = []

                        for column, data in columns.items():
                            value = user.get(data["field"], "N/A")

                            if data["format_number"]:
                                row_values.append(format_number(value) if value else "0")
                            elif column == "SSL":
                                row_values.append("ON" if value == "1" else "OFF")
                            else:
                                row_values.append(value or "")

                        table.add_row(*row_values)
                else:
                    title = "Users"

                    if dolphie.is_mysql_version_at_least("5.7"):
                        dolphie.secondary_db_connection.execute(MySQLQueries.ps_user_statisitics)
                    else:
                        dolphie.secondary_db_connection.execute(MySQLQueries.ps_user_statisitics_56)

                    users = dolphie.secondary_db_connection.fetchall()

                    columns = {
                        "User": {"field": "user", "format_number": False},
                        "Active": {"field": "current_connections", "format_number": True},
                        "Total": {"field": "total_connections", "format_number": True},
                        "Rows Read": {"field": "rows_examined", "format_number": True},
                        "Rows Sent": {"field": "rows_sent", "format_number": True},
                        "Rows Updated": {"field": "rows_affected", "format_number": True},
                        "Tmp Tables": {"field": "created_tmp_tables", "format_number": True},
                        "Tmp Disk Tables": {"field": "created_tmp_disk_tables", "format_number": True},
                        "Plugin": {"field": "plugin", "format_number": False},
                    }

                    table = Table(
                        header_style="b",
                        box=box.SIMPLE_HEAVY,
                        show_edge=False,
                        style="table_border",
                    )
                    for column, data in columns.items():
                        table.add_column(column, no_wrap=True)

                    for user in users:
                        row_values = []

                        for column, data in columns.items():
                            value = user.get(data.get("field"), "N/A")

                            if data["format_number"]:
                                row_values.append(format_number(value) if value else "0")
                            else:
                                row_values.append(value or "")

                        table.add_row(*row_values)

                screen_data = Group(
                    Align.center(f"[b light_blue]{title} Connected ([highlight]{len(users)}[/highlight])\n"),
                    Align.center(table),
                )

                self.call_from_thread(show_command_screen)
        except ManualException as e:
            self.notify(e.reason, title=f"Error running command '{key}'", severity="error", timeout=10)

        tab.spinner.hide()

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
                        f":tada:  [b]New version [highlight]{latest_version}[/highlight] is available![/b] :tada:\n\n"
                        f"Please update at your earliest convenience\n"
                        f"[dark_gray]Find more details at https://github.com/charles-001/dolphie",
                        title="",
                        severity="information",
                        timeout=20,
                    )

                    logger.warning(
                        f"New version v{latest_version} is available! Please update at your earliest convenience. "
                        "Find more details at https://github.com/charles-001/dolphie"
                    )
        except Exception:
            pass

    def compose(self):
        yield TopBar(host="", app_version=__version__, help="press [b highlight]?[/b highlight] for commands")
        yield Tabs(id="host_tabs")


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
