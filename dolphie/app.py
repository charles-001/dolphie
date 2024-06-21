#!/usr/bin/env python3

# ****************************
# *        Dolphie           *
# * Author: Charles Thompson *
# ****************************


import os
import re
from datetime import datetime, timedelta
from functools import partial
from importlib import metadata

import dolphie.Modules.MetricManager as MetricManager
import requests
from dolphie import Dolphie
from dolphie.DataTypes import (
    ConnectionSource,
    ConnectionStatus,
    HotkeyCommands,
    ProcesslistThread,
    ProxySQLProcesslistThread,
)
from dolphie.Modules.ArgumentParser import ArgumentParser, Config
from dolphie.Modules.Functions import (
    format_bytes,
    format_number,
    format_query,
    format_sys_table_memory,
)
from dolphie.Modules.ManualException import ManualException
from dolphie.Modules.Queries import MySQLQueries, ProxySQLQueries
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
from dolphie.Widgets.help import HelpScreen
from dolphie.Widgets.modal import CommandModal
from dolphie.Widgets.proxysql_thread_screen import ProxySQLThreadScreen
from dolphie.Widgets.thread_screen import ThreadScreen
from dolphie.Widgets.topbar import TopBar
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
from textual.widgets import Switch, TabbedContent, TabPane
from textual.worker import Worker, WorkerState, get_current_worker

try:
    __package_name__ = metadata.metadata(__package__ or __name__)["Name"]
    __version__ = metadata.version(__package__ or __name__)
except Exception:
    __package_name__ = "Dolphie"
    __version__ = "N/A"


class DolphieApp(App):
    TITLE = "Dolphie"
    CSS_PATH = "Dolphie.css"

    def __init__(self, config: Config):
        super().__init__()

        self.config = config

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
                "highlight": "#91abec",
                "label": "#c5c7d2",
                "b label": "b #c5c7d2",
                "light_blue": "#bbc8e8",
                "b white": "b #e9e9e9",
                "b highlight": "b #91abec",
                "bold red": "b #fd8383",
                "b light_blue": "b #bbc8e8",
                "panel_border": "#6171a6",
                "table_border": "#333f62",
            }
        )
        self.console.push_theme(theme)
        self.console.set_window_title(self.TITLE)

    @work(thread=True, group="main")
    async def run_worker_main(self, tab_id: int):
        tab = self.tab_manager.get_tab(tab_id)

        # Get our worker thread
        tab.worker = get_current_worker()
        tab.worker.name = tab_id
        tab.worker_running = True

        dolphie = tab.dolphie
        try:
            if not dolphie.main_db_connection.is_connected():
                self.tab_manager.rename_tab(tab)  # this will use dolphie.host instead of host_with_port
                self.tab_manager.update_topbar(tab=tab, connection_status=ConnectionStatus.connecting)
                tab.loading_indicator.display = True

                dolphie.db_connect()
                self.tab_manager.rename_tab(tab)

            dolphie.worker_start_time = datetime.now()
            dolphie.polling_latency = (dolphie.worker_start_time - dolphie.worker_previous_start_time).total_seconds()
            dolphie.refresh_latency = round(dolphie.polling_latency - dolphie.refresh_interval, 2)
            dolphie.worker_previous_start_time = dolphie.worker_start_time

            if dolphie.connection_source == ConnectionSource.mysql:
                self.process_mysql_data(tab)
            elif dolphie.connection_source == ConnectionSource.proxysql:
                self.process_proxysql_data(tab)
                dolphie.proxysql_process_execution_time = (datetime.now() - dolphie.worker_start_time).total_seconds()
        except ManualException as exception:
            # This will set up the worker state change function below to trigger the
            # host setup modal with the error
            tab.worker_cancel_error = exception

            await self.tab_manager.disconnect_tab(tab)

        tab.worker_running = False

    def on_worker_state_changed(self, event: Worker.StateChanged):
        tab = self.tab_manager.get_tab(event.worker.name)
        if not tab:
            return

        dolphie = tab.dolphie

        if event.worker.group == "main":
            tab.worker_running = False

            if event.state == WorkerState.SUCCESS:
                self.monitor_read_only_change(tab)

                # Skip this if the conditions are right
                if len(self.screen_stack) > 1 or dolphie.pause_refresh or not dolphie.main_db_connection.is_connected():
                    tab.worker_timer = self.set_timer(dolphie.refresh_interval, partial(self.run_worker_main, tab.id))

                    return

                # If the main container isn't displayed, it means we just connected
                if not tab.main_container.display:
                    tab.main_container.display = True
                    tab.sparkline.display = True

                    # Hide all graph tabs so we can show the ones we want
                    tabs = tab.metric_graph_tabs.query(TabPane)
                    for graph_tab in tabs:
                        tab.metric_graph_tabs.hide_tab(graph_tab.id)

                    # Show the tabs that are for the current connection source
                    for metric_instance in dolphie.metric_manager.metrics.__dict__.values():
                        if dolphie.connection_source in metric_instance.connection_source:
                            tab.metric_graph_tabs.show_tab(f"graph_tab_{metric_instance.tab_name}_{tab.id}")

                refresh_interval = dolphie.refresh_interval
                if dolphie.connection_source == ConnectionSource.mysql:
                    self.refresh_screen_mysql(tab)
                elif dolphie.connection_source == ConnectionSource.proxysql:
                    self.refresh_screen_proxysql(tab)

                    # If we have a lot of client connections, increase the refresh interval based on the
                    # proxysql process execution time. René asked for this to be added to reduce load on ProxySQL
                    client_connections = dolphie.global_status.get("Client_Connections_connected", 0)
                    if client_connections > 30000:
                        percentage = 0.60
                    elif client_connections > 20000:
                        percentage = 0.50
                    elif client_connections > 10000:
                        percentage = 0.40
                    else:
                        percentage = 0

                    if percentage:
                        refresh_interval = dolphie.refresh_interval + (
                            dolphie.proxysql_process_execution_time * percentage
                        )

                tab.worker_timer = self.set_timer(refresh_interval, partial(self.run_worker_main, tab.id))
            elif event.state == WorkerState.CANCELLED:
                # Only show the modal if there's a worker cancel error
                if tab.worker_cancel_error:
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
                if len(self.screen_stack) > 1 or dolphie.pause_refresh:
                    tab.replicas_worker_timer = self.set_timer(
                        dolphie.refresh_interval, partial(self.run_worker_replicas, tab.id)
                    )

                    return

                if dolphie.panels.replication.visible and dolphie.replica_manager.available_replicas:
                    replication_panel.create_replica_panel(tab)

                tab.replicas_worker_timer = self.set_timer(
                    dolphie.refresh_interval, partial(self.run_worker_replicas, tab.id)
                )

    @work(thread=True, group="replicas")
    def run_worker_replicas(self, tab_id: int):
        tab = self.tab_manager.get_tab(tab_id)

        # Get our worker thread
        tab.replicas_worker = get_current_worker()
        tab.replicas_worker.name = tab_id

        dolphie = tab.dolphie

        if dolphie.panels.replication.visible:
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
            dolphie.replica_manager.remove_all()

    def process_mysql_data(self, tab: Tab):
        dolphie = tab.dolphie

        dolphie.global_variables = dolphie.main_db_connection.fetch_status_and_variables("variables")
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

        dolphie.previous_replica_sbm = dolphie.replica_lag
        dolphie.replica_lag_source, dolphie.replica_lag, dolphie.replication_status = (
            replication_panel.fetch_replication_data(tab)
        )

        # If using MySQL 8, fetch the replication applier status data
        if (
            dolphie.is_mysql_version_at_least("8.0")
            and dolphie.panels.replication.visible
            and dolphie.global_variables.get("replica_parallel_workers", 0) > 1
        ):
            dolphie.main_db_connection.execute(MySQLQueries.replication_applier_status)
            dolphie.replication_applier_status = dolphie.main_db_connection.fetchall()

        dolphie.massage_metrics_data()

        if dolphie.group_replication or dolphie.innodb_cluster:
            if dolphie.is_mysql_version_at_least("8.0.13"):
                dolphie.main_db_connection.execute(MySQLQueries.group_replication_get_write_concurrency)
                dolphie.group_replication_data = dolphie.main_db_connection.fetchone()

            dolphie.main_db_connection.execute(MySQLQueries.get_group_replication_members)
            dolphie.group_replication_members = dolphie.main_db_connection.fetchall()
            for member_role_data in dolphie.group_replication_members:
                if (
                    member_role_data.get("MEMBER_ID") == dolphie.server_uuid
                    and member_role_data.get("MEMBER_ROLE") == "PRIMARY"
                ):
                    dolphie.is_group_replication_primary = True
                    break

        if dolphie.panels.dashboard.visible:
            if dolphie.is_mysql_version_at_least("8.2.0") and dolphie.connection_source_alt != ConnectionSource.mariadb:
                dolphie.main_db_connection.execute(MySQLQueries.show_binary_log_status)
            else:
                dolphie.main_db_connection.execute(MySQLQueries.show_master_status)
            dolphie.binlog_status = dolphie.main_db_connection.fetchone()

            # This can cause MySQL to crash: https://perconadev.atlassian.net/browse/PS-9066
            # if dolphie.global_variables.get("binlog_transaction_compression") == "ON":
            #     dolphie.main_db_connection.execute(MySQLQueries.get_binlog_transaction_compression_percentage)
            #     dolphie.binlog_transaction_compression_percentage = dolphie.main_db_connection.fetchone().get(
            #         "compression_percentage"
            #     )

        if dolphie.panels.processlist.visible:
            dolphie.processlist_threads = processlist_panel.fetch_data(tab)

        if dolphie.is_mysql_version_at_least("5.7"):
            if dolphie.panels.metadata_locks.visible:
                dolphie.metadata_locks = metadata_locks_panel.fetch_data(tab)
            else:
                # Reset this data so the graph doesn't show old data
                dolphie.metadata_locks = {}

            if dolphie.panels.ddl.visible:
                dolphie.main_db_connection.execute(MySQLQueries.ddls)
                dolphie.ddl = dolphie.main_db_connection.fetchall()

        dolphie.metric_manager.refresh_data(
            worker_start_time=dolphie.worker_start_time,
            polling_latency=dolphie.polling_latency,
            global_variables=dolphie.global_variables,
            global_status=dolphie.global_status,
            innodb_metrics=dolphie.innodb_metrics,
            disk_io_metrics=dolphie.disk_io_metrics,
            metadata_lock_metrics=dolphie.metadata_locks,
            replication_status=dolphie.replication_status,
            replication_lag=dolphie.replica_lag,
        )

    def process_proxysql_data(self, tab: Tab):
        dolphie = tab.dolphie

        dolphie.global_variables = dolphie.main_db_connection.fetch_status_and_variables("variables")
        dolphie.global_status = dolphie.main_db_connection.fetch_status_and_variables("mysql_stats")

        dolphie.main_db_connection.execute(ProxySQLQueries.command_stats)
        dolphie.proxysql_command_stats = dolphie.main_db_connection.fetchall()

        # Here, we're going to format the command stats to match the global status keys of
        # MySQL and get total count of queries
        total_queries_count = 0
        for row in dolphie.proxysql_command_stats:
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

        if dolphie.panels.proxysql_hostgroup_summary.visible:
            dolphie.main_db_connection.execute(ProxySQLQueries.hostgroup_summary)
            dolphie.proxysql_hostgroup_summary = dolphie.main_db_connection.fetchall()

        if dolphie.panels.processlist.visible:
            dolphie.processlist_threads = proxysql_processlist_panel.fetch_data(tab)

        if dolphie.panels.proxysql_mysql_query_rules.visible:
            dolphie.main_db_connection.execute(ProxySQLQueries.query_rules_summary)
            dolphie.proxysql_mysql_query_rules = dolphie.main_db_connection.fetchall()

        dolphie.metric_manager.refresh_data(
            worker_start_time=dolphie.worker_start_time,
            polling_latency=dolphie.polling_latency,
            global_variables=dolphie.global_variables,
            global_status=dolphie.global_status,
            proxysql_command_stats=dolphie.proxysql_command_stats,
        )

    def refresh_screen_proxysql(self, tab: Tab):
        dolphie = tab.dolphie

        if tab.loading_indicator.display:
            tab.loading_indicator.display = False
            self.tab_manager.update_topbar(tab=tab, connection_status="ONLINE")

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
        dolphie.processlist_threads_snapshot = dolphie.processlist_threads.copy()

        # This denotes that we've gone through the first loop of the worker thread
        dolphie.completed_first_loop = True

    def refresh_screen_mysql(self, tab: Tab):
        dolphie = tab.dolphie

        if tab.loading_indicator.display:
            tab.loading_indicator.display = False

            self.layout_graphs(tab)
            self.tab_manager.update_topbar(tab=tab, connection_status=dolphie.connection_status)

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
                tab.metric_graph_tabs.show_tab(f"graph_tab_replication_lag_{tab.id}")
            else:
                tab.metric_graph_tabs.hide_tab(f"graph_tab_replication_lag_{tab.id}")

            # Refresh the graph(s) for the selected tab
            self.update_graphs(tab.metric_graph_tabs.get_pane(tab.metric_graph_tabs.active).name)

        # We take a snapshot of the processlist to be used for commands
        # since the data can change after a key is pressed
        dolphie.processlist_threads_snapshot = dolphie.processlist_threads.copy()

        # This denotes that we've gone through the first loop of the worker thread
        dolphie.completed_first_loop = True

    def monitor_read_only_change(self, tab: Tab):
        dolphie = tab.dolphie

        if dolphie.connection_source == ConnectionSource.proxysql:
            return

        current_ro_status = dolphie.global_variables.get("read_only")
        formatted_ro_status = ConnectionStatus.read_only if current_ro_status == "ON" else ConnectionStatus.read_write
        status = "read-only" if current_ro_status == "ON" else "read/write"

        message = f"Host [light_blue]{dolphie.host_with_port}[/light_blue] is now [b highlight]{status}[/b highlight]"

        if current_ro_status == "ON" and not dolphie.replication_status and not dolphie.group_replication:
            message += " ([yellow]SHOULD BE READ/WRITE?[/yellow])"
        elif current_ro_status == "ON" and dolphie.group_replication and dolphie.is_group_replication_primary:
            message += " ([yellow]SHOULD BE READ/WRITE?[/yellow])"

        if (
            dolphie.connection_status in [ConnectionStatus.read_write, ConnectionStatus.read_only]
            and dolphie.connection_status != formatted_ro_status
        ):
            self.app.notify(title="Read-only mode change", message=message, severity="warning", timeout=15)

            self.tab_manager.update_topbar(tab=tab, connection_status=formatted_ro_status)

        dolphie.connection_status = formatted_ro_status

    @work()
    async def connect_as_hostgroup(self, hostgroup: str):
        self.loading_hostgroups = True
        self.notify(f"Connecting to hosts in hostgroup [highlight]{hostgroup}", severity="information")

        for host in self.config.hostgroup_hosts.get(hostgroup, []):
            # We only want to switch if it's the first tab created
            switch_tab = True if not self.tab_manager.active_tab else False

            tab = await self.tab_manager.create_tab(tab_name=host, use_hostgroup=True, switch_tab=switch_tab)

            self.run_worker_main(tab.id)
            self.run_worker_replicas(tab.id)

        self.loading_hostgroups = False
        self.notify(f"Finished connecting to hosts in hostgroup [highlight]{hostgroup}", severity="success")

    async def on_mount(self):
        self.tab_manager = TabManager(app=self.app, config=self.config)

        if self.config.hostgroup:
            self.connect_as_hostgroup(self.config.hostgroup)
        else:
            tab = await self.tab_manager.create_tab(tab_name="Initial Tab")

            if self.config.host_setup:
                self.tab_manager.setup_host_tab(tab)
            else:
                self.run_worker_main(self.tab_manager.active_tab.id)
                self.run_worker_replicas(self.tab_manager.active_tab.id)

        self.check_for_new_version()

    def _handle_exception(self, error: Exception) -> None:
        self.bell()
        self.exit(message=Traceback(show_locals=True, width=None, locals_max_length=5))

    @on(TabbedContent.TabActivated, "#host_tabs")
    def tab_changed(self, event: TabbedContent.TabActivated):
        self.tab_manager.switch_tab(event.pane.name)

    @on(TabbedContent.TabActivated, ".metrics_host_tabs")
    def metric_tab_changed(self, event: TabbedContent.TabActivated):
        metric_instance_name = event.pane.name

        if metric_instance_name:
            self.update_graphs(metric_instance_name)

    def update_graphs(self, tab_metric_instance_name):
        if not self.tab_manager.active_tab or not self.tab_manager.active_tab.panel_graphs.display:
            return

        for metric_instance in self.tab_manager.active_tab.dolphie.metric_manager.metrics.__dict__.values():
            if tab_metric_instance_name == metric_instance.tab_name:
                for graph_name in metric_instance.graphs:
                    getattr(self.tab_manager.active_tab, graph_name).render_graph(metric_instance)

        self.update_stats_label(tab_metric_instance_name)

    def update_stats_label(self, tab_metric_instance_name):
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

    def toggle_panel(self, panel_name):
        # We store the panel objects in the tab object (i.e. tab.panel_dashboard, tab.panel_processlist, etc.)
        panel = self.tab_manager.active_tab.get_panel_widget(panel_name)

        new_display_status = not panel.display

        setattr(getattr(self.tab_manager.active_tab.dolphie.panels, panel_name), "visible", new_display_status)

        if panel_name not in [self.tab_manager.active_tab.dolphie.panels.graphs.name]:
            self.refresh_panel(self.tab_manager.active_tab, panel_name, toggled=True)

        panel.display = new_display_status

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

        if tab.dolphie.connection_source == ConnectionSource.mysql:
            if toggled or not tab.dolphie.completed_first_loop:
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

        metric_instance_name = event.switch.name
        metric = event.switch.id

        metric_instance = getattr(self.tab_manager.active_tab.dolphie.metric_manager.metrics, metric_instance_name)
        metric_data: MetricManager.MetricData = getattr(metric_instance, metric)
        metric_data.visible = event.value

        self.update_graphs(metric_instance_name)

    async def on_key(self, event: events.Key):
        if len(self.screen_stack) > 1:
            return

        await self.capture_key(event.key)

    async def capture_key(self, key):
        tab = self.tab_manager.active_tab
        if not tab:
            return

        exclude_keys = [
            "up",
            "down",
            "left",
            "right",
            "pageup",
            "pagedown",
            "home",
            "end",
            "tab",
            "enter",
            "grave_accent",
            "q",
            "question_mark",
            "plus",
            "minus",
            "equals_sign",
            "ctrl+a",
            "ctrl+d",
        ]

        screen_data = None
        dolphie = tab.dolphie

        # Prevent commands from being run if the secondary connection is processing a query already
        if key not in exclude_keys:
            if dolphie.secondary_db_connection and dolphie.secondary_db_connection.running_query:
                self.notify("There's already a command running - please wait for it to finish")
                return

            if not dolphie.main_db_connection.is_connected():
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
            if dolphie.connection_source == ConnectionSource.proxysql:
                self.toggle_panel(dolphie.panels.proxysql_hostgroup_summary.name)
                dolphie.proxysql_per_second_data = {}
                self.tab_manager.active_tab.proxysql_hostgroup_summary_datatable.clear()

                return

            self.toggle_panel(dolphie.panels.replication.name)

            tab.replicas_container.display = False
            if not dolphie.panels.replication.visible:
                for member in dolphie.app.query(f".replica_container_{dolphie.tab_id}"):
                    member.remove()
            else:
                if dolphie.replica_manager.available_replicas:
                    tab.replicas_container.display = True
                    tab.replicas_title.update(
                        f"[b]Loading [highlight]{len(dolphie.replica_manager.available_replicas)}[/highlight]"
                        " replicas...\n"
                    )
                    tab.replicas_loading_indicator.display = True
        elif key == "4":
            self.toggle_panel(dolphie.panels.graphs.name)
            self.app.update_graphs("dml")
        elif key == "5":
            if dolphie.connection_source == ConnectionSource.proxysql:
                self.toggle_panel(dolphie.panels.proxysql_mysql_query_rules.name)
                return

            if not dolphie.is_mysql_version_at_least("5.7") or not dolphie.performance_schema_enabled:
                self.notify("Metadata Locks panel requires MySQL 5.7+ with Performance Schema enabled")
                return

            query = (
                "SELECT enabled FROM performance_schema.setup_instruments WHERE name = 'wait/lock/metadata/sql/mdl';"
            )

            dolphie.secondary_db_connection.execute(query)
            row = dolphie.secondary_db_connection.fetchone()
            if row and row.get("enabled") == "NO":
                self.notify(
                    "Metadata Locks panel requires Performance Schema to have"
                    " [highlight]wait/lock/metadata/sql/mdl[/highlight] enabled in setup_instruments table"
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
            self.tab_manager.topbar.host = ""
            self.tab_manager.setup_host_tab(new_tab)
        elif key == "equals_sign":

            def command_get_input(tab_name):
                self.tab_manager.rename_tab(tab, tab_name)

            self.app.push_screen(
                CommandModal(command=HotkeyCommands.rename_tab, message="What would you like to rename the tab to?"),
                command_get_input,
            )
        elif key == "minus":
            if len(self.tab_manager.tabs) == 1:
                self.notify("Removing all tabs is not permitted", severity="error")
            else:
                await self.tab_manager.remove_tab(tab)
                await self.tab_manager.disconnect_tab(tab=tab, update_topbar=False)

                self.notify(f"Tab [highlight]{tab.name}[/highlight] [white]has been removed", severity="success")
                self.tab_manager.tabs.pop(tab.id, None)

        elif key == "ctrl+a" or key == "ctrl+d":
            all_tabs = [tab.id for tab in self.tab_manager.get_all_tabs()]

            if key == "ctrl+a":
                switch_to_tab = all_tabs[(all_tabs.index(tab.id) - 1) % len(all_tabs)]
            elif key == "ctrl+d":
                switch_to_tab = all_tabs[(all_tabs.index(tab.id) + 1) % len(all_tabs)]

            self.tab_manager.switch_tab(switch_to_tab)

        elif key == "a":
            if dolphie.show_additional_query_columns:
                dolphie.show_additional_query_columns = False
                self.notify("Processlist will now hide additional columns")
            else:
                dolphie.show_additional_query_columns = True
                self.notify("Processlist will now show additional columns")

        elif key == "c":
            dolphie.user_filter = ""
            dolphie.db_filter = ""
            dolphie.host_filter = ""
            dolphie.hostgroup_filter = ""
            dolphie.query_time_filter = ""
            dolphie.query_filter = ""

            self.notify("Cleared all filters", severity="success")

        elif key == "d":
            if dolphie.connection_source == ConnectionSource.proxysql:
                self.notify(f"Command [highlight]{key}[/highlight] is only available for MySQL connections")
                return

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

        elif key == "f":

            def command_get_input(filter_data):
                filter_name, filter_value = filter_data[0], filter_data[1]
                filters_mapping = {
                    "User": "user_filter",
                    "Database": "db_filter",
                    "Host": "host_filter",
                    "Hostgroup": "hostgroup_filter",
                    "Query time": "query_time_filter",
                    "Query text": "query_filter",
                }

                attribute = filters_mapping.get(filter_name)
                if attribute:
                    setattr(dolphie, attribute, int(filter_value) if attribute == "query_time_filter" else filter_value)
                    self.notify(
                        f"Filtering [b]{filter_name.capitalize()}[/b] by [b highlight]{filter_value}[/b highlight]",
                        severity="success",
                    )
                else:
                    self.notify(f"Invalid filter name {filter_name}", severity="error")

            self.app.push_screen(
                CommandModal(
                    command=HotkeyCommands.thread_filter,
                    message="Select which field you'd like to filter by",
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
                    message="Kill Process",
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
                    message="Kill processes based around parameters",
                    processlist_data=dolphie.processlist_threads_snapshot,
                ),
                command_get_input,
            )

        elif key == "l":
            if dolphie.connection_source == ConnectionSource.proxysql:
                self.notify(f"Command [highlight]{key}[/highlight] is only available for MySQL connections")
                return

            self.run_command_in_worker(key=key, dolphie=dolphie)

        elif key == "o":
            if dolphie.connection_source == ConnectionSource.proxysql:
                self.notify(f"Command [highlight]{key}[/highlight] is only available for MySQL connections")
                return

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
            if not dolphie.pause_refresh:
                dolphie.pause_refresh = True
                self.notify(f"Refresh is paused! Press [b highlight]{key}[/b highlight] again to resume")
            else:
                dolphie.pause_refresh = False
                self.notify("Refreshing has resumed", severity="success")

        if key == "P":
            if dolphie.connection_source == ConnectionSource.proxysql:
                self.notify(f"Command [highlight]{key}[/highlight] is only available for MySQL connections")
                return

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
                    thread_table.add_row("[label]Hostgroup", thread_data.hostgroup)
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
                    message="Process Details",
                    processlist_data=dolphie.processlist_threads_snapshot,
                ),
                command_get_input,
            )

        elif key == "T":
            if dolphie.connection_source == ConnectionSource.proxysql:
                self.notify(f"Command [highlight]{key}[/highlight] is only available for MySQL connections")
                return

            if dolphie.show_trxs_only:
                dolphie.show_trxs_only = False
                dolphie.show_idle_threads = False
                self.notify("Processlist will now no longer only show threads that have an active transaction")
            else:
                dolphie.show_trxs_only = True
                dolphie.show_idle_threads = True
                self.notify("Processlist will now only show threads that have an active transaction")

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
                screen_data = Group(
                    Align.center("[b light_blue]Host Cache[/b light_blue]\n"), "There are currently no hosts resolved"
                )

        elif key == "question_mark":
            self.app.push_screen(HelpScreen(dolphie.connection_source))

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

                    self.notify("Killed Process ID [b highlight]%s[/b highlight]" % thread_id, severity="success")
                except ManualException as e:
                    self.notify(e.reason, title="Error killing Process ID", severity="error")

            elif key == "K":

                def execute_kill(thread_id):
                    if dolphie.connection_source_alt == ConnectionSource.aws_rds:
                        query = "CALL mysql.rds_kill(%s)"
                    elif dolphie.connection_source_alt == ConnectionSource.azure_mysql:
                        query = "CALL mysql.az_kill(%s)"
                    elif dolphie.connection_source == ConnectionSource.proxysql:
                        query = "KILL CONNECTION %s"
                    else:
                        query = "KILL %s"

                    dolphie.secondary_db_connection.execute(query % thread_id)

                kill_type, kill_value, include_sleeping_queries, lower_limit, upper_limit = additional_data
                db_field = {"username": "user", "host": "host", "time_range": "time"}.get(kill_type)

                commands_to_kill = ["Query", "Execute"]

                if include_sleeping_queries:
                    commands_to_kill.append("Sleep")

                tab.spinner.show()
                threads_killed = 0

                # We need to make a copy of the threads snapshot to so it doesn't change while we're iterating over it
                threads = dolphie.processlist_threads_snapshot.copy()
                for thread_id, thread in threads.items():
                    thread: ProcesslistThread
                    try:
                        if thread.command in commands_to_kill:
                            if kill_type == "time_range":
                                if lower_limit <= thread.time <= upper_limit:
                                    execute_kill(thread_id)
                                    threads_killed += 1
                            elif getattr(thread, db_field) == kill_value:
                                execute_kill(thread_id)
                                threads_killed += 1
                    except ManualException as e:
                        self.notify(e.reason, title=f"Error Killing Process ID {thread_id}", severity="error")

                if threads_killed:
                    self.notify(f"Killed [highlight]{threads_killed}[/highlight] processes")
                else:
                    self.notify("No processes were killed")

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
                thread_table.add_row("[label]Rows Locked", thread_data.trx_rows_locked)
                thread_table.add_row("[label]Rows Modified", thread_data.trx_rows_modified)

                if dolphie.global_variables.get("innodb_thread_concurrency"):
                    thread_table.add_row("[label]Tickets", thread_data.trx_concurrency_tickets)

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
                        f":tada:  [b]New version [highlight]v{latest_version}[/highlight] is available![/b] :tada:\n\n"
                        f"Please update at your earliest convenience\n"
                        f"[dark_gray]Find more details at https://github.com/charles-001/dolphie",
                        title="",
                        severity="information",
                        timeout=20,
                    )
        except Exception:
            pass

    def compose(self):
        yield TopBar(host="", app_version=__version__, help="press [b highlight]?[/b highlight] for help")
        yield TabbedContent(id="host_tabs")


def main():
    # Set environment variables so Textual can use all the pretty colors
    os.environ["TERM"] = "xterm-256color"
    os.environ["COLORTERM"] = "truecolor"

    parser = ArgumentParser(__version__)

    app = DolphieApp(parser.config)
    app.run()


if __name__ == "__main__":
    main()
