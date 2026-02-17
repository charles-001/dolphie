import copy
import os
import uuid

import dolphie.Modules.MetricManager as MetricManager
from dolphie.DataTypes import ConnectionSource, ConnectionStatus, Panels
from dolphie.Dolphie import Dolphie
from dolphie.Modules.ArgumentParser import Config, HostGroupMember
from dolphie.Modules.ManualException import ManualException
from dolphie.Modules.ReplayManager import ReplayManager
from dolphie.Widgets.SpinnerWidget import SpinnerWidget
from dolphie.Widgets.TabSetupModal import TabSetupModal
from dolphie.Widgets.TopBar import TopBar
from rich.text import Text
from textual.app import App
from textual.containers import (
    Center,
    Container,
    Horizontal,
    ScrollableContainer,
    VerticalScroll,
)
from textual.content import Content
from textual.timer import Timer
from textual.widgets import (
    Button,
    DataTable,
    Label,
    LoadingIndicator,
    ProgressBar,
    RadioButton,
    RadioSet,
    Sparkline,
    Static,
    Switch,
)
from textual.widgets import Tab as TabWidget
from textual.widgets import TabbedContent, TabPane, Tabs
from textual.worker import Worker


class Tab:
    def __init__(
        self,
        id: str,
        name: str,
        dolphie: Dolphie = None,
        manual_tab_name: str = None,
        replay_manager: ReplayManager = None,
    ):
        self.id = id
        self.name = name
        self.dolphie = dolphie
        self.manual_tab_name = manual_tab_name
        self.replay_manager = replay_manager

        self.worker: Worker = None
        self.worker_timer: Timer = None
        self.worker_cancel_error: ManualException = None

        self.replay_manual_control: bool = False

        self.replicas_worker: Worker = None
        self.replicas_worker_timer: Timer = None

        # Track mounted grid widgets to avoid DOM queries each refresh cycle
        self.clusterset_widgets: dict[str, Static] = {}
        self.galera_widgets: dict[str, Static] = {}
        self.member_widgets: dict[str, Static] = {}
        self.replica_widgets: dict[str, Static] = {}

    def save_references_to_components(self):
        app = self.dolphie.app

        self.main_container = app.query_one("#main_container", VerticalScroll)
        self.metric_graph_tabs = app.query_one("#metric_graph_tabs", TabbedContent)
        self.loading_indicator = app.query_one("#loading_indicator", LoadingIndicator)
        self.sparkline = app.query_one("#panel_dashboard_queries_qps", Sparkline)
        self.panel_dashboard = app.query_one("#panel_dashboard", Container)
        self.panel_graphs = app.query_one("#panel_graphs", Container)
        self.panel_replication = app.query_one("#panel_replication", Container)
        self.panel_metadata_locks = app.query_one("#panel_metadata_locks", Container)
        self.panel_processlist = app.query_one("#panel_processlist", Container)
        self.panel_ddl = app.query_one("#panel_ddl", Container)
        self.panel_pfs_metrics = app.query_one("#panel_pfs_metrics", Container)
        self.panel_statements_summary = app.query_one("#panel_statements_summary", Container)
        self.panel_proxysql_hostgroup_summary = app.query_one("#panel_proxysql_hostgroup_summary", Container)
        self.panel_proxysql_mysql_query_rules = app.query_one("#panel_proxysql_mysql_query_rules", Container)
        self.panel_proxysql_command_stats = app.query_one("#panel_proxysql_command_stats", Container)

        self.spinner = app.query_one("#spinner", SpinnerWidget)
        self.spinner.hide()

        self.ddl_title = app.query_one("#ddl_title", Label)
        self.ddl_datatable = app.query_one("#ddl_datatable", DataTable)

        self.pfs_metrics_file_io_datatable = app.query_one("#pfs_metrics_file_io_datatable", DataTable)
        self.pfs_metrics_table_io_waits_datatable = app.query_one("#pfs_metrics_table_io_waits_datatable", DataTable)
        self.pfs_metrics_radio_set = app.query_one("#pfs_metrics_radio_set", RadioSet)
        self.pfs_metrics_delta = app.query_one("#pfs_metrics_delta", RadioButton)
        self.pfs_metrics_tabs = app.query_one("#pfs_metrics_tabs", TabbedContent)

        self.processlist_title = app.query_one("#processlist_title", Label)
        self.processlist_datatable = app.query_one("#processlist_data", DataTable)
        self.statements_summary_title = app.query_one("#statements_summary_title", Label)
        self.statements_summary_datatable = app.query_one("#statements_summary_datatable", DataTable)
        self.statements_summary_radio_set = app.query_one("#statements_summary_radio_set", RadioSet)
        self.metadata_locks_title = app.query_one("#metadata_locks_title", Label)
        self.metadata_locks_datatable = app.query_one("#metadata_locks_datatable", DataTable)
        self.proxysql_hostgroup_summary_title = app.query_one("#proxysql_hostgroup_summary_title", Static)
        self.proxysql_hostgroup_summary_datatable = app.query_one("#proxysql_hostgroup_summary_datatable", DataTable)
        self.proxysql_mysql_query_rules_title = app.query_one("#proxysql_mysql_query_rules_title", Static)
        self.proxysql_mysql_query_rules_datatable = app.query_one("#proxysql_mysql_query_rules_datatable", DataTable)
        self.proxysql_command_stats_title = app.query_one("#proxysql_command_stats_title", Static)
        self.proxysql_command_stats_datatable = app.query_one("#proxysql_command_stats_datatable", DataTable)

        self.dashboard_replay_container = app.query_one("#dashboard_replay_container", Container)
        self.dashboard_replay_progressbar = app.query_one("#dashboard_replay_progressbar", ProgressBar)
        self.dashboard_replay_start_end = app.query_one("#dashboard_replay_start_end", Static)
        self.dashboard_replay = app.query_one("#dashboard_replay", Static)
        self.dashboard_section_1 = app.query_one("#dashboard_section_1", Static)
        self.dashboard_section_2 = app.query_one("#dashboard_section_2", Static)
        self.dashboard_section_3 = app.query_one("#dashboard_section_3", Static)
        self.dashboard_section_4 = app.query_one("#dashboard_section_4", Static)
        self.dashboard_section_5 = app.query_one("#dashboard_section_5", Static)
        self.dashboard_section_6 = app.query_one("#dashboard_section_6", Static)

        self.clusterset_container = app.query_one("#clusterset_container", Container)
        self.clusterset_title = app.query_one("#clusterset_title", Label)
        self.clusterset_grid = app.query_one("#clusterset_grid", Container)

        self.galera_container = app.query_one("#galera_container", Container)
        self.galera_title = app.query_one("#galera_title", Label)
        self.galera_data = app.query_one("#galera_data", Static)
        self.galera_grid = app.query_one("#galera_grid", Container)

        self.group_replication_container = app.query_one("#group_replication_container", Container)
        self.group_replication_grid = app.query_one("#group_replication_grid", Container)
        self.group_replication_data = app.query_one("#group_replication_data", Static)
        self.group_replication_title = app.query_one("#group_replication_title", Label)

        self.replicas_grid = app.query_one("#replicas_grid", Container)
        self.replicas_container = app.query_one("#replicas_container", Container)
        self.replicas_title = app.query_one("#replicas_title", Label)
        self.replicas_loading_indicator = app.query_one("#replicas_loading_indicator", LoadingIndicator)

        self.replication_container = app.query_one("#replication_container", Container)
        self.replication_variables = app.query_one("#replication_variables", Label)
        self.replication_status = app.query_one("#replication_status", Static)
        self.replication_thread_applier_container = app.query_one(
            "#replication_thread_applier_container", ScrollableContainer
        )
        self.replication_thread_applier = app.query_one("#replication_thread_applier", Static)

    def get_panel_widget(self, panel_name: str) -> Container:
        return getattr(self, f"panel_{panel_name}")

    def refresh_replay_dashboard_section(self):
        if not self.dolphie.replay_file:
            return

        min_timestamp = self.replay_manager.min_replay_timestamp
        max_timestamp = self.replay_manager.max_replay_timestamp
        current_timestamp = self.replay_manager.current_replay_timestamp

        # Highlight if the max timestamp matches the current timestamp
        max_timestamp = (
            f"[b][$green]{max_timestamp}[/b][$green]" if max_timestamp == current_timestamp else max_timestamp
        )

        # Update the dashboard title with the timestamp of the replay event
        self.dashboard_replay.update(
            f"[b]Replay[/b] ([$dark_gray]{os.path.basename(self.dolphie.replay_file)}[/$dark_gray])"
        )
        self.dashboard_replay_start_end.update(
            f"{min_timestamp} [$b_highlight]<-[/$b_highlight] "
            f"[$b_light_blue]{current_timestamp}[/$b_light_blue] [$b_highlight]->[/$b_highlight] "
            f"{max_timestamp}"
        )

        # Update the progress bar with the current replay progress
        if self.replay_manager.current_replay_id == self.replay_manager.min_replay_id:
            current_position = 0
        else:
            current_position = self.replay_manager.current_replay_id - self.replay_manager.min_replay_id + 1

        self.dashboard_replay_progressbar.update(progress=current_position, total=self.replay_manager.total_replay_rows)

    def toggle_entities_displays(self):
        def toggle_tab(tab_name, visible):
            if visible:
                self.metric_graph_tabs.show_tab(tab_name)
            else:
                self.metric_graph_tabs.hide_tab(tab_name)

        self.dashboard_section_6.display = bool(self.dolphie.system_utilization)
        toggle_tab("graph_tab_system", self.dolphie.system_utilization)

        if self.dolphie.connection_source == ConnectionSource.mysql:
            self.dashboard_section_5.display = bool(
                self.dolphie.replication_status and not self.dolphie.panels.replication.visible
            )

            toggle_tab("graph_tab_replication_lag", self.dolphie.replication_status)
            toggle_tab(
                "graph_tab_adaptive_hash_index",
                self.dolphie.global_variables.get("innodb_adaptive_hash_index") != "OFF",
            )
            toggle_tab(
                "graph_tab_locks",
                (self.dolphie.metadata_locks_enabled and self.dolphie.panels.metadata_locks.visible)
                or self.dolphie.replay_file,
            )

        elif self.dolphie.connection_source == ConnectionSource.proxysql:
            self.dashboard_section_5.display = False

    def toggle_metric_graph_tabs_display(self):
        self.main_container.display = True

        # Hide/show the tabs that are available for the current connection source
        for metric_instance in self.dolphie.metric_manager.metrics.__dict__.values():
            if self.dolphie.connection_source in metric_instance.connection_source:
                self.metric_graph_tabs.show_tab(f"graph_tab_{metric_instance.tab_name}")
            else:
                self.metric_graph_tabs.hide_tab(f"graph_tab_{metric_instance.tab_name}")

        # Only show the replay section if we're in replay mode
        self.dashboard_replay_container.display = bool(self.dolphie.replay_file)

        # Update the graph switch values based on the tab's metric data so each tab can have
        # its own set of visible metrics
        metric_switches = self.dolphie.app.tab_manager.metric_switches
        for metric_instance_name, metric_instance in self.dolphie.metric_manager.metrics.__dict__.items():
            for metric, metric_data in metric_instance.__dict__.items():
                if (
                    isinstance(metric_data, MetricManager.MetricData)
                    and metric_data.graphable
                    and metric_data.create_switch
                ):
                    switch = metric_switches.get(f"{metric_instance_name}-{metric}")
                    if switch:
                        switch.value = metric_data.visible

        # Layout redo log graphs based on whether active count is available
        show_active_count = bool(
            self.dolphie.global_status.get("Active_redo_log_count") and not self.dolphie.replay_file
        )
        self.graph_redo_log_data_written.styles.width = "55%" if show_active_count else "88%"
        self.graph_redo_log_active_count.display = show_active_count
        if show_active_count:
            self.dolphie.metric_manager.metrics.redo_log_active_count.Active_redo_log_count.visible = True

    def toggle_replication_panel_components(self):
        def toggle_container_display(container: Container, items, tracked: dict):
            container.display = bool(items)
            for widget in tracked.values():
                widget.parent.display = True

        toggle_container_display(
            self.galera_container, self.dolphie.galera_cluster_members, self.galera_widgets
        )
        toggle_container_display(
            self.replicas_container, self.dolphie.replica_manager.available_replicas, self.replica_widgets
        )
        toggle_container_display(
            self.group_replication_container, self.dolphie.group_replication_members, self.member_widgets
        )
        toggle_container_display(
            self.clusterset_container, self.dolphie.innodb_cluster_clustersets, self.clusterset_widgets
        )

    def remove_replication_panel_components(self):
        for tracked in (self.replica_widgets, self.member_widgets, self.galera_widgets, self.clusterset_widgets):
            for widget in tracked.values():
                widget.parent.remove()
            tracked.clear()



class TabManager:
    def __init__(self, app: App, config: Config):
        self.app = app
        self.config = config

        self.active_tab: Tab = None
        self.tabs: dict[str, Tab] = {}

        self.host_tabs = self.app.query_one("#host_tabs", Tabs)

        self.loading_hostgroups: bool = False
        self.last_replay_time: int = 0

        self.topbar = self.app.query_one(TopBar)

    def update_connection_status(self, tab: Tab, connection_status: ConnectionStatus):
        previous_status = tab.dolphie.connection_status
        tab.dolphie.connection_status = connection_status
        self.update_topbar(tab=tab)

        # Only rename when the host info may have changed (not for read_write/read_only toggles)
        if previous_status not in (ConnectionStatus.read_write, ConnectionStatus.read_only):
            self.rename_tab(tab)

    def update_topbar(self, tab: Tab):
        dolphie = tab.dolphie

        # If we're in daemon mode, don't waste time on this
        if dolphie.daemon_mode:
            return

        # Only update the topbar if we're on the active tab
        if self.active_tab is tab:
            if dolphie.connection_status:
                self.topbar.connection_status = dolphie.connection_status
                self.topbar.host = dolphie.host_with_port

                if (
                    dolphie.record_for_replay
                    and tab.replay_manager
                    and dolphie.connection_status != ConnectionStatus.disconnected
                ):
                    self.topbar.replay_file_size = tab.replay_manager.replay_file_size
                else:
                    self.topbar.replay_file_size = None
            else:
                self.topbar.replay_file_size = None
                self.topbar.connection_status = None
                self.topbar.host = ""

    async def create_ui_widgets(self):
        if self.config.daemon_mode:
            return

        await self.app.mount(
            LoadingIndicator(id="loading_indicator"),
            VerticalScroll(
                SpinnerWidget(id="spinner", text="Processing command"),
                Center(
                    Container(
                        Static(id="dashboard_replay", classes="dashboard_replay"),
                        Static(id="dashboard_replay_start_end", classes="dashboard_replay"),
                        Horizontal(
                            Button("⏪ Back", id="back_button", classes="replay_button"),
                            Button("⏸️  Pause", id="pause_button", classes="replay_button"),
                            Button("⏩ Forward", id="forward_button", classes="replay_button"),
                            Button("🔍 Seek", id="seek_button", classes="replay_button"),
                            classes="replay_buttons",
                        ),
                        ProgressBar(
                            id="dashboard_replay_progressbar", total=100, show_percentage=False, show_eta=False
                        ),
                        id="dashboard_replay_container",
                        classes="dashboard_replay",
                    )
                ),
                Container(
                    Center(
                        Static(id="dashboard_section_1", classes="panel_container"),
                        Static(id="dashboard_section_6", classes="panel_container"),
                        Static(id="dashboard_section_2", classes="panel_container"),
                        Static(id="dashboard_section_3", classes="panel_container"),
                        Static(id="dashboard_section_5", classes="panel_container"),
                        Static(id="dashboard_section_4", classes="panel_container"),
                    ),
                    Sparkline([], id="panel_dashboard_queries_qps"),
                    id="panel_dashboard",
                    classes="dashboard",
                ),
                Container(Label(id="metric_graphs_title", classes="panel_title"), TabbedContent(id="metric_graph_tabs"), id="panel_graphs"),
                Container(
                    Container(
                        Label(id="replication_title", classes="panel_title"),
                        Label(id="replication_variables"),
                        Center(
                            ScrollableContainer(Static(id="replication_status"), classes="replication_status"),
                            ScrollableContainer(
                                Static(id="replication_thread_applier"),
                                id="replication_thread_applier_container",
                                classes="replication_thread_applier",
                            ),
                        ),
                        id="replication_container",
                        classes="replication",
                    ),
                    Container(
                        Label(id="clusterset_title", classes="panel_title"),
                        Container(id="clusterset_grid"),
                        id="clusterset_container",
                        classes="group_replication",
                    ),
                    Container(
                        Label(id="galera_title", classes="panel_title"),
                        Static(id="galera_data"),
                        Container(id="galera_grid"),
                        id="galera_container",
                        classes="group_replication",
                    ),
                    Container(
                        Label(id="group_replication_title", classes="panel_title"),
                        Label(id="group_replication_data"),
                        Container(id="group_replication_grid"),
                        id="group_replication_container",
                        classes="group_replication",
                    ),
                    Container(
                        Label(id="replicas_title", classes="panel_title"),
                        LoadingIndicator(id="replicas_loading_indicator"),
                        Container(id="replicas_grid"),
                        id="replicas_container",
                        classes="replicas",
                    ),
                    id="panel_replication",
                    classes="replication_panel",
                ),
                Container(
                    Label(id="metadata_locks_title"),
                    DataTable(id="metadata_locks_datatable", show_cursor=False, zebra_stripes=True),
                    id="panel_metadata_locks",
                    classes="panel_container",
                ),
                Container(
                    Label(id="ddl_title"),
                    DataTable(id="ddl_datatable", show_cursor=False),
                    id="panel_ddl",
                    classes="panel_container",
                ),
                Container(
                    Label(id="pfs_metrics_title"),
                    RadioSet(
                        *(
                            [
                                RadioButton("Delta since last reset", id="pfs_metrics_delta", value=True),
                                RadioButton("Total since MySQL restart", id="pfs_metrics_total"),
                            ]
                        ),
                        id="pfs_metrics_radio_set",
                    ),
                    TabbedContent(id="pfs_metrics_tabs"),
                    id="panel_pfs_metrics",
                    classes="panel_container",
                ),
                Container(
                    Label(id="proxysql_hostgroup_summary_title"),
                    DataTable(id="proxysql_hostgroup_summary_datatable", show_cursor=False),
                    id="panel_proxysql_hostgroup_summary",
                    classes="panel_container",
                ),
                Container(
                    Label(id="proxysql_mysql_query_rules_title"),
                    DataTable(
                        id="proxysql_mysql_query_rules_datatable",
                        classes="proxysql_mysql_query_rules_datatable",
                        show_cursor=False,
                    ),
                    id="panel_proxysql_mysql_query_rules",
                    classes="panel_container",
                ),
                Container(
                    Label(id="proxysql_command_stats_title"),
                    DataTable(
                        id="proxysql_command_stats_datatable",
                        classes="proxysql_command_stats_datatable",
                        show_cursor=False,
                    ),
                    id="panel_proxysql_command_stats",
                    classes="panel_container",
                ),
                Container(
                    Label(id="processlist_title"),
                    DataTable(id="processlist_data", show_cursor=False),
                    id="panel_processlist",
                    classes="panel_container",
                ),
                Container(
                    Label(id="statements_summary_title"),
                    Label(
                        Text.from_markup(":bulb: [label]Prepared statements are not included in this panel"),
                        id="statements_summary_info",
                    ),
                    RadioSet(
                        *(
                            [
                                RadioButton("Delta since panel opened", id="statements_summarys_delta", value=True),
                                RadioButton("Delta since last sample", id="statements_summary_delta_last_sample"),
                                RadioButton("Total since MySQL restart", id="statements_summary_total"),
                            ]
                        ),
                        id="statements_summary_radio_set",
                    ),
                    DataTable(id="statements_summary_datatable", show_cursor=False),
                    id="panel_statements_summary",
                    classes="panel_container",
                ),
                classes="tab",
                id="main_container",
            ),
        )

        self.app.query_one("#main_container").display = False
        self.app.query_one("#loading_indicator").display = False

        panels = Panels()
        self.app.query_one("#metric_graphs_title", Label).update(panels.graphs.title)
        self.app.query_one("#replication_title", Label).update(panels.replication.title)
        self.app.query_one("#pfs_metrics_title", Label).update(panels.pfs_metrics.title)
        self.app.query_one("#statements_summary_title", Label).update(panels.statements_summary.title)

        # Loop the metric instances and create the graph tabs
        metric_manager = MetricManager.MetricManager(None)
        metric_graph_tabs = self.app.query_one("#metric_graph_tabs", TabbedContent)
        for metric_instance_name, metric_instance in metric_manager.metrics.__dict__.items():
            metric_tab_name = metric_instance.tab_name
            graph_names = metric_instance.graphs
            graph_tab_name = metric_instance.graph_tab_name

            if not self.app.query(f"#graph_tab_{metric_tab_name}"):
                await metric_graph_tabs.add_pane(
                    TabPane(
                        graph_tab_name,
                        Label(id=f"metric_graph_stats_{metric_tab_name}", classes="metric_graph_stats"),
                        Horizontal(id=f"metric_graph_container_{metric_tab_name}", classes="metric_graph_container"),
                        Horizontal(
                            id=f"switch_container_{metric_tab_name}",
                            classes="switch_container switch_container",
                        ),
                        id=f"graph_tab_{metric_tab_name}",
                        name=metric_tab_name,
                    )
                )

            tab_pane = self.app.query_one(f"#graph_tab_{metric_tab_name}", TabPane)
            graph_containers = {}
            for graph_name in graph_names:
                graph_container = (
                    "metric_graph_container2"
                    if graph_name in ["graph_system_network", "graph_system_disk_io"]
                    else "metric_graph_container"
                )
                container_id = f"{graph_container}_{metric_tab_name}"

                # Add graph_container2 only if it's needed
                if container_id not in graph_containers and not self.app.query(f"#{container_id}"):
                    if graph_container == "metric_graph_container2":
                        horizontal = Horizontal(id=container_id, classes="metric_graph_container2")
                        await tab_pane.mount(horizontal, after=1)
                        graph_containers[container_id] = horizontal

                if container_id not in graph_containers:
                    graph_containers[container_id] = self.app.query_one(f"#{container_id}", Horizontal)

                await graph_containers[container_id].mount(
                    MetricManager.Graph(id=f"{graph_name}", classes="panel_data")
                )

            switch_container = self.app.query_one(f"#switch_container_{metric_tab_name}", Horizontal)
            for metric, metric_data in metric_instance.__dict__.items():
                if (
                    isinstance(metric_data, MetricManager.MetricData)
                    and metric_data.graphable
                    and metric_data.create_switch
                    and not self.app.query(f"#switch_container_{metric_tab_name} #{metric_instance_name}-{metric}")
                ):
                    switch = Switch(animate=False, id=f"{metric_instance_name}-{metric}", name=metric_tab_name)
                    await switch_container.mount(Label(metric_data.label), switch)

                    # Toggle the switch if the metric is visible (means to enable it by default)
                    if metric_data.visible:
                        switch.toggle()

        # Add the PFS metrics tabs
        pfs_metrics_tabs = self.app.query_one("#pfs_metrics_tabs", TabbedContent)
        await pfs_metrics_tabs.add_pane(
            TabPane(
                "File I/O",
                DataTable(id="pfs_metrics_file_io_datatable", show_cursor=False),
                id="pfs_metrics_file_io_tab",
            )
        )
        await pfs_metrics_tabs.add_pane(
            TabPane(
                "Table I/O Waits Summary",
                Label(
                    Text.from_markup(":bulb: [label]Format for each metric: Wait time (Operations count)"),
                    id="pfs_metrics_format",
                ),
                DataTable(id="pfs_metrics_table_io_waits_datatable", show_cursor=False),
                id="pfs_metrics_table_io_waits_tab",
            ),
        )

        # Set what marker we use for graphs
        for graph in self.app.query(MetricManager.Graph):
            graph.marker = self.config.graph_marker

        # Cache switch references for fast lookup during tab switches
        self.metric_switches: dict[str, Switch] = {}
        for switch in self.app.query(Switch):
            self.metric_switches[switch.id] = switch

    async def create_tab(
        self, tab_name: str = None, hostgroup_member: HostGroupMember = None, switch_tab: bool = True
    ) -> Tab:
        if len(self.app.screen_stack) > 1:
            return

        tab_id = f"t{uuid.uuid4().hex}"

        # Create a new tab instance
        tab = Tab(id=tab_id, name=tab_name)

        # If we're using hostgroups
        config = copy.deepcopy(self.config)
        if hostgroup_member and self.config.hostgroup_hosts:
            config.replay_file = None
            config.host = hostgroup_member.host
            config.port = hostgroup_member.port
            tab.manual_tab_name = hostgroup_member.tab_title

            # If the hostgroup member has a credential profile, update config with its credentials
            credential_profile_data = self.config.credential_profiles.get(hostgroup_member.credential_profile)
            if credential_profile_data:
                config.credential_profile = hostgroup_member.credential_profile

                if credential_profile_data.user:
                    config.user = credential_profile_data.user
                if credential_profile_data.password:
                    config.password = credential_profile_data.password
                if credential_profile_data.socket:
                    config.socket = credential_profile_data.socket
                if credential_profile_data.ssl:
                    config.ssl = credential_profile_data.ssl

        # Create a new Dolphie instance
        dolphie = Dolphie(config=config, app=self.app)
        dolphie.tab_id = tab_id

        # Set the tab's Dolphie instance
        tab.dolphie = dolphie

        # If we're in daemon mode, stop here since we don't need to
        # do anything else with the UI
        if dolphie.daemon_mode:
            self.active_tab = tab
            self.tabs[tab_id] = tab

            for panel in dolphie.daemon_mode_panels:
                getattr(dolphie.panels, panel).visible = True

            return tab

        tab.save_references_to_components()

        # Create the tab in the UI
        initial_tab_name = "" if hostgroup_member else tab_name
        self.host_tabs.add_tab(TabWidget(initial_tab_name, id=tab_id))

        # Loop the metric instances and save references to the graphs and its labels
        for metric_instance in dolphie.metric_manager.metrics.__dict__.values():
            metric_tab_name = metric_instance.tab_name
            graph_names = metric_instance.graphs

            # Save references graph's labels
            setattr(tab, metric_tab_name, self.app.query_one(f"#metric_graph_stats_{metric_tab_name}"))

            # Save references to the graphs
            for graph_name in graph_names:
                setattr(tab, graph_name, self.app.query_one(f"#{graph_name}"))

        if tab.manual_tab_name:
            self.rename_tab(tab, tab.manual_tab_name)

        tab.replication_container.display = False
        tab.replicas_container.display = False
        tab.galera_container.display = False
        tab.group_replication_container.display = False

        # By default, hide all the panels
        for panel in tab.dolphie.panels.all():
            tab.get_panel_widget(panel).display = False

        # Set panels to be visible for the ones the user specifies
        for panel in dolphie.startup_panels:
            tab.get_panel_widget(panel).display = True
            getattr(dolphie.panels, panel).visible = True

        # Set static graph widths (only needs to happen once per tab)
        tab.graph_redo_log_bar.styles.width = "12%"
        tab.graph_redo_log_active_count.styles.width = "33%"
        tab.graph_adaptive_hash_index.styles.width = "50%"
        tab.graph_adaptive_hash_index_hit_ratio.styles.width = "50%"
        tab.graph_system_cpu.styles.width = "50%"
        tab.graph_system_network.styles.width = "50%"
        tab.graph_system_memory.styles.width = "50%"
        tab.graph_system_disk_io.styles.width = "50%"

        # Set the sparkline data to 0
        tab.sparkline.data = [0]

        self.tabs[tab_id] = tab

        if switch_tab:
            self.switch_tab(tab_id)

        return tab

    async def remove_tab(self, tab: Tab):
        self.host_tabs.remove_tab(tab.id)

    def rename_tab(self, tab: Tab, manual_name: str = None):
        if tab.dolphie.daemon_mode:
            return

        new_name = None
        if not manual_name and not tab.manual_tab_name:
            # host_with_port is the full host:port string, we want to split & truncate it to 24 characters
            host = tab.dolphie.host_with_port.split(":")[0][:24]
            if not host:
                return

            # If the last character isn't a letter or number, remove it
            if not host[-1].isalnum():
                host = host[:-1]

            new_name = f"{host}:[dark_gray]{tab.dolphie.port}"
        elif manual_name:
            new_name = manual_name
        elif tab.manual_tab_name:
            new_name = tab.manual_tab_name

        if new_name:
            tab.name = new_name

            if tab.dolphie.replay_file:
                new_name = f"[b recording][Replay][/b recording] {new_name}"

            self.host_tabs.get_tab(tab.id).label = Content.from_rich_text(new_name, console=self.app.console)

    def switch_tab(self, tab_id: str, set_active: bool = True):
        tab = self.get_tab(tab_id)
        if not tab:
            return

        # Update the active/current tab
        self.active_tab = tab

        # Prevent recursive calls
        if set_active:
            self.host_tabs.active = tab_id

        # Update the topbar
        self.update_topbar(tab=tab)

        tab.main_container.display = bool(tab.dolphie.main_db_connection.is_connected())

    def get_tab(self, id: str) -> Tab:
        return self.tabs.get(id)

    async def disconnect_tab(self, tab: Tab, update_topbar: bool = True):
        for worker in (tab.worker, tab.replicas_worker):
            if worker:
                worker.cancel()
        for timer in (tab.worker_timer, tab.replicas_worker_timer):
            if timer:
                timer.stop()

        tab.worker = tab.worker_timer = None
        tab.replicas_worker = tab.replicas_worker_timer = None

        tab.dolphie.main_db_connection.close()
        tab.dolphie.secondary_db_connection.close()

        tab.dolphie.replica_manager.remove_all_replicas()

        if self.active_tab is tab:
            tab.main_container.display = False
            tab.loading_indicator.display = False

        tab.sparkline.data = [0]
        tab.remove_replication_panel_components()

        if update_topbar:
            self.update_connection_status(tab=tab, connection_status=ConnectionStatus.disconnected)

    def setup_host_tab(self, tab: Tab):
        dolphie = tab.dolphie

        async def command_get_input(data):
            # Set tab_setup to False since it's only used when Dolphie first loads
            if self.config.tab_setup:
                self.config.tab_setup = False

            hostgroup = data.get("hostgroup")
            if hostgroup:
                self.config.record_for_replay = data.get("record_for_replay")
                dolphie.app.connect_as_hostgroup(hostgroup)
            else:
                # Disconnect the existing tab (cancel workers, close connections, cleanup UI)
                await self.disconnect_tab(tab, update_topbar=False)

                # Update connection details on the existing Dolphie instance
                host_port = data["host"].split(":")
                dolphie.host = host_port[0]
                dolphie.port = int(host_port[1]) if len(host_port) > 1 else 3306
                dolphie.credential_profile = data.get("credential_profile")
                dolphie.user = data.get("username")
                dolphie.password = data.get("password")
                dolphie.socket = data.get("socket_file")
                dolphie.ssl = data.get("ssl")
                dolphie.record_for_replay = data.get("record_for_replay")
                dolphie.replay_file = data.get("replay_file")

                # Reset all runtime state with the new connection details
                dolphie.reset_runtime_variables()
                tab.worker_cancel_error = None
                tab.replay_manager = None

                tab.loading_indicator.display = True
                tab.dashboard_replay_container.display = False

                if dolphie.replay_file:
                    tab.replay_manager = ReplayManager(dolphie)
                    if not tab.replay_manager.verify_replay_file():
                        tab.loading_indicator.display = False
                        self.setup_host_tab(tab)
                        return

                    self.update_connection_status(tab=tab, connection_status=ConnectionStatus.connected)
                    dolphie.app.run_worker_replay(tab.id)
                else:
                    dolphie.app.run_worker_main(tab.id)
                    dolphie.app.run_worker_replicas(tab.id)

        # If we're here because of a worker cancel error or manually disconnected,
        # we want to pre-populate the host/port
        if (
            tab.worker_cancel_error
            or dolphie.connection_status == ConnectionStatus.disconnected
            or self.config.tab_setup
        ):
            host = dolphie.host
            port = dolphie.port
        else:
            host = ""
            port = ""

        dolphie.app.push_screen(
            TabSetupModal(
                credential_profile=dolphie.credential_profile,
                credential_profiles=dolphie.config.credential_profiles,
                host=host,
                port=port,
                username=dolphie.user,
                password=dolphie.password,
                ssl=dolphie.ssl,
                record_for_replay=dolphie.record_for_replay,
                socket_file=dolphie.socket,
                hostgroups=dolphie.hostgroup_hosts.keys(),
                available_hosts=dolphie.tab_setup_available_hosts,
                replay_directory=dolphie.config.replay_dir,
                replay_files=dolphie.get_replay_files(),
                error_message=tab.worker_cancel_error,
            ),
            command_get_input,
        )
