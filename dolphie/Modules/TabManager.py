from __future__ import annotations

import copy
import os
import uuid
from typing import TYPE_CHECKING, Any

from textual.containers import (
    Center,
    Container,
    ScrollableContainer,
    VerticalScroll,
)
from textual.timer import Timer
from textual.widget import Widget
from textual.widgets import (
    Label,
    LoadingIndicator,
    ProgressBar,
    RadioButton,
    RadioSet,
    Sparkline,
    Static,
    TabbedContent,
    TabPane,
    Tabs,
)
from textual.widgets import Tab as TabWidget
from textual.worker import Worker

from dolphie.DataTypes import ConnectionSource, ConnectionStatus, ConnectionStatusType, Panels
from dolphie.Dolphie import Dolphie
from dolphie.Modules.ArgumentParser import Config, HostGroupMember
from dolphie.Modules.Functions import merge_filters
from dolphie.Modules.ManualException import ManualException
from dolphie.Modules.ReplayManager import ReplayManager
from dolphie.Modules.Theme import ThemedDataTable as DataTable
from dolphie.Modules.Theme import themed_content
from dolphie.Widgets.MetricGraphDashboard import MetricGraphDashboard
from dolphie.Widgets.ReplayControls import ReplayControls
from dolphie.Widgets.SpinnerWidget import SpinnerWidget
from dolphie.Widgets.TabSetupModal import TabSetupModal
from dolphie.Widgets.TopBar import TopBar

if TYPE_CHECKING:
    from dolphie.App import DolphieApp


class Tab:
    def __init__(
        self,
        id: str,
        name: str,
        dolphie: Dolphie | None = None,
        manual_tab_name: str | None = None,
        replay_manager: ReplayManager | None = None,
    ):
        self.id = id
        self.name = name
        self._dolphie = dolphie
        self.manual_tab_name = manual_tab_name
        self.replay_manager = replay_manager

        self.worker: Worker[Any] | None = None
        self.worker_timer: Timer | None = None
        self.worker_cancel_error: ManualException | None = None

        self.replay_manual_control: bool = False

        self.replicas_worker: Worker[Any] | None = None
        self.replicas_worker_timer: Timer | None = None

        # Track mounted grid widgets to avoid DOM queries each refresh cycle
        self.channel_widgets: dict[str, Static] = {}
        self.clusterset_widgets: dict[str, Static] = {}
        self.galera_widgets: dict[str, Static] = {}
        self.member_widgets: dict[str, Static] = {}
        self.replica_widgets: dict[str, Static] = {}

    @property
    def dolphie(self) -> Dolphie:
        if self._dolphie is None:
            raise RuntimeError("Tab has not been initialized with a Dolphie instance")
        return self._dolphie

    @dolphie.setter
    def dolphie(self, value: Dolphie) -> None:
        self._dolphie = value

    def save_references_to_components(self):
        app = self.dolphie.app

        self.main_container = app.query_one("#main_container", VerticalScroll)
        self.graph_dashboard = app.query_one(MetricGraphDashboard)
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

        self.replay_controls = app.query_one(ReplayControls)
        self.dashboard_replay_container = self.replay_controls
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
        self.replication_status_grid = app.query_one("#replication_status_grid", Container)
        self.replication_status_single = app.query_one("#replication_status_single", Static)
        self.replication_thread_applier_container = app.query_one(
            "#replication_thread_applier_container", ScrollableContainer
        )
        self.replication_thread_applier = app.query_one("#replication_thread_applier", Static)

    def get_panel_widget(self, panel_name: str) -> Container:
        return getattr(self, f"panel_{panel_name}")

    def refresh_replay_dashboard_section(self):
        replay_manager = self.replay_manager
        if not self.dolphie.replay_file or replay_manager is None:
            return

        min_timestamp = replay_manager.min_replay_timestamp
        max_timestamp = replay_manager.max_replay_timestamp
        current_timestamp = replay_manager.current_replay_timestamp

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
        if replay_manager.current_replay_id == replay_manager.min_replay_id:
            current_position = 0
        else:
            current_position = replay_manager.current_replay_id - replay_manager.min_replay_id + 1

        self.dashboard_replay_progressbar.update(progress=current_position, total=replay_manager.total_replay_rows)

        # Keep the controls in sync with the active tab: reflect this tab's pause state
        # and disable Back/Forward at the boundaries so they reflect what's possible.
        self.replay_controls.paused = self.dolphie.pause_refresh
        self.replay_controls.set_boundary_states(
            at_start=replay_manager.current_replay_id <= replay_manager.min_replay_id,
            at_end=replay_manager.current_replay_id >= replay_manager.max_replay_id,
        )

    def toggle_entities_displays(self):
        self.dashboard_section_6.display = bool(self.dolphie.system_utilization)

        if self.dolphie.connection_source == ConnectionSource.mysql:
            self.dashboard_section_5.display = bool(
                self.dolphie.replication_status and not self.dolphie.panels.replication.visible
            )
        elif self.dolphie.connection_source == ConnectionSource.proxysql:
            self.dashboard_section_5.display = False

    def sync_shared_ui(self) -> None:
        """Bind shared replay and graph widgets to this host."""
        self.main_container.display = True

        # Only show the replay section if we're in replay mode
        self.dashboard_replay_container.display = bool(self.dolphie.replay_file)
        self.graph_dashboard.bind_host(self.dolphie)

    def toggle_replication_panel_components(self):
        def toggle_container_display(container: Container, items, tracked: dict[str, Static]):
            container.display = bool(items)
            for widget in tracked.values():
                if widget.parent is not None:
                    widget.parent.display = True

        toggle_container_display(self.galera_container, self.dolphie.galera_cluster_members, self.galera_widgets)
        toggle_container_display(
            self.replicas_container, self.dolphie.replica_manager.discovery_count, self.replica_widgets
        )
        toggle_container_display(
            self.group_replication_container, self.dolphie.group_replication_members, self.member_widgets
        )
        toggle_container_display(self.clusterset_container, self.dolphie.clusterset_instances, self.clusterset_widgets)

    def remove_replication_panel_components(self):
        for tracked in (
            self.channel_widgets,
            self.replica_widgets,
            self.member_widgets,
            self.galera_widgets,
            self.clusterset_widgets,
        ):
            for widget in tracked.values():
                if widget.parent is not None:
                    if isinstance(widget.parent, Widget):
                        widget.parent.remove()
            tracked.clear()


class TabManager:
    def __init__(self, app: DolphieApp, config: Config):
        self.app = app
        self.config = config

        self.active_tab: Tab | None = None
        self.tabs: dict[str, Tab] = {}

        self.host_tabs = self.app.query_one("#host_tabs", Tabs)

        self.loading_hostgroups: bool = False
        self.last_replay_time: int = 0
        self.metric_graph_dashboard: MetricGraphDashboard | None = None

        self.topbar = self.app.query_one(TopBar)

    def update_connection_status(self, tab: Tab, connection_status: ConnectionStatusType):
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

        self.metric_graph_dashboard = MetricGraphDashboard(
            marker=self.config.graph_marker,
            id="metric_graph_dashboard",
        )
        await self.app.mount(
            LoadingIndicator(id="loading_indicator"),
            VerticalScroll(
                SpinnerWidget(id="spinner", text="Processing command"),
                Center(
                    ReplayControls(id="dashboard_replay_container", classes="dashboard_replay"),
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
                Container(
                    Label(id="metric_graphs_title", classes="panel_title"),
                    self.metric_graph_dashboard,
                    id="panel_graphs",
                ),
                Container(
                    Container(
                        Label(id="replication_title", classes="panel_title"),
                        Label(id="replication_variables"),
                        Container(id="replication_status_grid", classes="replication_status_grid"),
                        Center(
                            ScrollableContainer(
                                Static(id="replication_status_single"),
                                classes="replication_status",
                            ),
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
                        "💡 [$label]Prepared statements are not included in this panel",
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
                    "💡 [$label]Format for each metric: Wait time (Operations count)",
                    id="pfs_metrics_format",
                ),
                DataTable(id="pfs_metrics_table_io_waits_datatable", show_cursor=False),
                id="pfs_metrics_table_io_waits_tab",
            ),
        )

    async def create_tab(
        self,
        tab_name: str | None = None,
        hostgroup_member: HostGroupMember | None = None,
        switch_tab: bool = True,
    ) -> Tab:
        tab_id = f"t{uuid.uuid4().hex}"

        # Create a new tab instance
        tab = Tab(id=tab_id, name=tab_name or "")

        # If we're using hostgroups
        config = copy.deepcopy(self.config)
        if hostgroup_member and self.config.hostgroup_hosts:
            config.replay_file = None
            config.host = hostgroup_member.host
            if hostgroup_member.port is not None:
                config.port = hostgroup_member.port
            tab.manual_tab_name = hostgroup_member.tab_title

            # If the hostgroup member has a credential profile, update config with its credentials
            credential_profile_data = (
                self.config.credential_profiles.get(hostgroup_member.credential_profile)
                if hostgroup_member.credential_profile
                else None
            )
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
                if credential_profile_data.filter_values:
                    # Merged so the profile only overrides the filters it sets, same as at startup
                    config.filter_values = merge_filters(config.filter_values, credential_profile_data.filter_values)
                if not tab.manual_tab_name and credential_profile_data.tab_title:
                    tab.manual_tab_name = credential_profile_data.tab_title
        elif config.credential_profile:
            # When launched with -C alone, use the credential profile's tab_title if set
            credential_profile_data = self.config.credential_profiles.get(config.credential_profile)
            if credential_profile_data and credential_profile_data.tab_title:
                tab.manual_tab_name = credential_profile_data.tab_title

        # Create a new Dolphie instance
        dolphie = Dolphie(config=config, app=self.app)

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
        initial_tab_name = "" if hostgroup_member else (tab_name or "")
        self.host_tabs.add_tab(TabWidget(initial_tab_name, id=tab_id))

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

        # Set the sparkline data to 0
        tab.sparkline.data = [0]

        self.tabs[tab_id] = tab

        if switch_tab:
            self.switch_tab(tab_id)

        return tab

    async def remove_tab(self, tab: Tab):
        self.host_tabs.remove_tab(tab.id)

    def rename_tab(self, tab: Tab, manual_name: str | None = None):
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

            new_name = f"{host}:[$dark_gray]{tab.dolphie.port}[/$dark_gray]"
        elif manual_name:
            new_name = manual_name
        elif tab.manual_tab_name:
            new_name = tab.manual_tab_name

        if new_name:
            tab.name = new_name

            if tab.dolphie.replay_file:
                new_name = f"[$b_recording][Replay][/$b_recording] {new_name}"

            tab_widget = self.host_tabs.get_tab(tab.id)
            if tab_widget is not None:
                tab_widget.label = themed_content(new_name)

    def switch_tab(self, tab_id: str, set_active: bool = True):
        tab = self.get_tab(tab_id)
        if not tab:
            return

        # Replication grids live in the shared UI. Remove widgets owned by inactive
        # host tabs so their cards cannot accumulate beside the active tab's cards.
        for inactive_tab in self.tabs.values():
            if inactive_tab is not tab:
                inactive_tab.remove_replication_panel_components()

        # Update the active/current tab
        self.active_tab = tab

        # Prevent recursive calls
        if set_active:
            self.host_tabs.active = tab_id

        # Update the topbar
        self.update_topbar(tab=tab)

        tab.main_container.display = bool(tab.dolphie.main_db_connection.is_connected())
        tab.graph_dashboard.bind_host(tab.dolphie, render=tab.panel_graphs.display)
        self.app.sync_replication_ui(tab)

    def get_tab(self, id: str) -> Tab | None:
        return self.tabs.get(id)

    async def disconnect_tab(self, tab: Tab, update_topbar: bool = True, wait_for_workers: bool = True):
        # Stop timers first so they can't fire (and queue a fresh worker iteration)
        # while we yield to the event loop in worker.wait() below.
        for timer in (tab.worker_timer, tab.replicas_worker_timer):
            if timer:
                timer.stop()
        tab.worker_timer = tab.replicas_worker_timer = None

        for worker in (tab.worker, tab.replicas_worker):
            if worker:
                worker.cancel()
        # Wait for workers to fully stop before modifying shared state to avoid
        # a race condition where a running worker reads partially-updated state.
        # Thread workers run to completion even after cancel(), so their state
        # transitions to SUCCESS rather than CANCELLED — which triggers
        # on_worker_state_changed to reschedule a new worker_timer. Stop timers
        # again after the await to nuke any timer set during the await window.
        if wait_for_workers:
            for worker in (tab.worker, tab.replicas_worker):
                if worker and not worker.is_finished:
                    try:
                        await worker.wait()
                    except Exception:
                        pass
        for timer in (tab.worker_timer, tab.replicas_worker_timer):
            if timer:
                timer.stop()

        tab.worker = tab.worker_timer = None
        tab.replicas_worker = tab.replicas_worker_timer = None

        tab.dolphie.main_db_connection.close()
        tab.dolphie.secondary_db_connection.close()

        tab.dolphie.replica_manager.remove_all_replicas()

        if not tab.dolphie.daemon_mode:
            if self.active_tab is tab:
                tab.main_container.display = False
                tab.loading_indicator.display = False

            tab.sparkline.data = [0]
            tab.remove_replication_panel_components()

        if tab.dolphie.daemon_mode:
            tab.dolphie.connection_status = ConnectionStatus.disconnected
        elif update_topbar:
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
