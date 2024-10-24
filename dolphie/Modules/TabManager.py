import asyncio
import copy
import uuid
from dataclasses import dataclass
from typing import Dict, List

from textual.app import App
from textual.containers import (
    Center,
    Container,
    Horizontal,
    ScrollableContainer,
    VerticalScroll,
)
from textual.timer import Timer
from textual.widgets import (
    Button,
    DataTable,
    Label,
    LoadingIndicator,
    Sparkline,
    Static,
    Switch,
)
from textual.widgets import Tab as TabWidget
from textual.widgets import TabbedContent, TabPane, Tabs
from textual.worker import Worker

import dolphie.Modules.MetricManager as MetricManager
from dolphie.DataTypes import ConnectionStatus
from dolphie.Dolphie import Dolphie
from dolphie.Modules.ArgumentParser import Config, HostGroupMember
from dolphie.Modules.ManualException import ManualException
from dolphie.Modules.ReplayManager import ReplayManager
from dolphie.Widgets.spinner import SpinnerWidget
from dolphie.Widgets.tab_setup import TabSetupModal
from dolphie.Widgets.topbar import TopBar


@dataclass
class Tab:
    id: int
    name: str
    dolphie: Dolphie = None
    manual_tab_name: str = None

    replay_manager: ReplayManager = None

    worker: Worker = None
    worker_timer: Timer = None
    worker_cancel_error: ManualException = None
    worker_running: bool = False

    replay_manual_control: bool = False

    replicas_worker: Worker = None
    replicas_worker_timer: Timer = None
    replicas_worker_running: bool = False

    main_container: VerticalScroll = None
    metric_graph_tabs: TabbedContent = None
    loading_indicator: LoadingIndicator = None
    sparkline: Sparkline = None
    panel_dashboard: Container = None
    panel_graphs: Container = None
    panel_replication: Container = None
    panel_metadata_locks: Container = None
    panel_ddl: Container = None
    panel_processlist: Container = None
    panel_proxysql_hostgroup_summary: Container = None
    panel_proxysql_mysql_query_rules: Container = None
    panel_proxysql_command_stats: Container = None

    spinner: SpinnerWidget = None

    dashboard_replay_container: Container = None
    dashboard_replay_start_end: Static = None
    dashboard_replay: Static = None
    dashboard_section_1: Static = None
    dashboard_section_2: Static = None
    dashboard_section_3: Static = None
    dashboard_section_4: Static = None
    dashboard_section_5: Static = None

    ddl_title: Label = None
    ddl_datatable: DataTable = None

    metadata_locks_title: Label = None
    metadata_locks_datatable: DataTable = None

    processlist_title: Label = None
    processlist_datatable: DataTable = None

    replication_container_title: Static = None
    replication_container: Container = None
    replication_variables: Label = None
    replication_status: Static = None
    replication_thread_applier_container: ScrollableContainer = None
    replication_thread_applier: Static = None

    group_replication_container: Container = None
    group_replication_grid: Container = None
    group_replication_title: Label = None
    group_replication_data: Static = None

    replicas_container: Container = None
    replicas_grid: Container = None
    replicas_loading_indicator: LoadingIndicator = None
    replicas_title: Label = None

    proxysql_hostgroup_summary_title: Static = None
    proxysql_hostgroup_summary_datatable: DataTable = None

    proxysql_mysql_query_rules_title: Static = None
    proxysql_mysql_query_rules_datatable: DataTable = None

    proxysql_command_stats_title: Static = None
    proxysql_command_stats_datatable: DataTable = None

    def get_panel_widget(self, panel_name: str) -> Container:
        return getattr(self, f"panel_{panel_name}")


class TabManager:
    def __init__(self, app: App, config: Config):
        self.app = app
        self.config = config

        self.active_tab: Tab = None
        self.tabs: Dict[str, Tab] = {}

        self.host_tabs = self.app.query_one("#host_tabs", Tabs)
        self.host_tabs.display = False

        self.topbar = self.app.query_one(TopBar)

    def update_connection_status(self, tab: Tab, connection_status: ConnectionStatus):
        tab.dolphie.connection_status = connection_status

        self.update_topbar(tab=tab)
        self.rename_tab(tab)

    def update_topbar(self, tab: Tab):
        dolphie = tab.dolphie

        # If we're in daemon mode, don't waste time on this
        if dolphie.daemon_mode:
            return

        # Only update the topbar if we're on the active tab
        if tab.id == self.active_tab.id:
            if dolphie.connection_status:
                self.topbar.connection_status = dolphie.connection_status
                self.topbar.host = dolphie.host_with_port

                if dolphie.record_for_replay and tab.replay_manager:
                    self.topbar.replay_file_size = tab.replay_manager.replay_file_size
            else:
                self.topbar.connection_status = None
                self.topbar.replay_file_size = None
                self.topbar.host = ""

    def generate_tab_id(self) -> str:
        tab_id = str(uuid.uuid4()).replace("-", "")
        # Check if the first character is a digit since Textual doesn't allow that
        if tab_id[0].isdigit():
            # Prepend a letter to ensure it does not start with a digit
            tab_id = "a" + tab_id
        return tab_id

    async def create_ui_widgets(self):
        if self.config.daemon_mode:
            return

        await self.app.mount(
            LoadingIndicator(id="loading_indicator", classes="connection_loading_indicator"),
            VerticalScroll(
                SpinnerWidget(id="spinner", text="Processing command"),
                Container(
                    Static(id="dashboard_replay", classes="dashboard_replay"),
                    Static(id="dashboard_replay_start_end", classes="dashboard_replay"),
                    Horizontal(
                        Button("⏪ Back", id="back_button", classes="replay_button"),
                        Button("⏸️  Pause", id="pause_button", classes="replay_button"),
                        Button("⏩ Forward", id="forward_button", classes="replay_button"),
                        Button("🔍 Seek", id="seek_button", classes="replay_button"),
                        classes="button_container",
                    ),
                    id="dashboard_replay_container",
                    classes="dashboard_replay",
                ),
                Container(
                    Center(
                        Static(id="dashboard_section_1", classes="dashboard_section_1"),
                        Static(id="dashboard_section_2", classes="dashboard_section_2_information"),
                        Static(id="dashboard_section_3", classes="dashboard_section_3"),
                        Static(id="dashboard_section_5", classes="dashboard_section_5"),
                        Static(id="dashboard_section_4", classes="dashboard_section_4"),
                    ),
                    Sparkline([], id="panel_dashboard_queries_qps"),
                    id="panel_dashboard",
                    classes="panel_container dashboard",
                ),
                Container(
                    TabbedContent(id="metric_graph_tabs", classes="metrics_host_tabs"),
                    id="panel_graphs",
                    classes="panel_container",
                ),
                Container(
                    Static(id="replication_container_title", classes="replication_container_title"),
                    Container(
                        Label("[b]Replication\n"),
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
                        Label(id="group_replication_title"),
                        Label(id="group_replication_data"),
                        Container(id="group_replication_grid"),
                        id="group_replication_container",
                        classes="group_replication",
                    ),
                    Container(
                        Label(id="replicas_title"),
                        LoadingIndicator(id="replicas_loading_indicator"),
                        Container(id="replicas_grid"),
                        id="replicas_container",
                        classes="replicas",
                    ),
                    id="panel_replication",
                    classes="panel_container replication_panel",
                ),
                Container(
                    Label(id="metadata_locks_title"),
                    DataTable(id="metadata_locks_datatable", show_cursor=False, zebra_stripes=True),
                    id="panel_metadata_locks",
                    classes="metadata_locks",
                ),
                Container(
                    Label(id="ddl_title"),
                    DataTable(id="ddl_datatable", show_cursor=False),
                    id="panel_ddl",
                    classes="ddl",
                ),
                Container(
                    Label(id="proxysql_hostgroup_summary_title"),
                    DataTable(
                        id="proxysql_hostgroup_summary_datatable",
                        classes="proxysql_hostgroup_summary_datatable",
                        show_cursor=False,
                    ),
                    id="panel_proxysql_hostgroup_summary",
                    classes="proxysql_hostgroup_summary",
                ),
                Container(
                    Label(id="proxysql_mysql_query_rules_title"),
                    DataTable(
                        id="proxysql_mysql_query_rules_datatable",
                        classes="proxysql_mysql_query_rules_datatable",
                        show_cursor=False,
                    ),
                    id="panel_proxysql_mysql_query_rules",
                    classes="proxysql_mysql_query_rules",
                ),
                Container(
                    Label(id="proxysql_command_stats_title"),
                    DataTable(
                        id="proxysql_command_stats_datatable",
                        classes="proxysql_command_stats_datatable",
                        show_cursor=False,
                    ),
                    id="panel_proxysql_command_stats",
                    classes="proxysql_command_stats",
                ),
                Container(
                    Label(id="processlist_title"),
                    DataTable(id="processlist_data", show_cursor=False),
                    id="panel_processlist",
                    classes="processlist",
                ),
                classes="tab",
                id="main_container",
            ),
        )

        self.app.query_one("#main_container").display = False
        self.app.query_one("#loading_indicator").display = False

    async def create_tab(
        self, tab_name: str = None, hostgroup_member: HostGroupMember = None, switch_tab: bool = True
    ) -> Tab:
        if len(self.app.screen_stack) > 1:
            return

        tab_id = self.generate_tab_id()

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
        # create all the widgets for the tab as that wastes resources
        if dolphie.daemon_mode:
            self.active_tab = tab
            self.tabs[tab_id] = tab

            return tab

        # Create the tab in the UI
        intial_tab_name = "" if hostgroup_member else tab_name
        self.host_tabs.add_tab(TabWidget(intial_tab_name, id=tab_id))

        # Loop the metric instances and create the graph tabs
        for metric_instance in dolphie.metric_manager.metrics.__dict__.values():
            metric_tab_name = metric_instance.tab_name
            graph_names = metric_instance.graphs
            graph_tab_name = metric_instance.graph_tab_name

            graph_tab = self.app.query(f"#graph_tab_{metric_tab_name}")
            if not graph_tab:
                await self.app.query_one("#metric_graph_tabs", TabbedContent).add_pane(
                    TabPane(
                        graph_tab_name,
                        Label(id=f"stats_{metric_tab_name}", classes="stats_data"),
                        Horizontal(id=f"graph_container_{metric_tab_name}"),
                        Horizontal(
                            id=f"switch_container_{metric_tab_name}",
                            classes="switch_container switch_container",
                        ),
                        id=f"graph_tab_{metric_tab_name}",
                        name=metric_tab_name,
                    )
                )

            # Save references to the labels
            setattr(tab, metric_tab_name, self.app.query_one(f"#stats_{metric_tab_name}"))

            for graph_name in graph_names:
                graph = self.app.query(f"#{graph_name}")
                if not graph:
                    await self.app.query_one(f"#graph_container_{metric_tab_name}", Horizontal).mount(
                        MetricManager.Graph(id=f"{graph_name}", classes="panel_data")
                    )

                # Save references to the graphs
                setattr(tab, graph_name, self.app.query_one(f"#{graph_name}"))

            for metric, metric_data in metric_instance.__dict__.items():
                switch = self.app.query(f"#switch_container_{metric_tab_name} #{metric}")

                if not switch:
                    if (
                        isinstance(metric_data, MetricManager.MetricData)
                        and metric_data.graphable
                        and metric_data.create_switch
                    ):
                        await self.app.query_one(f"#switch_container_{metric_tab_name}", Horizontal).mount(
                            Label(metric_data.label)
                        )
                        await self.app.query_one(f"#switch_container_{metric_tab_name}", Horizontal).mount(
                            Switch(animate=False, id=metric, name=metric_tab_name)
                        )

                        # Toggle the switch if the metric is visible (means to enable it by default)
                        if metric_data.visible:
                            self.app.query_one(f"#switch_container_{metric_tab_name} #{metric}", Switch).toggle()

        if tab.manual_tab_name:
            self.rename_tab(tab, tab.manual_tab_name)

        # Save references to the widgets in the tab
        tab.main_container = self.app.query_one("#main_container", VerticalScroll)
        tab.metric_graph_tabs = self.app.query_one("#metric_graph_tabs", TabbedContent)
        tab.loading_indicator = self.app.query_one("#loading_indicator", LoadingIndicator)
        tab.sparkline = self.app.query_one("#panel_dashboard_queries_qps", Sparkline)
        tab.panel_dashboard = self.app.query_one("#panel_dashboard", Container)
        tab.panel_graphs = self.app.query_one("#panel_graphs", Container)
        tab.panel_replication = self.app.query_one("#panel_replication", Container)
        tab.panel_metadata_locks = self.app.query_one("#panel_metadata_locks", Container)
        tab.panel_processlist = self.app.query_one("#panel_processlist", Container)
        tab.panel_ddl = self.app.query_one("#panel_ddl", Container)
        tab.panel_proxysql_hostgroup_summary = self.app.query_one("#panel_proxysql_hostgroup_summary", Container)
        tab.panel_proxysql_mysql_query_rules = self.app.query_one("#panel_proxysql_mysql_query_rules", Container)
        tab.panel_proxysql_command_stats = self.app.query_one("#panel_proxysql_command_stats", Container)

        tab.spinner = self.app.query_one("#spinner", SpinnerWidget)
        tab.spinner.hide()

        tab.ddl_title = self.app.query_one("#ddl_title", Label)
        tab.ddl_datatable = self.app.query_one("#ddl_datatable", DataTable)
        tab.processlist_title = self.app.query_one("#processlist_title", Label)
        tab.processlist_datatable = self.app.query_one("#processlist_data", DataTable)
        tab.metadata_locks_title = self.app.query_one("#metadata_locks_title", Label)
        tab.metadata_locks_datatable = self.app.query_one("#metadata_locks_datatable", DataTable)
        tab.proxysql_hostgroup_summary_title = self.app.query_one("#proxysql_hostgroup_summary_title", Static)
        tab.proxysql_hostgroup_summary_datatable = self.app.query_one(
            "#proxysql_hostgroup_summary_datatable", DataTable
        )
        tab.proxysql_mysql_query_rules_title = self.app.query_one("#proxysql_mysql_query_rules_title", Static)
        tab.proxysql_mysql_query_rules_datatable = self.app.query_one(
            "#proxysql_mysql_query_rules_datatable", DataTable
        )
        tab.proxysql_command_stats_title = self.app.query_one("#proxysql_command_stats_title", Static)
        tab.proxysql_command_stats_datatable = self.app.query_one("#proxysql_command_stats_datatable", DataTable)

        tab.dashboard_replay_container = self.app.query_one("#dashboard_replay_container", Container)
        tab.dashboard_replay_start_end = self.app.query_one("#dashboard_replay_start_end", Static)
        tab.dashboard_replay = self.app.query_one("#dashboard_replay", Static)
        tab.dashboard_section_1 = self.app.query_one("#dashboard_section_1", Static)
        tab.dashboard_section_2 = self.app.query_one("#dashboard_section_2", Static)
        tab.dashboard_section_3 = self.app.query_one("#dashboard_section_3", Static)
        tab.dashboard_section_4 = self.app.query_one("#dashboard_section_4", Static)
        tab.dashboard_section_5 = self.app.query_one("#dashboard_section_5", Static)

        tab.group_replication_container = self.app.query_one("#group_replication_container", Container)
        tab.group_replication_grid = self.app.query_one("#group_replication_grid", Container)
        tab.group_replication_data = self.app.query_one("#group_replication_data", Static)
        tab.group_replication_title = self.app.query_one("#group_replication_title", Label)

        tab.replicas_grid = self.app.query_one("#replicas_grid", Container)
        tab.replicas_container = self.app.query_one("#replicas_container", Container)
        tab.replicas_title = self.app.query_one("#replicas_title", Label)
        tab.replicas_loading_indicator = self.app.query_one("#replicas_loading_indicator", LoadingIndicator)

        tab.replication_container_title = self.app.query_one("#replication_container_title", Static)
        tab.replication_container = self.app.query_one("#replication_container", Container)
        tab.replication_variables = self.app.query_one("#replication_variables", Label)
        tab.replication_status = self.app.query_one("#replication_status", Static)
        tab.replication_thread_applier_container = self.app.query_one(
            "#replication_thread_applier_container", ScrollableContainer
        )
        tab.replication_thread_applier = self.app.query_one("#replication_thread_applier", Static)

        tab.replication_container.display = False
        tab.replicas_container.display = False
        tab.group_replication_container.display = False

        # By default, hide all the panels
        for panel in tab.dolphie.panels.all():
            self.app.query_one(f"#panel_{panel}").display = False

        # Set panels to be visible for the ones the user specifies
        for panel in dolphie.startup_panels:
            self.app.query_one(f"#panel_{panel}").display = True
            setattr(getattr(dolphie.panels, panel), "visible", True)

        # Set what marker we use for graphs
        graphs = self.app.query(MetricManager.Graph)
        for graph in graphs:
            graph.marker = dolphie.graph_marker

        # Set the sparkline data to 0
        tab.sparkline.data = [0]

        self.host_tabs.display = True
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

            self.host_tabs.query(TabWidget).filter("#" + tab.id)[0].label = new_name

    def switch_tab(self, tab_id: int, set_active: bool = True):
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

        if not tab.dolphie.main_db_connection.is_connected():
            tab.main_container.display = False
        else:
            tab.main_container.display = True

    def get_tab(self, id: str) -> Tab:
        return self.tabs.get(id)

    def get_all_tabs(self) -> List[Tab]:
        all_tabs = []

        for tab in self.tabs.values():
            all_tabs.append(tab)

        return all_tabs

    async def disconnect_tab(self, tab: Tab, update_topbar: bool = True):
        if tab.worker_timer:
            tab.worker_timer.stop()
        if tab.replicas_worker_timer:
            tab.replicas_worker_timer.stop()

        if tab.worker:
            tab.worker.cancel()

        if tab.replicas_worker:
            tab.replicas_worker.cancel()

        tab.dolphie.main_db_connection.close()
        tab.dolphie.secondary_db_connection.close()

        tab.dolphie.replica_manager.disconnect_all()

        if self.active_tab.id == tab.id:
            tab.main_container.display = False
            tab.loading_indicator.display = False

        tab.sparkline.data = [0]

        # Hide all the panels as they will be re-enabled if needed
        tab.replication_container.display = False
        tab.replicas_container.display = False
        tab.group_replication_container.display = False

        # Remove all the replica and member containers
        queries = [f".replica_container_{tab.id}", f".member_container_{tab.id}"]
        for query in queries:
            for container in tab.dolphie.app.query(query):
                await container.remove()

        if update_topbar:
            self.update_connection_status(tab=tab, connection_status=ConnectionStatus.disconnected)

    def setup_host_tab(self, tab: Tab):
        dolphie = tab.dolphie

        async def command_get_input(data):
            # Set tab_setup to false since it's only used when Dolphie first loads
            if self.config.tab_setup:
                self.config.tab_setup = False

            host_port = data["host"].split(":")

            dolphie.credential_profile = data.get("credential_profile")
            dolphie.host = host_port[0]
            dolphie.port = int(host_port[1]) if len(host_port) > 1 else 3306
            dolphie.user = data.get("username")
            dolphie.password = data.get("password")
            hostgroup = data.get("hostgroup")
            dolphie.socket = data.get("socket_file")
            dolphie.ssl = data.get("ssl")

            if hostgroup:
                dolphie.app.connect_as_hostgroup(hostgroup)
            else:
                dolphie.replay_file = data.get("replay_file")

                await self.disconnect_tab(tab)

                tab.loading_indicator.display = True
                while True:
                    if not tab.worker_running and not tab.replicas_worker_running:
                        tab.worker_cancel_error = ""

                        tab.dashboard_replay_container.display = False

                        dolphie.reset_runtime_variables()

                        if dolphie.replay_file:
                            tab.replay_manager = ReplayManager(dolphie)
                            self.update_connection_status(tab=tab, connection_status=ConnectionStatus.connected)
                            dolphie.app.run_worker_replay(tab.id)
                        else:
                            dolphie.app.run_worker_main(tab.id)
                            dolphie.app.run_worker_replicas(tab.id)

                        break

                    await asyncio.sleep(0.25)

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
                socket_file=dolphie.socket,
                hostgroups=dolphie.hostgroup_hosts.keys(),
                available_hosts=dolphie.tab_setup_available_hosts,
                replay_directory=dolphie.config.replay_dir,
                replay_files=dolphie.get_replay_files(),
                error_message=tab.worker_cancel_error,
            ),
            command_get_input,
        )
