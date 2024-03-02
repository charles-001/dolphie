import asyncio
from dataclasses import dataclass
from typing import List

import dolphie.Modules.MetricManager as MetricManager
from dolphie import Dolphie
from dolphie.DataTypes import ConnectionStatus
from dolphie.Modules.ArgumentParser import Config
from dolphie.Modules.ManualException import ManualException
from dolphie.Widgets.host_setup import HostSetupModal
from dolphie.Widgets.spinner import SpinnerWidget
from dolphie.Widgets.topbar import TopBar
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
    DataTable,
    Label,
    LoadingIndicator,
    Sparkline,
    Static,
    Switch,
    TabbedContent,
    TabPane,
)
from textual.worker import Worker


@dataclass
class Tab:
    id: int
    name: str
    dolphie: Dolphie = None
    manual_tab_name: str = None

    worker: Worker = None
    worker_timer: Timer = None
    worker_cancel_error: ManualException = None
    worker_running: bool = False

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
    # panel_innodb_trx_locks: Container = None
    panel_metadata_locks: Container = None
    panel_ddl: Container = None
    panel_processlist: Container = None

    spinner: SpinnerWidget = None

    dashboard_host_information: Static = None
    dashboard_innodb: Static = None
    dashboard_binary_log: Static = None
    dashboard_statistics: Static = None
    dashboard_replication: Static = None

    ddl_title: Label = None
    ddl_datatable: DataTable = None

    # innodb_trx_locks_title: Label = None
    # innodb_trx_locks_datatable: DataTable = None

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

    cluster_data: Static = None

    def get_panel_widget(self, panel_name: str) -> Container:
        return getattr(self, f"panel_{panel_name}")


class TabManager:
    def __init__(self, app: App, config: Config):
        self.app = app
        self.config = config

        self.active_tab: Tab = None
        self.tabs: dict = {}
        self.tab_id_counter: int = 1

        self.host_tabs = self.app.query_one("#host_tabs", TabbedContent)
        self.host_tabs.display = False

        self.topbar = self.app.query_one(TopBar)

    def update_topbar(self, tab: Tab, connection_status: ConnectionStatus):
        dolphie = tab.dolphie

        dolphie.connection_status = connection_status

        # Only update the topbar if we're on the active tab
        if tab.id == self.active_tab.id:
            if dolphie.connection_status:
                self.topbar.connection_status = dolphie.connection_status
                self.topbar.host = dolphie.mysql_host
            else:
                self.topbar.connection_status = None
                self.topbar.host = ""

    async def create_tab(self, tab_name: str, use_hostgroup: bool = False, switch_tab: bool = True) -> Tab:
        tab_id = self.tab_id_counter

        if len(self.app.screen_stack) > 1:
            return

        # Create our new tab instance
        tab = Tab(id=tab_id, name=tab_name)

        # If we're using hostgroups
        if use_hostgroup and self.config.hostgroup_hosts:
            host = tab_name
            # Split entry by "~" to get custom tab name if there is one
            tab_name_split_rename = tab_name.split("~")

            if len(tab_name_split_rename) == 2:
                # Get the hostname from the first part of the split
                host = tab_name_split_rename[0]
                tab.manual_tab_name = tab_name_split_rename[1]

            # Split tab name by ":" to extract host and port
            tab_host_split = host.split(":")

            # Extract host and port information
            original_config_port = self.config.port

            self.config.host = tab_host_split[0]
            self.config.port = tab_host_split[1] if len(tab_host_split) > 1 else self.config.port

        # Create a new Dolphie instance
        dolphie = Dolphie(config=self.config, app=self.app)
        dolphie.tab_id = tab_id
        dolphie.tab_name = tab_name

        # Save the Dolphie instance to the tab
        tab.dolphie = dolphie

        # Revert the port back to its original value
        if use_hostgroup and self.config.hostgroup_hosts and len(tab_host_split) > 1:
            self.config.port = original_config_port

        intial_tab_name = "" if use_hostgroup else tab_name
        await self.host_tabs.add_pane(
            TabPane(
                intial_tab_name,
                LoadingIndicator(id=f"loading_indicator_{tab_id}", classes="connection_loading_indicator"),
                SpinnerWidget(id=f"spinner_{tab_id}", text="Processing command"),
                VerticalScroll(
                    Container(
                        Center(
                            Static(id=f"dashboard_host_information_{tab_id}", classes="dashboard_host_information"),
                            Static(id=f"dashboard_innodb_{tab_id}", classes="dashboard_innodb_information"),
                            Static(id=f"dashboard_binary_log_{tab_id}", classes="dashboard_binary_log"),
                            Static(id=f"dashboard_replication_{tab_id}", classes="dashboard_replication"),
                            Static(id=f"dashboard_statistics_{tab_id}", classes="dashboard_statistics"),
                        ),
                        Sparkline([], id=f"panel_dashboard_queries_qps_{tab_id}"),
                        id=f"panel_dashboard_{tab_id}",
                        classes="panel_container dashboard",
                    ),
                    Container(
                        TabbedContent(id=f"metric_graph_tabs_{tab_id}", classes="metrics_host_tabs"),
                        id=f"panel_graphs_{tab_id}",
                        classes="panel_container",
                    ),
                    Container(
                        Static(id=f"replication_container_title_{tab_id}", classes="replication_container_title"),
                        Container(
                            Label("[b]Replication\n"),
                            Label(id=f"replication_variables_{tab_id}"),
                            Center(
                                ScrollableContainer(
                                    Static(id=f"replication_status_{tab_id}"), classes="replication_status"
                                ),
                                ScrollableContainer(
                                    Static(id=f"replication_thread_applier_{tab_id}"),
                                    id=f"replication_thread_applier_container_{tab_id}",
                                    classes="replication_thread_applier",
                                ),
                            ),
                            id=f"replication_container_{tab_id}",
                            classes="replication",
                        ),
                        Container(
                            Label(id=f"group_replication_title_{tab_id}"),
                            Label(id=f"group_replication_data_{tab_id}"),
                            Container(id=f"group_replication_grid_{tab_id}"),
                            id=f"group_replication_container_{tab_id}",
                            classes="group_replication",
                        ),
                        Static(id=f"cluster_data_{tab_id}"),
                        Container(
                            Label(id=f"replicas_title_{tab_id}"),
                            LoadingIndicator(id=f"replicas_loading_indicator_{tab_id}"),
                            Container(id=f"replicas_grid_{tab_id}"),
                            id=f"replicas_container_{tab_id}",
                            classes="replicas",
                        ),
                        id=f"panel_replication_{tab_id}",
                        classes="panel_container replication_panel",
                    ),
                    # Container(
                    #     Label(id=f"innodb_trx_locks_title_{tab_id}"),
                    #     DataTable(id=f"innodb_trx_locks_datatable_{tab_id}", show_cursor=False),
                    #     id=f"panel_innodb_trx_locks_{tab_id}",
                    #     classes="innodb_trx_locks",
                    # ),
                    Container(
                        Label(id=f"metadata_locks_title_{tab_id}"),
                        DataTable(id=f"metadata_locks_datatable_{tab_id}", show_cursor=False, zebra_stripes=True),
                        id=f"panel_metadata_locks_{tab_id}",
                        classes="metadata_locks",
                    ),
                    Container(
                        Label(id=f"ddl_title_{tab_id}"),
                        DataTable(id=f"ddl_datatable_{tab_id}", show_cursor=False),
                        id=f"panel_ddl_{tab_id}",
                        classes="ddl",
                    ),
                    Container(
                        Label(id=f"processlist_title_{tab_id}"),
                        DataTable(id=f"processlist_data_{tab_id}", show_cursor=False),
                        id=f"panel_processlist_{tab_id}",
                        classes="processlist",
                    ),
                    classes="tab",
                    id=f"main_container_{tab_id}",
                ),
                id=f"tab_{tab_id}",
                name=tab_id,
            ),
        )

        metrics = MetricManager.MetricManager().metrics
        metric_tab_labels = [
            ("DML", metrics.dml, True),
            ("Locks", metrics.locks, True),
            ("Table Cache", metrics.table_cache, True),
            ("Threads", metrics.threads, True),
            ("BP Requests", metrics.buffer_pool_requests, True),
            ("Checkpoint", metrics.checkpoint, False),
            ("Redo Log", metrics.redo_log, False),
            ("AHI", metrics.adaptive_hash_index, True),
            ("Temp Objects", metrics.temporary_objects, True),
            ("Aborted Connections", metrics.aborted_connections, True),
            ("Disk I/O", metrics.disk_io, True),
            ("Replication", metrics.replication_lag, False),
        ]

        for tab_formatted_name, metric_instance, create_switches in metric_tab_labels:
            metric_tab_name = metric_instance.tab_name
            graph_names = metric_instance.graphs

            await self.app.query_one(f"#metric_graph_tabs_{tab_id}", TabbedContent).add_pane(
                TabPane(
                    tab_formatted_name,
                    Label(id=f"stats_{metric_tab_name}_{tab_id}", classes="stats_data"),
                    Horizontal(id=f"graph_container_{metric_tab_name}_{tab_id}"),
                    Horizontal(
                        id=f"switch_container_{metric_tab_name}_{tab_id}",
                        classes=f"switch_container_{tab_id} switch_container",
                    ),
                    id=f"graph_tab_{metric_tab_name}_{tab_id}",
                    name=metric_tab_name,
                )
            )
            # Save references to the labels
            setattr(tab, metric_tab_name, self.app.query_one(f"#stats_{metric_tab_name}_{tab_id}"))

            if metric_tab_name == "redo_log":
                graph_names = ["graph_redo_log", "graph_redo_log_active_count", "graph_redo_log_bar"]
            elif metric_tab_name == "adaptive_hash_index":
                graph_names = ["graph_adaptive_hash_index", "graph_adaptive_hash_index_hit_ratio"]

            for graph_name in graph_names:
                await self.app.query_one(f"#graph_container_{metric_tab_name}_{tab_id}", Horizontal).mount(
                    MetricManager.Graph(id=f"{graph_name}_{tab_id}", classes="panel_data")
                )
                # Save references to the graphs
                setattr(tab, graph_name, self.app.query_one(f"#{graph_name}_{tab_id}"))

            if create_switches:
                for metric, metric_data in metric_instance.__dict__.items():
                    if isinstance(metric_data, MetricManager.MetricData) and metric_data.graphable:
                        await self.app.query_one(f"#switch_container_{metric_tab_name}_{tab_id}", Horizontal).mount(
                            Label(metric_data.label)
                        )
                        await self.app.query_one(f"#switch_container_{metric_tab_name}_{tab_id}", Horizontal).mount(
                            Switch(animate=False, id=metric, name=metric_tab_name)
                        )

        # Save the tab instance to the tabs dictionary
        self.tabs[tab_id] = tab

        if tab.manual_tab_name:
            self.rename_tab(tab, tab.manual_tab_name)

        # Save references to the widgets in the tab
        tab.main_container = self.app.query_one(f"#main_container_{tab.id}", VerticalScroll)
        tab.metric_graph_tabs = self.app.query_one(f"#metric_graph_tabs_{tab.id}", TabbedContent)
        tab.loading_indicator = self.app.query_one(f"#loading_indicator_{tab.id}", LoadingIndicator)
        tab.sparkline = self.app.query_one(f"#panel_dashboard_queries_qps_{tab.id}", Sparkline)
        tab.panel_dashboard = self.app.query_one(f"#panel_dashboard_{tab.id}", Container)
        tab.panel_graphs = self.app.query_one(f"#panel_graphs_{tab.id}", Container)
        tab.panel_replication = self.app.query_one(f"#panel_replication_{tab.id}", Container)
        # tab.panel_innodb_trx_locks = self.app.query_one(f"#panel_innodb_trx_locks_{tab.id}", Container)
        tab.panel_metadata_locks = self.app.query_one(f"#panel_metadata_locks_{tab.id}", Container)
        tab.panel_processlist = self.app.query_one(f"#panel_processlist_{tab.id}", Container)
        tab.panel_ddl = self.app.query_one(f"#panel_ddl_{tab.id}", Container)

        tab.spinner = self.app.query_one(f"#spinner_{tab.id}", SpinnerWidget)
        tab.spinner.hide()

        tab.ddl_title = self.app.query_one(f"#ddl_title_{tab.id}", Label)
        tab.ddl_datatable = self.app.query_one(f"#ddl_datatable_{tab.id}", DataTable)
        tab.processlist_title = self.app.query_one(f"#processlist_title_{tab.id}", Label)
        tab.processlist_datatable = self.app.query_one(f"#processlist_data_{tab.id}", DataTable)
        # tab.innodb_trx_locks_title = self.app.query_one(f"#innodb_trx_locks_title_{tab.id}", Label)
        # tab.innodb_trx_locks_datatable = self.app.query_one(f"#innodb_trx_locks_datatable_{tab.id}", DataTable)
        tab.metadata_locks_title = self.app.query_one(f"#metadata_locks_title_{tab.id}", Label)
        tab.metadata_locks_datatable = self.app.query_one(f"#metadata_locks_datatable_{tab.id}", DataTable)

        tab.dashboard_host_information = self.app.query_one(f"#dashboard_host_information_{tab.id}", Static)
        tab.dashboard_innodb = self.app.query_one(f"#dashboard_innodb_{tab.id}", Static)
        tab.dashboard_binary_log = self.app.query_one(f"#dashboard_binary_log_{tab.id}", Static)
        tab.dashboard_statistics = self.app.query_one(f"#dashboard_statistics_{tab.id}", Static)
        tab.dashboard_replication = self.app.query_one(f"#dashboard_replication_{tab.id}", Static)

        tab.group_replication_container = self.app.query_one(f"#group_replication_container_{tab.id}", Container)
        tab.group_replication_grid = self.app.query_one(f"#group_replication_grid_{tab.id}", Container)

        tab.replicas_grid = self.app.query_one(f"#replicas_grid_{tab.id}", Container)
        tab.replicas_container = self.app.query_one(f"#replicas_container_{tab.id}", Container)

        tab.group_replication_data = self.app.query_one(f"#group_replication_data_{tab.id}", Static)
        tab.group_replication_title = self.app.query_one(f"#group_replication_title_{tab.id}", Label)
        tab.replicas_title = self.app.query_one(f"#replicas_title_{tab.id}", Label)
        tab.replicas_loading_indicator = self.app.query_one(f"#replicas_loading_indicator_{tab.id}", LoadingIndicator)

        tab.cluster_data = self.app.query_one(f"#cluster_data_{tab.id}", Static)

        tab.replication_container_title = self.app.query_one(f"#replication_container_title_{tab.id}", Static)
        tab.replication_container = self.app.query_one(f"#replication_container_{tab.id}", Container)
        tab.replication_variables = self.app.query_one(f"#replication_variables_{tab.id}", Label)
        tab.replication_status = self.app.query_one(f"#replication_status_{tab.id}", Static)
        tab.replication_thread_applier_container = self.app.query_one(
            f"#replication_thread_applier_container_{tab.id}", ScrollableContainer
        )
        tab.replication_thread_applier = self.app.query_one(f"#replication_thread_applier_{tab.id}", Static)

        # By default, hide all the panels
        tab.sparkline.display = False
        tab.main_container.display = False
        tab.loading_indicator.display = False

        for panel in tab.dolphie.panels.all():
            self.app.query_one(f"#panel_{panel}_{tab.id}").display = False

        # Set panels to be visible for the ones the user specifies
        for panel in dolphie.startup_panels:
            self.app.query_one(f"#panel_{panel}_{tab.id}").display = True
            setattr(getattr(dolphie.panels, panel), "visible", True)

        # Set what marker we use for graphs
        graphs = self.app.query(MetricManager.Graph)
        for graph in graphs:
            graph.marker = dolphie.graph_marker

        # Set default switches to be toggled on
        switches = self.app.query(f".switch_container_{tab_id} Switch")
        switches_to_toggle = [switch for switch in switches if switch.id not in ["Queries", "Threads_connected"]]
        for switch in switches_to_toggle:
            switch.toggle()

        if switch_tab:
            self.switch_tab(tab_id)

        # Set the sparkline data to 0
        tab.sparkline.data = [0]

        # Increment the tab id counter
        self.tab_id_counter += 1

        self.host_tabs.display = True

        return tab

    async def remove_tab(self, tab: Tab):
        await self.host_tabs.remove_pane(f"tab_{tab.id}")

    def rename_tab(self, tab: Tab, new_name: str = None):
        if not new_name and not tab.manual_tab_name:
            # mysql_host is the full host:port string, we want to split & truncate it to 24 characters
            host = tab.dolphie.mysql_host.split(":")[0][:24]
            if not host:
                return

            # If the last character isn't a letter or number, remove it
            if not host[-1].isalnum():
                host = host[:-1]

            new_name = f"{host}:[dark_gray]{tab.dolphie.port}"
        elif new_name:
            tab.manual_tab_name = new_name

        if new_name:
            tab.dolphie.tab_name = new_name
            tab.name = new_name
            self.host_tabs.get_tab(f"tab_{tab.id}").label = new_name

    def switch_tab(self, tab_id: int):
        tab = self.get_tab(tab_id)
        if not tab:
            return

        # Update the active/current tab
        self.active_tab = tab

        # Switch to the new tab in the UI
        self.host_tabs.active = f"tab_{tab.id}"

        self.update_topbar(tab=tab, connection_status=tab.dolphie.connection_status)

    def get_tab(self, id: int) -> Tab:
        if id in self.tabs:
            return self.tabs[id]

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

        tab.dolphie.replica_manager.remove_all()

        tab.main_container.display = False
        tab.sparkline.display = False
        tab.loading_indicator.display = False

        tab.sparkline.data = [0]

        tab.replicas_title.update("")
        for member in tab.dolphie.app.query(f".replica_container_{tab.id}"):
            await member.remove()

        if update_topbar:
            self.update_topbar(tab=tab, connection_status=ConnectionStatus.disconnected)

    def setup_host_tab(self, tab: Tab):
        dolphie = tab.dolphie

        async def command_get_input(data):
            # Set host_setup to false since it's only used when Dolphie first loads
            if self.config.host_setup:
                self.config.host_setup = False

            host_port = data["host"].split(":")

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
                await self.disconnect_tab(tab)

                tab.loading_indicator.display = True
                while True:
                    if not tab.worker_running and not tab.replicas_worker_running:
                        tab.worker_cancel_error = ""

                        dolphie.reset_runtime_variables()
                        dolphie.app.run_worker_main(tab.id)
                        dolphie.app.run_worker_replicas(tab.id)

                        break

                    await asyncio.sleep(0.25)

        # If we're here because of a worker cancel error or manually disconnected,
        # we want to pre-populate the host/port
        if (
            tab.worker_cancel_error
            or dolphie.connection_status == ConnectionStatus.disconnected
            or self.config.host_setup
        ):
            host = dolphie.host
            port = dolphie.port
        else:
            host = ""
            port = ""

        dolphie.app.push_screen(
            HostSetupModal(
                host=host,
                port=port,
                username=dolphie.user,
                password=dolphie.password,
                ssl=dolphie.ssl,
                socket_file=dolphie.socket,
                hostgroups=dolphie.hostgroup_hosts.keys(),
                available_hosts=dolphie.host_setup_available_hosts,
                error_message=tab.worker_cancel_error,
            ),
            command_get_input,
        )
