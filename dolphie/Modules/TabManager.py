import copy
from dataclasses import dataclass

import dolphie.Modules.MetricManager as MetricManager
from dolphie import Dolphie
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
from textual.worker import Worker, WorkerState


@dataclass
class Tab:
    id: int
    name: str
    dolphie: Dolphie

    worker: Worker = None
    worker_timer: Timer = None
    worker_cancel_error: str = None

    replicas_worker: worker = None
    replicas_worker_timer: Timer = None

    topbar: TopBar = None
    main_container: VerticalScroll = None
    loading_indicator: LoadingIndicator = None
    sparkline: Sparkline = None
    panel_dashboard: Container = None
    panel_graphs: Container = None
    panel_replication: Container = None
    panel_locks: Container = None
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

    locks_title: Label = None
    locks_datatable: DataTable = None

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

    queue_for_removal: bool = False

    def disconnect(self, update_topbar: bool = True):
        if self.worker_timer:
            self.worker_timer.stop()
        if self.replicas_worker_timer:
            self.replicas_worker_timer.stop()

        if self.worker:
            self.worker.cancel()
        self.worker = None

        if self.replicas_worker:
            self.replicas_worker.cancel()
        self.replicas_worker = None

        if self.dolphie.main_db_connection:
            self.dolphie.main_db_connection.close()

        if self.dolphie.secondary_db_connection:
            self.dolphie.secondary_db_connection.close()

        self.dolphie.replica_manager.remove_all()

        self.main_container.display = False
        self.sparkline.display = False

        self.replicas_title.update("")
        for member in self.dolphie.app.query(f".replica_container_{self.id}"):
            member.remove()

        if update_topbar:
            self.update_topbar()

    def update_topbar(self, custom_text: str = None):
        dolphie = self.dolphie

        if custom_text:
            self.topbar.host = custom_text
            return

        if dolphie.main_db_connection and not dolphie.main_db_connection.is_connected():
            dolphie.read_only_status = "DISCONNECTED"
        else:
            if not self.worker:
                if not self.loading_indicator.display:
                    self.topbar.host = ""

                # If there is no worker instance, we don't update the topbar
                return

        if dolphie.read_only_status and dolphie.mysql_host:
            self.topbar.host = f"[[white]{dolphie.read_only_status}[/white]] {dolphie.mysql_host}"

    def host_setup(self):
        dolphie = self.dolphie

        def command_get_input(data):
            host_port = data["host"].split(":")

            dolphie.host = host_port[0]
            dolphie.port = int(host_port[1]) if len(host_port) > 1 else 3306

            password = data.get("password")
            if password:
                dolphie.password = password

            self.disconnect()
            dolphie.reset_runtime_variables()

            if not self.worker or self.worker.state == WorkerState.CANCELLED:
                self.worker_cancel_error = ""

                self.dolphie.app.run_worker_main(self.id)
                self.dolphie.app.run_worker_replicas(self.id)

        self.loading_indicator.display = False

        # If we're here because of a worker cancel error, we want to pre-populate the host/port
        if self.worker_cancel_error:
            host = dolphie.host
            port = dolphie.port

            self.update_topbar()
        else:
            host = ""
            port = ""

        self.dolphie.app.push_screen(
            HostSetupModal(
                host=host,
                port=port,
                available_hosts=dolphie.host_setup_available_hosts,
                error_message=self.worker_cancel_error,
            ),
            command_get_input,
        )


class TabManager:
    def __init__(self, app: App):
        self.app = app

        self.tabs: dict = {}
        self.tab_id_counter: int = 1

        self.tabbed_content = self.app.query_one("#tabbed_content", TabbedContent)

    async def create_tab(self, tab_name: str, dolphie: Dolphie):
        tab_id = self.tab_id_counter

        await self.tabbed_content.add_pane(
            TabPane(
                tab_name,
                LoadingIndicator(id=f"loading_indicator_{tab_id}"),
                SpinnerWidget(id=f"spinner_{tab_id}"),
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
                        TabbedContent(id=f"tabbed_content_{tab_id}", classes="metrics_tabbed_content"),
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
                    Container(
                        Label(id=f"locks_title_{tab_id}"),
                        DataTable(id=f"locks_datatable_{tab_id}", show_cursor=False),
                        id=f"panel_locks_{tab_id}",
                        classes="locks",
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

        metrics = dolphie.metric_manager.metrics
        metric_tab_labels = [
            ("DML", metrics.dml, True),
            ("Locks", metrics.locks, False),
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

            await self.app.query_one(f"#tabbed_content_{tab_id}", TabbedContent).add_pane(
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

            if metric_tab_name == "redo_log":
                graph_names = ["graph_redo_log", "graph_redo_log_active_count", "graph_redo_log_bar"]
            elif metric_tab_name == "adaptive_hash_index":
                graph_names = ["graph_adaptive_hash_index", "graph_adaptive_hash_index_hit_ratio"]

            for graph_name in graph_names:
                await self.app.query_one(f"#graph_container_{metric_tab_name}_{tab_id}", Horizontal).mount(
                    MetricManager.Graph(id=f"{graph_name}_{tab_id}", classes="panel_data")
                )

            if create_switches:
                for metric, metric_data in metric_instance.__dict__.items():
                    if isinstance(metric_data, MetricManager.MetricData) and metric_data.graphable:
                        await self.app.query_one(f"#switch_container_{metric_tab_name}_{tab_id}", Horizontal).mount(
                            Label(metric_data.label)
                        )
                        await self.app.query_one(f"#switch_container_{metric_tab_name}_{tab_id}", Horizontal).mount(
                            Switch(animate=False, id=metric, name=metric_tab_name)
                        )

        # Create a new tab instance
        dolphie = copy.copy(dolphie)
        dolphie.reset_runtime_variables()
        if tab_id != 1:
            dolphie.host = ""
            dolphie.port = ""

        tab = Tab(id=tab_id, name=tab_name, dolphie=dolphie)

        dolphie.tab_id = tab_id
        dolphie.tab_name = tab_name

        # Save references to the widgets in the tab
        tab.topbar = self.app.query_one(TopBar)
        tab.main_container = self.app.query_one(f"#main_container_{tab.id}", VerticalScroll)
        tab.loading_indicator = self.app.query_one(f"#loading_indicator_{tab.id}", LoadingIndicator)
        tab.sparkline = self.app.query_one(f"#panel_dashboard_queries_qps_{tab.id}", Sparkline)
        tab.panel_dashboard = self.app.query_one(f"#panel_dashboard_{tab.id}", Container)
        tab.panel_graphs = self.app.query_one(f"#panel_graphs_{tab.id}", Container)
        tab.panel_replication = self.app.query_one(f"#panel_replication_{tab.id}", Container)
        tab.panel_locks = self.app.query_one(f"#panel_locks_{tab.id}", Container)
        tab.panel_processlist = self.app.query_one(f"#panel_processlist_{tab.id}", Container)
        tab.panel_ddl = self.app.query_one(f"#panel_ddl_{tab.id}", Container)

        tab.spinner = self.app.query_one(f"#spinner_{tab.id}", SpinnerWidget)
        tab.spinner.hide()

        tab.ddl_title = self.app.query_one(f"#ddl_title_{tab.id}", Label)
        tab.ddl_datatable = self.app.query_one(f"#ddl_datatable_{tab.id}", DataTable)
        tab.processlist_title = self.app.query_one(f"#processlist_title_{tab.id}", Label)
        tab.processlist_datatable = self.app.query_one(f"#processlist_data_{tab.id}", DataTable)
        tab.locks_title = self.app.query_one(f"#locks_title_{tab.id}", Label)
        tab.locks_datatable = self.app.query_one(f"#locks_datatable_{tab.id}", DataTable)

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
        for panel in tab.dolphie.panels.all():
            self.app.query_one(f"#panel_{panel}_{tab.id}").display = False

        # Set panels to be visible for the ones the user specifies
        for panel in dolphie.startup_panels:
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

        # Save the tab instance to the tab manager
        self.tabs[tab_id] = tab

        # Switch to the new tab
        self.switch_tab(tab_id)

        # Increment the tab id counter
        self.tab_id_counter += 1

    async def remove_tab(self, tab_id: int):
        await self.tabbed_content.remove_pane(f"tab_{self.get_tab(tab_id).id}")

        tab = self.get_tab(tab_id)
        tab.queue_for_removal = True

    def switch_tab(self, tab_id: int):
        tab = self.get_tab(tab_id)

        self.app.tab = tab  # Update the current tab variable for the app

        self.tabbed_content.active = f"tab_{tab.id}"

        tab.update_topbar()

    def get_tab(self, id: int) -> Tab:
        if id in self.tabs:
            return self.tabs[id]

    def get_all_tabs(self) -> list:
        all_tabs = []

        for tab in self.tabs.values():
            tab: Tab
            if not tab.queue_for_removal:
                all_tabs.append(tab.id)

        return all_tabs
