import copy
from dataclasses import dataclass

import dolphie.Modules.MetricManager as MetricManager
from dolphie import Dolphie
from dolphie.Widgets.quick_switch import QuickSwitchHostModal
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
    Rule,
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

    main_container: VerticalScroll = None
    loading_indicator: LoadingIndicator = None
    sparkline: Sparkline = None
    panel_dashboard: Container = None
    panel_graphs: Container = None
    panel_replication: Container = None
    panel_locks: DataTable = None
    panel_processlist: DataTable = None

    dashboard_host_information: Static = None
    dashboard_innodb: Static = None
    dashboard_binary_log: Static = None
    dashboard_statistics: Static = None
    dashboard_replication: Static = None

    replication_container: Container = None
    replication_variables: Label = None
    replication_status: Static = None
    replication_thread_applier_container: ScrollableContainer = None
    replication_thread_applier: Static = None

    group_replication_container: Container = None
    replicas_container: Container = None

    replicas_loading_indicator: LoadingIndicator = None
    replicas_title: Label = None
    group_replication_title: Label = None
    group_replication_data: Static = None
    cluster_data: Static = None

    topbar_data: str = "Connecting to MySQL"

    queue_for_removal: bool = False

    def update_topbar(self):
        dolphie = self.dolphie

        if not dolphie.read_only_status:
            if not self.loading_indicator.display:
                self.topbar_data = ""
            return

        if dolphie.main_db_connection and not dolphie.main_db_connection.connection.open:
            dolphie.read_only_status = "DISCONNECTED"

        self.topbar_data = f"[[white]{dolphie.read_only_status}[/white]] {dolphie.mysql_host}"

    def quick_switch_connection(self):
        dolphie = self.dolphie

        def command_get_input(data):
            host_port = data["host"].split(":")

            dolphie.host = host_port[0]
            dolphie.port = int(host_port[1]) if len(host_port) > 1 else 3306

            password = data.get("password")
            if password:
                dolphie.password = password

            # Trigger a quick switch connection for the worker thread
            dolphie.quick_switched_connection = True

            self.loading_indicator.display = True
            self.main_container.display = False
            self.sparkline.display = False
            self.topbar_data = "Connecting to MySQL"

            if not self.worker or self.worker.state == WorkerState.CANCELLED:
                self.worker_cancel_error = ""
                self.dolphie.app.worker_fetch_data(self.id)

        self.loading_indicator.display = False

        # If we're here because of a worker cancel error, we want to pre-populate the host/port
        if self.worker_cancel_error:
            host = dolphie.host
            port = dolphie.port
        else:
            host = ""
            port = ""

        self.dolphie.app.push_screen(
            QuickSwitchHostModal(
                host=host,
                port=port,
                quick_switch_hosts=dolphie.quick_switch_hosts,
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
                        Container(
                            Rule(line_style="heavy"),
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
                            Rule(line_style="heavy"),
                            id=f"replication_container_{tab_id}",
                            classes="replication",
                        ),
                        Container(
                            Rule(line_style="heavy"),
                            Label(id=f"group_replication_title_{tab_id}"),
                            Label(id=f"group_replication_data_{tab_id}"),
                            Rule(line_style="heavy"),
                            id=f"group_replication_container_{tab_id}",
                            classes="group_replication",
                        ),
                        Static(id=f"cluster_data_{tab_id}"),
                        Container(
                            Rule(line_style="heavy"),
                            Label(id=f"replicas_title_{tab_id}"),
                            LoadingIndicator(id=f"replicas_loading_indicator_{tab_id}"),
                            Rule(line_style="heavy"),
                            id=f"replicas_container_{tab_id}",
                            classes="replicas",
                        ),
                        id=f"panel_replication_{tab_id}",
                        classes="panel_container replication_panel",
                    ),
                    DataTable(id=f"panel_locks_{tab_id}", classes="panel_container pad_top_1", show_cursor=False),
                    DataTable(id=f"panel_processlist_{tab_id}", classes="panel_container pad_top_1", show_cursor=False),
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
        tab.main_container = self.app.query_one(f"#main_container_{tab.id}", VerticalScroll)
        tab.loading_indicator = self.app.query_one(f"#loading_indicator_{tab.id}", LoadingIndicator)
        tab.sparkline = self.app.query_one(f"#panel_dashboard_queries_qps_{tab.id}", Sparkline)
        tab.panel_dashboard = self.app.query_one(f"#panel_dashboard_{tab.id}", Container)
        tab.panel_graphs = self.app.query_one(f"#panel_graphs_{tab.id}", Container)
        tab.panel_replication = self.app.query_one(f"#panel_replication_{tab.id}", Container)
        tab.panel_locks = self.app.query_one(f"#panel_locks_{tab.id}", DataTable)
        tab.panel_processlist = self.app.query_one(f"#panel_processlist_{tab.id}", DataTable)

        tab.dashboard_host_information = self.app.query_one(f"#dashboard_host_information_{tab.id}", Static)
        tab.dashboard_innodb = self.app.query_one(f"#dashboard_innodb_{tab.id}", Static)
        tab.dashboard_binary_log = self.app.query_one(f"#dashboard_binary_log_{tab.id}", Static)
        tab.dashboard_statistics = self.app.query_one(f"#dashboard_statistics_{tab.id}", Static)
        tab.dashboard_replication = self.app.query_one(f"#dashboard_replication_{tab.id}", Static)

        tab.group_replication_container = self.app.query_one(f"#group_replication_container_{tab.id}", Container)
        tab.replicas_container = self.app.query_one(f"#replicas_container_{tab.id}", Container)

        tab.group_replication_data = self.app.query_one(f"#group_replication_data_{tab.id}", Static)
        tab.group_replication_title = self.app.query_one(f"#group_replication_title_{tab.id}", Label)
        tab.replicas_title = self.app.query_one(f"#replicas_title_{tab.id}", Label)
        tab.replicas_loading_indicator = self.app.query_one(f"#replicas_loading_indicator_{tab.id}", LoadingIndicator)

        tab.cluster_data = self.app.query_one(f"#cluster_data_{tab.id}", Static)

        tab.replication_container = self.app.query_one(f"#replication_container_{tab.id}", Container)
        tab.replication_variables = self.app.query_one(f"#replication_variables_{tab.id}", Label)
        tab.replication_status = self.app.query_one(f"#replication_status_{tab.id}", Static)
        tab.replication_thread_applier_container = self.app.query_one(
            f"#replication_thread_applier_container_{tab.id}", ScrollableContainer
        )
        tab.replication_thread_applier = self.app.query_one(f"#replication_thread_applier_{tab.id}", Static)

        # By default, hide all the panels
        tab.panel_dashboard.display = False
        tab.panel_graphs.display = False
        tab.panel_replication.display = False
        tab.panel_locks.display = False
        tab.panel_processlist.display = False
        tab.sparkline.display = False

        # Set panels to be visible for the ones the user specifies
        for panel in dolphie.startup_panels:
            setattr(dolphie, f"display_{panel}_panel", True)

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

        # Update the topbar
        self.app.topbar.host = tab.topbar_data

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
