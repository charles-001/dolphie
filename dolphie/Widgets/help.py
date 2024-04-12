from dolphie.DataTypes import ConnectionSource
from dolphie.Widgets.topbar import TopBar
from rich import box
from rich.align import Align
from rich.console import Group
from rich.table import Table
from textual import events
from textual.app import ComposeResult
from textual.screen import Screen
from textual.widgets import Label, Static, TabbedContent, TabPane


class HelpScreen(Screen):
    CSS = """
        HelpScreen {
            background: #0a0e1b;
        }
        HelpScreen .tab {
            background: #101626;
            border: tall #1d253e;
        }
        HelpScreen TabbedContent {
            align: center middle;
        }
        HelpScreen ContentSwitcher {
            width: 60%;
        }
        HelpScreen #note {
            margin-top: 1;
            content-align: center middle;
            width: 100%;
        }
    """

    def __init__(self, connection_status, app_version, host, connection_source):
        super().__init__()

        self.connection_status = connection_status
        self.app_version = app_version
        self.host = host
        self.connection_source = connection_source

    def on_key(self, event: events.Key):
        if event.key == "q" and self.screen.is_attached:
            self.app.pop_screen()

    def on_mount(self):
        keys = {
            "1": "Show/hide Dashboard",
            "2": "Show/hide Processlist",
            "3": "Show/hide Replication/Replicas",
            "4": "Show/hide Graph Metrics",
            "5": "Show/hide Metadata Locks",
            "6": "Show/hide DDLs",
            "`": "Open Host Setup",
            "+": "Create a new tab",
            "-": "Remove the current tab",
            "=": "Rename the current tab",
            "a": "Toggle additional processlist columns",
            "c": "Clear all filters set",
            "d": "Display all databases",
            "D": "Disconnect from the tab's host",
            "e": "Display error log from Performance Schema",
            "f": "Filter processlist by a supported option",
            "i": "Toggle displaying idle processs",
            "k": "Kill a process by its ID",
            "K": "Kill a process by a supported option",
            "l": "Display the most recent deadlock",
            "o": "Display output from SHOW ENGINE INNODB STATUS",
            "m": "Display memory usage",
            "p": "Pause refreshing of panels",
            "P": "Switch between using Information Schema/Performance Schema for processlist panel",
            "q": "Quit",
            "r": "Set the refresh interval",
            "R": "Reset all metrics",
            "t": "Display details of a process along with an EXPLAIN of its query",
            "T": "Transaction view - toggle displaying processs that only have an active transaction",
            "s": "Toggle sorting for Age in descending/ascending order",
            "u": "List active connected users and their statistics",
            "v": "Variable wildcard search sourced from SHOW GLOBAL VARIABLES",
            "z": "Display all entries in the host cache",
            "space": "Force a manual refresh of all panels except replicas",
            "ctrl+a": "Switch to the previous tab",
            "ctrl+d": "Switch to the next tab",
        }

        table_keys = Table(
            box=box.SIMPLE_HEAVY,
            show_edge=False,
            style="table_border",
            title="Commands",
            title_style="bold #bbc8e8",
            header_style="bold",
        )
        table_keys.add_column("Key", justify="center", style="b highlight")
        table_keys.add_column("Description")

        for key, description in keys.items():
            table_keys.add_row(key, description)

        datapoints = {
            "Read Only": "If the host is in read-only mode",
            "Read Hit": "The percentage of how many reads are from InnoDB buffer pool compared to from disk",
            "Lag": ("Retrieves metric from: Default -> SHOW SLAVE STATUS, HB -> Heartbeat table"),
            "Chkpt Age": (
                "This depicts how close InnoDB is before it starts to furiously flush dirty data\nto disk "
                "(Lower is better)"
            ),
            "AHI Hit": (
                "The percentage of how many lookups there are from Adapative Hash Index\ncompared to it not"
                " being used"
            ),
            "Diff": "This is the size difference of the binary log between each refresh interval",
            "Cache Hit": "The percentage of how many binary log lookups are from cache instead of from disk",
            "History List": "History list length (number of un-purged row changes in InnoDB's undo logs)",
            "QPS": "Queries per second from Com_queries in SHOW GLOBAL STATUS",
            "Latency": "How much time it takes to receive data from the host for each refresh interval",
            "processs": "Con = Connected, Run = Running, Cac = Cached from SHOW GLOBAL STATUS",
            "Speed": "How many seconds were taken off of replication lag from the last refresh interval",
            "Tickets": "Relates to innodb_concurrency_tickets variable",
            "R-Lock/Mod": "Relates to how many rows are locked/modified for the process's transaction",
            "GR": "Group Replication",
        }

        table_terminology = Table(
            box=box.SIMPLE_HEAVY,
            show_edge=False,
            style="table_border",
            title="Terminology",
            title_style="bold #bbc8e8",
            header_style="bold",
        )
        table_terminology.add_column("Datapoint", style="b highlight")
        table_terminology.add_column("Description")
        for datapoint, description in sorted(datapoints.items()):
            table_terminology.add_row(datapoint, description)

        self.query_one("#mysql_help").update(Group(Align.center(table_keys), "", Align.center(table_terminology)))

        keys = {
            "1": "Show/hide Dashboard",
            "2": "Show/hide Processlist",
            "3": "Show/hide Hostgroup Summary",
            "4": "Show/hide Graph Metrics",
            "5": "Show/hide MySQL Query Rules",
            "6": "Show/hide Command Statistics",
            "`": "Open Host Setup",
            "+": "Create a new tab",
            "-": "Remove the current tab",
            "=": "Rename the current tab",
            "a": "Toggle additional processlist columns",
            "c": "Clear all filters set",
            "D": "Disconnect from the tab's host",
            "f": "Filter processlist by a supported option",
            "i": "Toggle displaying idle processs",
            "k": "Kill a process by its ID",
            "K": "Kill a process by a supported option",
            "p": "Pause refreshing of panels",
            "q": "Quit",
            "r": "Set the refresh interval",
            "R": "Reset all metrics",
            "t": "Display details of a process",
            "s": "Toggle sorting for Age in descending/ascending order",
            "u": "List frontend users connected",
            "v": "Variable wildcard search sourced from SHOW GLOBAL VARIABLES",
            "z": "Display all entries in the host cache",
            "space": "Force a manual refresh of all panels except replicas",
            "ctrl+a": "Switch to the previous tab",
            "ctrl+d": "Switch to the next tab",
        }

        table_keys = Table(
            box=box.SIMPLE_HEAVY,
            show_edge=False,
            style="table_border",
            title="Commands",
            title_style="bold #bbc8e8",
            header_style="bold",
        )
        table_keys.add_column("Key", justify="center", style="b highlight")
        table_keys.add_column("Description")

        for key, description in keys.items():
            table_keys.add_row(key, description)

        datapoints = {
            "FE": "Frontend",
            "BE": "Backend",
            "Conn": "Connection",
            "CP": "Connection Pool",
        }

        table_terminology = Table(
            box=box.SIMPLE_HEAVY,
            show_edge=False,
            style="table_border",
            title="Terminology",
            title_style="bold #bbc8e8",
            header_style="bold",
        )
        table_terminology.add_column("Datapoint", style="b highlight")
        table_terminology.add_column("Description")
        for datapoint, description in sorted(datapoints.items()):
            table_terminology.add_row(datapoint, description)

        self.query_one("#proxysql_help").update(Group(Align.center(table_keys), "", Align.center(table_terminology)))

        if self.connection_source == ConnectionSource.proxysql:
            self.query_one("#tabbed_content", TabbedContent).active = "tab_proxysql"

    def compose(self) -> ComposeResult:
        yield TopBar(connection_status=self.connection_status, app_version=self.app_version, host=self.host)
        with TabbedContent(id="tabbed_content"):
            yield TabPane("MySQL", Static(id="mysql_help", shrink=True), id="tab_mysql", classes="tab")
            yield TabPane("ProxySQL", Static(id="proxysql_help", shrink=True), id="tab_proxysql", classes="tab")
        yield Label(
            "[light_blue][b]Note[/b]: Textual puts your terminal in application mode which disables selecting"
            " text.\nTo see how to select text on your terminal, visit: https://tinyurl.com/dolphie-copy-text",
            id="note",
        )
