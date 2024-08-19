from dolphie.DataTypes import ConnectionSource
from rich import box
from rich.align import Align
from rich.console import Group
from rich.table import Table
from textual import on
from textual.app import ComposeResult
from textual.containers import Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Label, Static, TabbedContent, TabPane


class HelpScreen(ModalScreen):
    CSS = """
        HelpScreen > Vertical {
            background: #131626;
            border: tall #384673;
            height: auto;
            width: 110;
        }
        HelpScreen .tab {
            align: center middle;
        }
        HelpScreen #note {
            margin-top: 1;
            content-align: center middle;
            width: 100%;
        }
        HelpScreen Button {
            margin-top: 1;
        }
    """

    BINDINGS = [("escape", "app.pop_screen"), ("q", "app.pop_screen")]

    def __init__(self, connection_source, replay_file):
        super().__init__()
        self.connection_source = connection_source
        self.replay_file = replay_file

    def on_mount(self):
        command_data = {
            ConnectionSource.mysql: {
                "Commands": {
                    "1": "Show/hide Dashboard",
                    "2": "Show/hide Processlist",
                    "3": "Show/hide Graph Metrics",
                    "4": "Show/hide Replication/Replicas",
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
                    "E": "Export the processlist to a CSV file",
                    "f": "Filter processlist by a supported option",
                    "i": "Toggle displaying idle process",
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
                    "T": "Toggle displaying processes that only have an active transaction",
                    "s": "Toggle sorting for Age in descending/ascending order",
                    "u": "List active connected users and their statistics",
                    "v": "Variable wildcard search sourced from SHOW GLOBAL VARIABLES",
                    "z": "Display all entries in the host cache",
                    "space": "Force a manual refresh of all panels except replicas",
                    "ctrl+a": "Switch to the previous tab",
                    "ctrl+d": "Switch to the next tab",
                },
                "Terminology": {
                    "Read Only": "If the host is in read-only mode",
                    "Read Hit": "The percentage of how many reads are from InnoDB buffer pool compared to from disk",
                    "Chkpt Age": (
                        "This depicts how close InnoDB is before it starts to furiously flush dirty data to disk "
                        "(Lower is better)"
                    ),
                    "AHI Hit": (
                        "The percentage of how many lookups there are from Adaptive Hash Index compared "
                        "to it not being used"
                    ),
                    "Cache Hit": "The percentage of how many binary log lookups are from cache instead of from disk",
                    "History List": "History list length (number of un-purged row changes in InnoDB's undo logs)",
                    "QPS": "Queries per second from Com_queries in SHOW GLOBAL STATUS",
                    "Latency": "How much time it takes to receive data from the host for each refresh interval",
                    "Threads": "Con = Connected, Run = Running, Cac = Cached from SHOW GLOBAL STATUS",
                    "Speed": "How fast replication is catching up in seconds after each refresh interval",
                    "Tickets": "Relates to innodb_concurrency_tickets variable",
                    "R-Lock/Mod": "Relates to how many rows are locked/modified for the process's transaction",
                    "GR": "Group Replication",
                },
            },
            ConnectionSource.proxysql: {
                "Commands": {
                    "1": "Show/hide Dashboard",
                    "2": "Show/hide Processlist",
                    "3": "Show/hide Graph Metrics",
                    "4": "Show/hide Hostgroup Summary",
                    "5": "Show/hide MySQL Query Rules",
                    "6": "Show/hide Command Statistics",
                    "`": "Open Host Setup",
                    "+": "Create a new tab",
                    "-": "Remove the current tab",
                    "=": "Rename the current tab",
                    "a": "Toggle additional processlist/query rule columns",
                    "c": "Clear all filters set",
                    "D": "Disconnect from the tab's host",
                    "e": "Display errors reported by backend servers during query execution",
                    "E": "Export the processlist to a CSV file",
                    "f": "Filter processlist by a supported option",
                    "i": "Toggle displaying idle process",
                    "k": "Kill a process by its ID",
                    "K": "Kill a process by a supported option",
                    "m": "Display memory usage",
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
                },
                "Terminology": {
                    "FE": "Frontend",
                    "BE": "Backend",
                    "Conn": "Connection",
                    "CP": "Connection Pool",
                    "MP": "Multiplex",
                },
            },
            "mysql_replay": {
                "Commands": {
                    "1": "Show/hide Dashboard",
                    "2": "Show/hide Processlist",
                    "3": "Show/hide Graph Metrics",
                    "5": "Show/hide Metadata Locks",
                    "`": "Open Host Setup",
                    "+": "Create a new tab",
                    "-": "Remove the current tab",
                    "=": "Rename the current tab",
                    "a": "Toggle additional processlist columns",
                    "c": "Clear all filters set",
                    "E": "Export the processlist to a CSV file",
                    "f": "Filter processlist by a supported option",
                    "p": "Pause replay",
                    "q": "Quit",
                    "r": "Set the refresh interval",
                    "t": "Display details of a process",
                    "T": "Toggle displaying processes that only have an active transaction",
                    "s": "Toggle sorting for Age in descending/ascending order",
                    "S": "Seek to a specific time in the replay",
                    "v": "Variable wildcard search sourced from SHOW GLOBAL VARIABLES",
                    "[": "Seek to the previous refresh interval",
                    "]": "Seek to the next refresh interval",
                    "ctrl+a": "Switch to the previous tab",
                    "ctrl+d": "Switch to the next tab",
                }
            },
            "proxysql_replay": {
                "Commands": {
                    "1": "Show/hide Dashboard",
                    "2": "Show/hide Processlist",
                    "3": "Show/hide Graph Metrics",
                    "4": "Show/hide Hostgroup Summary",
                    "`": "Open Host Setup",
                    "+": "Create a new tab",
                    "-": "Remove the current tab",
                    "=": "Rename the current tab",
                    "a": "Toggle additional processlist columns",
                    "c": "Clear all filters set",
                    "E": "Export the processlist to a CSV file",
                    "f": "Filter processlist by a supported option",
                    "p": "Pause replay",
                    "q": "Quit",
                    "r": "Set the refresh interval",
                    "t": "Display details of a process",
                    "s": "Toggle sorting for Age in descending/ascending order",
                    "S": "Seek to a specific time in the replay",
                    "v": "Variable wildcard search sourced from SHOW GLOBAL VARIABLES",
                    "[": "Seek to the previous refresh interval",
                    "]": "Seek to the next refresh interval",
                    "ctrl+a": "Switch to the previous tab",
                    "ctrl+d": "Switch to the next tab",
                }
            },
        }

        # Create and update tables for each connection source
        for source, content in command_data.items():
            commands = self._create_table("Commands", content["Commands"])
            terminology = self._create_table("Terminology", content.get("Terminology", {}))

            if source == ConnectionSource.mysql:
                self.query_one("#mysql_help").update(Group(Align.center(commands), "", Align.center(terminology)))
            elif source == ConnectionSource.proxysql:
                self.query_one("#proxysql_help").update(Group(Align.center(commands), "", Align.center(terminology)))
            elif source == "mysql_replay":
                self.query_one("#mysql_replay_help").update(Group(Align.center(commands)))
            elif source == "proxysql_replay":
                self.query_one("#proxysql_replay_help").update(Group(Align.center(commands)))

        # Activate appropriate tab based on connection source
        tabbed_content = self.query_one("#tabbed_content", TabbedContent)
        if self.replay_file:
            if self.connection_source == ConnectionSource.proxysql:
                tabbed_content.active = "tab_proxysql_replay"
            else:
                tabbed_content.active = "tab_mysql_replay"
        else:
            if self.connection_source == ConnectionSource.proxysql:
                tabbed_content.active = "tab_proxysql"

    def _create_table(self, title: str, data: dict):
        table = Table(
            box=box.SIMPLE_HEAVY,
            show_edge=False,
            style="table_border",
            title=title,
            title_style="bold #bbc8e8",
            header_style="bold",
        )
        if title == "Terminology":
            table.add_column("Datapoint", style="b highlight")
            table.add_column("Description")
            for key, description in sorted(data.items()):
                table.add_row(key, description)
        else:
            table.add_column("Key", justify="center", style="b highlight")
            table.add_column("Description")
            for key, description in data.items():
                table.add_row(key, description)
        return table

    def compose(self) -> ComposeResult:
        with Vertical():
            with TabbedContent(id="tabbed_content"):
                yield TabPane("MySQL", Static(id="mysql_help", shrink=True), id="tab_mysql", classes="tab")
                yield TabPane(
                    "MySQL Replay", Static(id="mysql_replay_help", shrink=True), id="tab_mysql_replay", classes="tab"
                )
                yield TabPane("ProxySQL", Static(id="proxysql_help", shrink=True), id="tab_proxysql", classes="tab")
                yield TabPane(
                    "ProxySQL Replay",
                    Static(id="proxysql_replay_help", shrink=True),
                    id="tab_proxysql_replay",
                    classes="tab",
                )

            yield Label(
                "[light_blue][b]Note[/b]: Textual puts your terminal in application mode which disables selecting"
                " text.\nTo see how to select text on your terminal, visit: https://tinyurl.com/dolphie-copy-text",
                id="note",
            )
            with Vertical(classes="button_container"):
                yield Button("Close")

    @on(Button.Pressed)
    def on_cancel_pressed(self, event: Button.Pressed) -> None:
        self.app.pop_screen()
