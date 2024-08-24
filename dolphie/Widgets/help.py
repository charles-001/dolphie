from dolphie.DataTypes import ConnectionSource
from dolphie.Modules.CommandManager import CommandManager
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

    def __init__(self, connection_source: ConnectionSource, command_manager: CommandManager):
        super().__init__()

        self.connection_source = connection_source
        self.command_manager = command_manager

    def on_mount(self):
        # Create and update tables for each connection source
        for source, content in self.command_manager.command_keys.items():
            commands = self._create_table("Commands", content["Commands"])
            terminology = self._create_table("Terminology", content.get("Terminology", {}))

            if source == ConnectionSource.mysql:
                self.query_one("#mysql_help").update(Align.center(commands))
            elif source == ConnectionSource.proxysql:
                self.query_one("#proxysql_help").update(Group(Align.center(commands), "", Align.center(terminology)))
            elif source == "mysql_replay":
                self.query_one("#mysql_replay_help").update(Align.center(commands))
            elif source == "proxysql_replay":
                self.query_one("#proxysql_replay_help").update(Align.center(commands))

        # Activate appropriate tab based on connection source
        tabbed_content = self.query_one("#tabbed_content", TabbedContent)
        if self.command_manager.replay_file:
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
            for key, key_data in sorted(data.items()):
                table.add_row(key, key_data["description"])
        else:
            table.add_column("Key", justify="center", style="b highlight")
            table.add_column("Description")
            for key_data in data.values():
                table.add_row(key_data["human_key"], key_data["description"])
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
