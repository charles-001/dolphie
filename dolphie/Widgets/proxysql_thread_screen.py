from rich.style import Style
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Center, Container
from textual.screen import Screen
from textual.widgets import Label, Rule, Static, TextArea
from textual.widgets.text_area import TextAreaTheme

from dolphie.Widgets.topbar import TopBar


class ProxySQLThreadScreen(Screen):
    AUTO_FOCUS = ""

    CSS = """
        ProxySQLThreadScreen {
            background: #0a0e1b;
        }
        ProxySQLThreadScreen Container {
            height: auto;
        }
        ProxySQLThreadScreen #thread_container {
            margin-top: 1;
            height: auto;
        }
        ProxySQLThreadScreen .title {
            width: 100%;
            content-align: center middle;
            color: #bbc8e8;
            text-style: bold;
        }
        ProxySQLThreadScreen .table {
            content-align: center middle;
            background: #101626;
            border: tall #1d253e;
            padding-left: 1;
            padding-right: 1;
            height: auto;
            width: auto;
        }
        ProxySQLThreadScreen TextArea {
            border: tall #1d253e;
            width: 100;
            height: 35;
        }
    """

    BINDINGS = [
        Binding("q", "app.pop_screen", "", show=False),
    ]

    def __init__(
        self,
        connection_status: str,
        app_version: str,
        host: str,
        thread_table: str,
        query: str,
        extended_info: str,
    ):
        super().__init__()

        self.connection_status = connection_status
        self.app_version = app_version
        self.host = host

        self.thread_table = thread_table
        self.formatted_query = query
        self.extended_info = extended_info

        dracula = TextAreaTheme.get_builtin_theme("dracula")
        dracula.base_style = Style(bgcolor="#101626")
        dracula.gutter_style = Style(color="#606e88")
        dracula.cursor_line_gutter_style = Style(color="#95a7c7", bgcolor="#20243b")
        dracula.cursor_line_style = Style(bgcolor="#20243b")
        dracula.selection_style = Style(bgcolor="#293c71")
        dracula.cursor_style = Style(bgcolor="#7a8ab2", color="#121e3a")
        dracula.syntax_styles = {
            "json.label": Style(color="#879bca", bold=True),
            "number": Style(color="#ca87a5"),
        }

        self.extended_info_text_area = TextArea(
            language="json", theme="dracula", show_line_numbers=True, read_only=True
        )
        if self.extended_info:
            self.extended_info_text_area.text = extended_info

    def on_mount(self):
        self.query_one("#thread_table").update(self.thread_table)

        if self.formatted_query:
            self.query_one("#query").update(self.formatted_query)
        else:
            self.query_one("#query_container").display = False

        if not self.extended_info:
            self.query_one("#extended_info_container").display = False

    def compose(self) -> ComposeResult:
        yield TopBar(connection_status=self.connection_status, app_version=self.app_version, host=self.host)

        with Container(id="thread_container"):
            yield Label("Thread Details", classes="title")
            yield Center(Static(id="thread_table", shrink=True, classes="table"))

        with Container(id="query_container"):
            yield Rule(line_style="heavy")
            yield Label("Query Details", classes="title")
            yield Center(Static(id="query", shrink=True, classes="table"))

        with Container(id="extended_info_container"):
            yield Rule(line_style="heavy")
            yield Label("Extended Information", classes="title")
            yield Center(self.extended_info_text_area)
