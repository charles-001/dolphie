from __future__ import annotations

from rich.console import RenderableType
from rich.style import Style
from rich.syntax import Syntax
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Center, Container
from textual.widgets import Label, Rule, Static, TextArea
from textual.widgets.text_area import TextAreaTheme

from dolphie.Widgets.DolphieScreen import DolphieScreen, ScreenContext


class ProxySQLThreadScreen(DolphieScreen):
    AUTO_FOCUS = ""

    CSS = """
        ProxySQLThreadScreen {
            background: $background;

            & Container {
                height: auto;
            }

            & #thread_container {
                margin-top: 1;
                height: auto;
            }

            & .title {
                width: 100%;
                content-align: center middle;
                color: $light_blue;
                text-style: bold;
            }

            & .table {
                content-align: center middle;
                background: #101626;
                border: tall #1d253e;
                padding-left: 1;
                padding-right: 1;
                height: auto;
                width: auto;
            }

            & TextArea {
                border: tall #1d253e;
                width: 100;
                height: 35;
            }
        }
    """

    BINDINGS = [
        Binding("c", "copy_query", "Copy Query"),
        Binding("j", "copy_json", "Copy JSON"),
    ]

    def __init__(
        self,
        context: ScreenContext,
        thread_table: RenderableType,
        query: Syntax | None,
        extended_info: str | None,
    ):
        super().__init__(context)

        self.thread_table = thread_table
        self.formatted_query = query
        self.extended_info = extended_info

        dracula = TextAreaTheme.get_builtin_theme("dracula")
        if dracula is None:
            raise RuntimeError("Textual's built-in dracula theme is unavailable")
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

        self.extended_info_text_area = TextArea(theme="dracula", show_line_numbers=True, read_only=True)
        if self.extended_info:
            self.extended_info_text_area.text = self.extended_info

    def on_mount(self):
        self.query_one("#thread_table", Static).update(self.thread_table)

        if self.formatted_query:
            self.query_one("#query", Static).update(self.formatted_query)
        else:
            self.query_one("#query_container", Container).display = False

        if not self.extended_info:
            self.query_one("#extended_info_container", Container).display = False

    def compose_content(self) -> ComposeResult:
        """Compose thread details, query text, and extended JSON."""
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

    def check_action(self, action: str, parameters: tuple[object, ...]) -> bool | None:
        """Hide copy commands when the corresponding content is unavailable."""
        if action == "copy_query":
            return bool(self.formatted_query)
        if action == "copy_json":
            return bool(self.extended_info)
        return True

    def action_copy_query(self) -> None:
        """Copy the displayed query."""
        if self.formatted_query is not None:
            self._copy_to_clipboard(self.formatted_query.code, "query")

    def action_copy_json(self) -> None:
        """Copy the displayed extended information."""
        if self.extended_info is not None:
            self._copy_to_clipboard(self.extended_info, "JSON data")

    def _copy_to_clipboard(self, text: str, content_type: str) -> None:
        try:
            self.app.copy_to_clipboard(text)
        except Exception as error:
            self.notify(f"Failed to copy {content_type} to clipboard: {error}", severity="error")
            return
        self.notify(f"Copied {content_type} to clipboard!", severity="information")
