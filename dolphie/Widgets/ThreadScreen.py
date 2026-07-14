from __future__ import annotations

from collections.abc import Mapping, Sequence

from rich.console import RenderableType
from rich.style import Style
from rich.syntax import Syntax
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Center, Container, ScrollableContainer
from textual.widgets import (
    Label,
    Rule,
    Static,
    TabbedContent,
    TabPane,
    TextArea,
)
from textual.widgets.text_area import TextAreaTheme

from dolphie.DataTypes import DatabaseScalar
from dolphie.Modules.Functions import coerce_float, format_number
from dolphie.Modules.Theme import ThemedDataTable as DataTable
from dolphie.Widgets.DolphieScreen import DolphieScreen, ScreenContext


class ThreadScreen(DolphieScreen):
    CSS = """
        ThreadScreen {
            background: $background;

            & #explain_table {
                margin-top: 1;
                background: #101626;
                border: tall #1d253e;
                overflow-x: auto;
                min-height: 5;
                max-height: 15;
                width: 100%;
            }

            & #explain_failure {
                margin-top: 1;
                max-width: 120;
            }

            & Container {
                height: auto;
            }

            & #thread_container {
                margin-top: 1;
                height: auto;
                layout: horizontal;
            }

            & .title {
                width: 100%;
                content-align: center middle;
                color: $light_blue;
                text-style: bold;
            }

            & Center {
                height: auto;
            }

            & #query {
                width: auto;
            }

            & .container > Center {
                layout: horizontal;
            }

            & #thread_container ScrollableContainer {
                height: auto;
                width: 50vw;
                max-height: 15;
            }

            & .table {
                content-align: center middle;
                background: $surface;
                border: tall #1d253e;
                padding-left: 1;
                padding-right: 1;
                height: auto;
            }

            & TextArea {
                border: tall #1d253e;
                max-height: 25;
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
        user_thread_attributes_table: RenderableType | None,
        query: Syntax | None,
        explain_data: Sequence[Mapping[str, DatabaseScalar]] | None,
        explain_json_data: str | None,
        explain_failure: str | None,
        transaction_history_table: RenderableType | None,
    ):
        super().__init__(context)

        self.thread_table = thread_table
        self.user_thread_attributes_table = user_thread_attributes_table
        self.formatted_query = query
        self.explain_data = explain_data
        self.explain_json_data = explain_json_data
        self.explain_failure = explain_failure
        self.transaction_history_table = transaction_history_table

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

        self.explain_json_text_area = TextArea(theme="dracula", show_line_numbers=True, read_only=True)

    def _copy_to_clipboard(self, text: str, content_type: str = "content"):
        """Copy text to clipboard and show notification."""
        try:
            self.app.copy_to_clipboard(text)
            self.notify(f"Copied {content_type} to clipboard!", severity="information")
        except Exception as e:
            self.notify(f"Failed to copy {content_type} to clipboard: {e}", severity="error")

    def action_copy_query(self) -> None:
        """Action to copy the query via keyboard shortcut."""
        if self.formatted_query is not None:
            self._copy_to_clipboard(self.formatted_query.code, "query")

    def action_copy_json(self) -> None:
        """Copy the JSON explain plan via keyboard shortcut."""
        if self.explain_json_data is not None:
            self._copy_to_clipboard(self.explain_json_data, "JSON data")

    def check_action(self, action: str, parameters: tuple[object, ...]) -> bool | None:
        """Hide copy commands when the corresponding content is unavailable."""
        if action == "copy_query":
            return bool(self.formatted_query)
        if action == "copy_json":
            return bool(self.explain_json_data)
        return True

    def on_mount(self):
        self.query_one("#thread_table", Static).update(self.thread_table)
        if self.formatted_query is not None:
            self.query_one("#query", Static).update(self.formatted_query)

        if self.transaction_history_table:
            self.query_one("#transaction_history_table", Static).update(self.transaction_history_table)
        else:
            self.query_one("#transaction_history_container", Container).display = False

        if self.user_thread_attributes_table:
            self.query_one("#user_thread_attributes_table", Static).update(self.user_thread_attributes_table)
        else:
            self.query_one("#user_thread_attributes_table", Static).display = False

        if self.formatted_query:
            if self.explain_failure:
                self.query_one("#explain_tabbed_content", TabbedContent).display = False
                self.query_one("#explain_failure", Label).update(self.explain_failure)
            elif self.explain_data:
                self.query_one("#explain_failure", Label).display = False

                explain_table = self.query_one("#explain_table", DataTable)

                columns = []
                for row in self.explain_data:
                    values = []
                    for column, value in row.items():
                        # Exclude possbile_keys field since it takes up too much space
                        if column == "possible_keys":
                            continue

                        # Don't duplicate columns
                        if column not in columns:
                            explain_table.add_column(f"[$label]{column}")
                            columns.append(column)

                        if column == "key" and value is None:
                            value = "[b $white on #B30000]NO INDEX[/]"

                        if column == "rows" and value is not None:
                            value = format_number(coerce_float(value))

                        values.append(str(value))

                    explain_table.add_row(*values)
            else:
                self.query_one("#explain_table", DataTable).display = False
                self.query_one("#explain_failure", Label).display = False
        else:
            self.query_one("#query_container", Container).display = False

        if self.explain_json_data:
            self.explain_json_text_area.text = self.explain_json_data
        else:
            self.query_one("#explain_tabbed_content", TabbedContent).display = False

    def compose_content(self) -> ComposeResult:
        """Compose thread context, query plans, and transaction history."""
        with Container(id="thread_container", classes="container"):
            with Container():
                yield Label("Thread Details", classes="title")
                yield ScrollableContainer(Static(id="thread_table"), classes="table")
            with Container():
                yield Label("Thread Attributes", classes="title")
                yield ScrollableContainer(Static(id="user_thread_attributes_table"), classes="table")

        with Container(id="query_container", classes="container"):
            yield Rule(line_style="heavy")

            yield Label("Query", classes="title")
            yield Center(Static(id="query", shrink=True, classes="table"))

            yield Center(Label("", id="explain_failure"))
            with TabbedContent(id="explain_tabbed_content", classes="container"):
                with TabPane("Table", id="table_explain_tab", classes="container"):
                    yield DataTable(show_cursor=False, id="explain_table", classes="table")

                with TabPane("JSON", id="json_explain_tab", classes="container"):
                    yield Center(self.explain_json_text_area)

        with Container(id="transaction_history_container", classes="container"):
            yield Rule(line_style="heavy")
            yield Label("Transaction History", id="transaction_history_label", classes="title")
            yield Center(
                Static(id="transaction_history_table", shrink=True, classes="table"),
                id="transaction_history_table_center",
            )
