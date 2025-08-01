from rich.style import Style
from rich.text import Text
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Center, Container, Horizontal, ScrollableContainer
from textual.screen import Screen
from textual.widgets import (
    Button,
    DataTable,
    Label,
    Rule,
    Static,
    TabbedContent,
    TabPane,
    TextArea,
)
from textual.widgets.text_area import TextAreaTheme

from dolphie.Modules.Functions import format_number
from dolphie.Widgets.TopBar import TopBar


class ThreadScreen(Screen):
    CSS = """
        ThreadScreen {
            background: #0a0e1b;

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
                color: #bbc8e8;
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

            & ScrollableContainer {
                height: auto;
                width: 50vw;
                max-height: 15;
            }

            & .table {
                content-align: center middle;
                background: #0f1525;
                border: tall #1d253e;
                padding-left: 1;
                padding-right: 1;
                height: auto;
            }

            & TextArea {
                border: tall #1d253e;
                max-height: 25;
            }

            & .copy-button {
                margin-left: 1;
                margin-right: 1;
                background: #1d253e;
                color: #bbc8e8;
                border: tall #2F3C59;
            }

            & .copy-button:hover {
                background: #2F3C59;
                color: #ffffff;
            }

            & .copy-buttons {
                margin-bottom: 1;
            }
        }

    """

    BINDINGS = [
        Binding("q", "app.pop_screen", "", show=False),
        Binding("c", "copy_query", "Copy Query", show=True),
    ]

    def __init__(
        self,
        connection_status: str,
        app_version: str,
        host: str,
        thread_table: str,
        user_thread_attributes_table: str,
        query: str,
        explain_data: str,
        explain_json_data: str,
        explain_failure: str,
        transaction_history_table: str,
    ):
        super().__init__()

        self.connection_status = connection_status
        self.app_version = app_version
        self.host = host

        self.thread_table = thread_table
        self.user_thread_attributes_table = user_thread_attributes_table
        self.formatted_query = query
        self.explain_data = explain_data
        self.explain_json_data = explain_json_data
        self.explain_failure = explain_failure
        self.transaction_history_table = transaction_history_table

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

        self.explain_json_text_area = TextArea(language="json", theme="dracula", show_line_numbers=True, read_only=True)

    def copy_to_clipboard(self, text: str, content_type: str = "content"):
        """Copy text to clipboard and show notification"""
        try:
            self.app.copy_to_clipboard(text)
            self.notify(f"Copied {content_type} to clipboard!", severity="information")
        except Exception as e:
            self.notify(f"Failed to copy {content_type} to clipboard: {e}", severity="error")

    def action_copy_query(self) -> None:
        """Action to copy the query via keyboard shortcut"""
        if self.formatted_query:
            self.copy_to_clipboard(self.formatted_query.code, "query")
        else:
            self.notify("No query to copy", severity="warning")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle button press events"""
        if event.button.id == "copy_query_btn":
            if self.formatted_query:
                self.copy_to_clipboard(self.formatted_query.code, "query")
            else:
                self.notify("No query to copy", severity="warning")
        elif event.button.id == "copy_json_btn":
            if self.explain_json_data:
                self.copy_to_clipboard(self.explain_json_data, "JSON data")
            else:
                self.notify("No JSON data to copy", severity="warning")

    def on_mount(self):
        self.query_one("#thread_table").update(self.thread_table)
        self.query_one("#query").update(self.formatted_query)

        if self.transaction_history_table:
            self.query_one("#transaction_history_table").update(self.transaction_history_table)
        else:
            self.query_one("#transaction_history_container").display = False

        if self.user_thread_attributes_table:
            self.query_one("#user_thread_attributes_table").update(self.user_thread_attributes_table)
        else:
            self.query_one("#user_thread_attributes_table").display = False

        if self.formatted_query:
            if self.explain_failure:
                self.query_one("#explain_tabbed_content").display = False
                self.query_one("#explain_failure").update(Text.from_markup(self.explain_failure))
            elif self.explain_data:
                self.query_one("#explain_failure").display = False

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
                            explain_table.add_column(f"[label]{column}")
                            columns.append(column)

                        if column == "key" and value is None:
                            value = "[b white on #B30000]NO INDEX[/b white on #B30000]"

                        if column == "rows":
                            value = format_number(value)

                        values.append(str(value))

                    explain_table.add_row(*values)
            else:
                self.query_one("#explain_table").display = False
                self.query_one("#explain_failure").display = False
        else:
            self.query_one("#query_container").display = False

        if self.explain_json_data:
            self.explain_json_text_area.text = self.explain_json_data
        else:
            self.query_one("#explain_tabbed_content").display = False

    def compose(self) -> ComposeResult:
        yield TopBar(connection_status=self.connection_status, app_version=self.app_version, host=self.host)

        with Container(id="thread_container", classes="container"):
            with Container():
                yield Label("Thread Details", classes="title")
                yield ScrollableContainer(Static(id="thread_table"), classes="table")
            with Container():
                yield Label("Thread Attributes", classes="title")
                yield ScrollableContainer(Static(id="user_thread_attributes_table"), classes="table")

        with Container(id="query_container", classes="container"):
            yield Rule(line_style="heavy")

            # Only show copy buttons if there's data to copy
            copy_buttons = []
            if self.formatted_query:
                copy_buttons.append(Button("📋 Copy Query", id="copy_query_btn", classes="copy-button"))
            if self.explain_json_data:
                copy_buttons.append(Button("📋 Copy JSON", id="copy_json_btn", classes="copy-button"))

            # Only create the horizontal container if there are buttons to show
            if copy_buttons:
                with Horizontal(classes="button_container copy-buttons"):
                    for button in copy_buttons:
                        yield button

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
