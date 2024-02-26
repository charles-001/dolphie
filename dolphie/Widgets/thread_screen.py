from dolphie.Modules.Functions import format_number
from dolphie.Widgets.topbar import TopBar
from textual import events, on
from textual.app import ComposeResult
from textual.containers import Center, Container, ScrollableContainer
from textual.screen import Screen
from textual.widgets import DataTable, Label, Rule, Static


class ThreadScreen(Screen):
    CSS = """
    ThreadScreen {
        background: #0a0e1b;
    }
    ThreadScreen #explain_table {
        margin-top: 1;
        background: #101626;
        border: tall #1d253e;
        overflow-x: auto;
        min-height: 5;
        max-height: 15;
        width: 100%;
    }
    ThreadScreen #explain_failure {
        margin-top: 1;
    }
    ThreadScreen Container {
        height: auto;
    }
    ThreadScreen #thread_container {
        margin-top: 1;
        height: auto;
        layout: horizontal;
    }
    ThreadScreen Label {
        width: 100%;
        content-align: center middle;
        color: #bbc8e8;
        text-style: bold;
    }
    ThreadScreen Center {
        height: auto;
    }
    ThreadScreen #query {
        width: auto;
    }
    ThreadScreen .container > Center {
        layout: horizontal;
    }
    ThreadScreen ScrollableContainer {
        height: auto;
        width: 50vw;
        max-height: 16;
    }

    ThreadScreen .table {
        content-align: center middle;
        background: #101626;
        border: tall #1d253e;
        padding-left: 1;
        padding-right: 1;
        height: auto;
    }
    """

    def __init__(
        self,
        connection_status: str,
        app_version: str,
        host: str,
        thread_table: str,
        user_thread_attributes_table: str,
        query: str,
        explain_data: str,
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
        self.explain_failure = explain_failure
        self.transaction_history_table = transaction_history_table

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
                self.query_one("#explain_table").display = False
                self.query_one("#explain_failure").update(self.explain_failure)
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

    def compose(self) -> ComposeResult:
        yield TopBar(connection_status=self.connection_status, app_version=self.app_version, host=self.host)

        with Container(id="thread_container", classes="container"):
            with Container():
                yield Label("Thread Details")
                yield ScrollableContainer(Static(id="thread_table"), classes="table")
            with Container():
                yield Label("Thread Attributes")
                yield ScrollableContainer(Static(id="user_thread_attributes_table"), classes="table")

        with Container(id="query_container", classes="container"):
            yield Rule(line_style="heavy")
            yield Label("Query Details")
            yield Center(Static(id="query", shrink=True, classes="table"))

            yield DataTable(show_cursor=False, id="explain_table", classes="table")
            yield Label("", id="explain_failure")

        with Container(id="transaction_history_container", classes="container"):
            yield Rule(line_style="heavy")
            yield Label("Transaction History", id="transaction_history_label")
            yield Center(
                Static(id="transaction_history_table", shrink=True, classes="table"),
                id="transaction_history_table_center",
            )

    @on(events.Key)
    def on_keypress(self, event: events.Key):
        if event.key == "q":
            if self.screen.is_attached:
                self.app.pop_screen()
