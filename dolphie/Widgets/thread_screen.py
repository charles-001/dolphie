from dolphie.Modules.Functions import format_number
from dolphie.Widgets.topbar import TopBar
from textual import events, on
from textual.app import ComposeResult
from textual.containers import Center, Container
from textual.screen import Screen
from textual.widgets import DataTable, Label, Rule, Static


class ThreadScreen(Screen):
    CSS = """
        ThreadScreen {
            background: #030918;
        }
        ThreadScreen #explain_table {
            margin-top: 1;
            background: #0b1221;
            border: tall #1c2238;
            overflow-x: auto;
            min-height: 5;
            max-height: 15;
        }
        ThreadScreen #explain_failure {
            margin-top: 1;
        }
        ThreadScreen .container {
            height: auto;
        }
        ThreadScreen .container > Label {
            width: 100%;
            content-align: center middle;
            color: #bbc8e8;
            text-style: bold;
            margin-bottom: 1;
        }
        ThreadScreen .container  > Center {
            height: auto;
        }
        ThreadScreen .container  > Center > Static {
            width: auto;
            content-align: center middle;
            background: #0b1221;
            border: tall #1c2238;
            padding-left: 1;
            padding-right: 1;
        }
    """

    def __init__(
        self,
        read_only: bool,
        app_version: str,
        host: str,
        thread_table: str,
        query: str,
        explain_data: str,
        explain_failure: str,
        transaction_history_table: str,
    ):
        super().__init__()

        self.read_only = read_only
        self.app_version = app_version
        self.host = host

        self.thread_table = thread_table
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
        yield TopBar(read_only=self.read_only, app_version=self.app_version, host=self.host)

        with Container(id="thread_container", classes="container"):
            yield Label("Thread Details")
            yield Center(Static(id="thread_table"))

        with Container(id="query_container", classes="container"):
            yield Rule(line_style="heavy")
            yield Label("Query Details")
            yield Center(Static(id="query", shrink=True))

            yield DataTable(show_cursor=False, id="explain_table")
            yield Label("", id="explain_failure")

        with Container(id="transaction_history_container", classes="container"):
            yield Rule(line_style="heavy")
            yield Label("Transaction History", id="transaction_history_label")
            yield Center(Static(id="transaction_history_table"))

    @on(events.Key)
    def on_keypress(self, event: events.Key):
        if event.key == "q":
            if self.screen.is_attached:
                self.app.pop_screen()
