import textwrap

from dolphie.Modules.MySQL import Database
from dolphie.Modules.Queries import MySQLQueries
from dolphie.Widgets.spinner import SpinnerWidget
from dolphie.Widgets.topbar import TopBar
from textual import events, on, work
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal
from textual.screen import Screen
from textual.widgets import DataTable, Input, Label, Switch


class EventLog(Screen):
    CSS = """
        EventLog Horizontal {
            height: auto;
            align: center top;
            background: #0a0e1b;
            width: 100%;
        }
        EventLog Horizontal > Label {
            color: #bbc8e8;
            text-style: bold;
            margin-right: -1;
        }
        EventLog DataTable {
            background: #0a0e1b;
            border: none;
            overflow-x: auto;
            max-height: 100%;
        }
        EventLog SpinnerWidget {
            margin-top: 1;
        }
        .input_container {
            align: left top;
            padding-left: 1;
        }
        .input_container > Input {
            border: none;
            background: #0a0e1b;
            margin: 0;
            height: 1;
        }
        #days_container > Input {
            width: 15;
        }
        #days_container > Label {
            margin-right: 2;
        }
        #info {
            padding-top: 1;
            width: 100%;
            text-align: center;
            text-style: bold;
        }
        #search {
            width: 90%;
            margin-bottom: 1;
        }
        #help {
            color: #8f9fc1;
            width: 100%;
            content-align: right middle;
        }
    """

    BINDINGS = [
        Binding("q", "app.pop_screen", "", show=False),
    ]

    def __init__(self, connection_status, app_version, host, db_connection: Database):
        super().__init__()

        self.connection_status = connection_status
        self.app_version = app_version
        self.host = host
        self.db_connection = db_connection

        self.levels = {
            "system": {"active": True, "sql": "prio = 'System'"},
            "warning": {"active": True, "sql": "prio = 'Warning'"},
            "error": {"active": True, "sql": "prio = 'Error'"},
            "note": {"active": True, "sql": "prio = 'Note'"},
        }

    def on_mount(self):
        self.datatable = self.query_one(DataTable)
        self.datatable.focus()

        self.spinner = self.query_one(SpinnerWidget)
        self.info = self.query_one("#info", Label)
        self.search_text = self.query_one("#search", Input)
        self.days_to_display = self.query_one("#days", Input)

        self.info.display = False
        self.datatable.display = False

        self.update_datatable()

    @on(events.Key)
    def on_keypress(self, event: events.Key):
        if event.key == "1":
            self.datatable.move_cursor(row=0)
        elif event.key == "2":
            self.datatable.move_cursor(row=self.datatable.row_count - 1)
        elif event.key == "r":
            self.update_datatable()

    def compose(self) -> ComposeResult:
        yield TopBar(connection_status=self.connection_status, app_version=self.app_version, host=self.host)
        yield Label(
            "[b white]r[/b white] = refresh/[b white]1[/b white] = top of events/"
            "[b white]2[/b white] = bottom of events",
            id="help",
        )
        with Horizontal():
            switch_options = [("System", "system"), ("Warning", "warning"), ("Error", "error"), ("Note", "note")]
            for label, switch_id in switch_options:
                yield Label(label)
                yield Switch(animate=False, id=switch_id, value=True)
        with Horizontal(id="days_container", classes="input_container"):
            yield Label("Days to display")
            yield Input(id="days", value="30")
        with Horizontal(id="search_container", classes="input_container"):
            yield Label("Search event text")
            yield Input(id="search", placeholder="Specify event text to display")
        yield SpinnerWidget(id="spinner", text="Loading events")
        yield Label("", id="info")
        with Container():
            yield DataTable(show_cursor=False)

    @on(Input.Submitted, "Input")
    def event_search(self):
        self.update_datatable()

    @work(thread=True)
    def update_datatable(self):
        for switch in self.query(Switch):
            self.levels[switch.id]["active"] = switch.value

        # Verify days is a number
        try:
            int(self.days_to_display.value)
        except ValueError:
            self.datatable.display = False
            self.info.display = True
            self.info.update("[red]Days to display must be a number[/red]")
            return

        self.spinner.show()

        self.info.display = False
        self.datatable.display = False

        active_sql_list = [data["sql"] for data in self.levels.values() if data["active"]]
        where_clause = " OR ".join(active_sql_list)

        if self.search_text.value:
            where_clause = f"({where_clause}) AND (data LIKE '%{self.search_text.value}%')"

        self.datatable.clear(columns=True)
        self.datatable.add_column("Date/Time")
        self.datatable.add_column("Subsystem")
        self.datatable.add_column("Level")
        self.datatable.add_column("Code")

        if where_clause:
            query = MySQLQueries.error_log.replace("$1", f"AND ({where_clause})")
            query = query.replace("$2", f"AND logged > NOW() - INTERVAL {self.days_to_display.value} DAY")
            event_count = self.db_connection.execute(query)
            data = self.db_connection.fetchall()

            if data:
                self.datatable.add_column(f"Event ({event_count})")

                for row in data:
                    level_color = ""
                    if row["level"] == "Error":
                        level_color = "red"
                    elif row["level"] == "Warning":
                        level_color = "yellow"
                    elif row["level"] == "Note":
                        level_color = "dark_gray"

                    level = row["level"]
                    if level_color:
                        level = f"[{level_color}]{row['level']}[/{level_color}]"

                    timestamp = f"[#858A97]{row['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}[/#858A97]"
                    error_code = f"[label]{row['error_code']}[/label]"
                    subsystem = row["subsystem"]

                    # Wrap the message to 78% of console width so hopefully we don't get a scrollbar
                    wrapped_message = textwrap.wrap(row["message"], width=round(self.app.console.width * 0.75))
                    wrapped_message = "\n".join(wrapped_message)

                    line_counts = [cell.count("\n") + 1 for cell in wrapped_message]
                    height = max(line_counts)

                    self.datatable.add_row(timestamp, subsystem, level, error_code, wrapped_message, height=height)

                self.datatable.display = True
                self.datatable.focus()
            else:
                self.datatable.display = False
                self.info.display = True
                self.info.update("No events found")
        else:
            self.datatable.display = False
            self.info.display = True
            self.info.update("No switches selected. Toggle the switches above to filter what events you'd like to see")

        self.spinner.hide()
