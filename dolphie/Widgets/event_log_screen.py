import textwrap

from dolphie.Modules.MySQL import Database
from dolphie.Modules.Queries import MySQLQueries
from dolphie.Widgets.spinner import SpinnerWidget
from dolphie.Widgets.topbar import TopBar
from textual import events, on, work
from textual.app import ComposeResult
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
            overflow-x: hidden;
            max-height: 100%;
        }
        EventLog SpinnerWidget {
            margin-top: 1;
        }
        #info {
            padding-top: 1;
            width: 100%;
            text-align: center;
            text-style: bold;
        }
        #search {
            background: #0a0e1b;
            content-align: right middle;
            padding-left: 1;
            margin: 0;
            border: none;
        }
        #help {
            color: #8f9fc1;
            width: 100%;
            content-align: right middle;
        }
    """

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
        }

    def on_mount(self):
        self.datatable = self.query_one(DataTable)
        self.datatable.focus()

        self.spinner = self.query_one(SpinnerWidget)
        self.info = self.query_one("#info", Label)
        self.search_text = self.query_one("#search", Input)

        self.info.display = False
        self.datatable.display = False
        self.search_text.display = False

        self.update_datatable()

    @on(events.Key)
    def on_keypress(self, event: events.Key):
        if event.key == "q":
            if self.screen.is_attached:
                self.app.pop_screen()
        elif event.key == "1":
            self.datatable.move_cursor(row=0)
        elif event.key == "2":
            self.datatable.move_cursor(row=self.datatable.row_count - 1)

    def compose(self) -> ComposeResult:
        yield TopBar(connection_status=self.connection_status, app_version=self.app_version, host=self.host)
        yield Label("[b white]1[/b white] = top of events/[b white]2[/b white] = bottom of events", id="help")
        with Horizontal():
            switch_options = [("System", "system"), ("Warning", "warning"), ("Error", "error")]
            for label, switch_id in switch_options:
                yield Label(label)
                yield Switch(animate=False, id=switch_id, value=True)
        yield SpinnerWidget(id="spinner", text="Loading events")
        yield Input(id="search", placeholder="Search (hit enter when ready)")
        yield Label("", id="info")
        with Container():
            yield DataTable(show_cursor=False)

    @on(Input.Submitted, "#search")
    def event_search(self):
        self.update_datatable()

    @on(Switch.Changed)
    def switch_changed(self, event: Switch.Changed):
        self.levels[event.switch.id]["active"] = event.value

        self.update_datatable()

    @work(thread=True)
    def update_datatable(self):
        self.spinner.show()

        self.info.display = False
        self.datatable.display = False
        self.search_text.display = False

        active_sql_list = [data["sql"] for data in self.levels.values() if data["active"]]
        where_clause = " OR ".join(active_sql_list)

        if self.search_text.value:
            where_clause = f"({where_clause}) AND (data LIKE '%{self.search_text.value}%')"

        self.datatable.clear(columns=True)
        self.datatable.add_column("Time")
        self.datatable.add_column("Level")

        if where_clause:
            query = MySQLQueries.error_log.replace("$1", f"AND ({where_clause})")
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

                    level = row["level"]
                    if level_color:
                        level = f"[{level_color}]{row['level']}[/{level_color}]"

                    timestamp = f"[#858A97]{row['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}[/#858A97]"

                    # Wrap the message to 78% of console width so hopefully we don't get a scrollbar
                    wrapped_message = textwrap.wrap(row["message"], width=round(self.app.console.width * 0.78))
                    wrapped_message = "\n".join(wrapped_message)

                    line_counts = [cell.count("\n") + 1 for cell in wrapped_message]
                    height = max(line_counts)

                    self.datatable.add_row(timestamp, level, wrapped_message, height=height)

                self.datatable.display = True
                self.search_text.display = True
                self.datatable.focus()
            else:
                self.datatable.display = False
                self.search_text.display = True
                self.info.display = True
                self.info.update("No events found")
        else:
            self.datatable.display = False
            self.search_text.display = False
            self.info.display = True
            self.info.update("Toggle the switches above to filter what events you'd like to see")

        self.spinner.hide()
