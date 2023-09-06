from dolphie.Modules.Queries import MySQLQueries
from dolphie.Widgets.topbar import TopBar
from textual import events, on
from textual.app import ComposeResult
from textual.containers import Container, Horizontal
from textual.screen import Screen
from textual.widgets import DataTable, Input, Label, Switch


class EventLog(Screen):
    CSS = """
        EventLog Horizontal {
            height: auto;
            # padding-top: 1;
            align: center top;
            background: #000718;
            width: 100%;
        }
        EventLog Horizontal > Label {
            color: #bbc8e8;
            text-style: bold;
            margin-right: -1;
        }
        #info {
            padding-top: 1;
            width: 100%;
            text-align: center;
            text-style: bold;
        }
        #search {
            background: #030918;
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

    def __init__(self, read_only, app_version, host, db):
        super().__init__()

        self.read_only = read_only
        self.app_version = app_version
        self.host = host
        self.db = db

        self.levels = {
            "system": {"active": True, "sql": "prio = 'System'"},
            "warning": {"active": True, "sql": "prio = 'Warning'"},
            "error": {"active": True, "sql": "prio = 'Error'"},
        }

    def on_mount(self):
        datatable = self.query_one(DataTable)
        datatable.focus()
        datatable.styles.overflow_x = "auto"

        self.update_datatable()

    @on(events.Key)
    def on_keypress(self, event: events.Key):
        if event.key == "q":
            if self.screen.is_attached:
                self.app.pop_screen()
        elif event.key == "1":
            table = self.query_one(DataTable)
            table.move_cursor(row=0)
        elif event.key == "2":
            table = self.query_one(DataTable)
            table.move_cursor(row=table.row_count - 1)

    def compose(self) -> ComposeResult:
        yield TopBar(
            read_only=self.read_only,
            app_version=self.app_version,
            host=self.host,
            help="Press [b]q[/b] to return",
        )

        yield Label("[b white]1[/b white] = top of events/[b white]2[/b white] = bottom of events", id="help")
        with Horizontal():
            switch_options = [("System", "system"), ("Warning", "warning"), ("Error", "error")]
            for label, switch_id in switch_options:
                yield Label(label)
                yield Switch(animate=False, id=switch_id, value=True)

        yield Label("", id="info")
        yield Input(id="search", placeholder="Search (hit enter when ready)")
        with Container():
            yield DataTable(show_cursor=False)

    @on(Input.Submitted, "#search")
    def event_search(self):
        self.update_datatable()

    @on(Switch.Changed)
    def switch_changed(self, event: Switch.Changed):
        self.levels[event.switch.id]["active"] = event.value

        self.update_datatable()

    def update_datatable(self):
        active_sql_list = [data["sql"] for data in self.levels.values() if data["active"]]
        where_clause = " OR ".join(active_sql_list)

        search_text = self.query_one("#search", Input)
        if search_text.value:
            where_clause = f"({where_clause}) AND (data LIKE '%{search_text.value}%')"

        table = self.query_one(DataTable)
        info = self.query_one("#info", Label)

        table.clear(columns=True)
        table.add_column("Time")
        table.add_column("Level")

        if where_clause:
            query = MySQLQueries.error_log.replace("$1", f"AND ({where_clause})")
            event_count = self.db.execute(query)
            data = self.db.fetchall()

            if data:
                table.add_column(f"Event ({event_count})")

                info.display = False
                table.display = True
                search_text.display = True

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

                    table.add_row(timestamp, level, row["message"])
            else:
                table.display = False
                search_text.display = False
                info.display = True
                info.update("No events found")
        else:
            table.display = False
            search_text.display = False
            info.display = True
            info.update("Toggle the switches above to filter what events you'd like to see")
