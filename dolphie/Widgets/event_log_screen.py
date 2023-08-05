from dolphie import Queries
from dolphie.Widgets.topbar import TopBar
from rich import box
from rich.table import Table
from textual import events
from textual.app import ComposeResult
from textual.containers import Horizontal
from textual.reactive import reactive
from textual.screen import Screen
from textual.widgets import Label, RichLog, Switch


class EventLog(Screen):
    CSS = """
        EventLog Horizontal {
            height: auto;
            padding-top: 1;
            align: center top;
            background: #000718;
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
        }
    """
    event_log_data = reactive("", init=False)

    def __init__(self, app_version, host, db):
        super().__init__()

        self.app_version = app_version
        self.host = host
        self.db = db
        self.obj_event_log_data = ""
        self.event_log_data = ""

        self.levels = {
            "system": {"active": False, "sql": "prio = 'System'"},
            "warning": {"active": False, "sql": "prio = 'Warning'"},
            "error": {"active": False, "sql": "prio = 'Error'"},
        }

    def on_mount(self):
        switches = self.query("Switch")
        for switch in switches:
            switch.toggle()

        richlog = self.query_one("#richlog", RichLog)
        richlog.focus()

    def on_key(self, event: events.Key):
        exclude_events = ["up", "down", "left", "right", "pageup", "pagedown", "home", "end", "tab", "enter"]
        if event.key not in exclude_events:
            self.app.pop_screen()

    def compose(self) -> ComposeResult:
        yield TopBar(app_version=self.app_version, host=self.host)
        with Horizontal():
            yield Label("System")
            yield Switch(animate=False, id="system")
            yield Label("Warning")
            yield Switch(animate=False, id="warning")
            yield Label("Error")
            yield Switch(animate=False, id="error")
        yield Label("", id="info")
        yield RichLog(id="richlog", auto_scroll=False, markup=True)

    def watch_event_log_data(self):
        info = self.query_one("#info", Label)
        text_log = self.query_one("#richlog", RichLog)
        text_log.clear()

        active_levels = any(data["active"] for data in self.levels.values())

        if self.event_log_data:
            info.display = False
            text_log.write(self.event_log_data)
        else:
            info.display = True
            if active_levels:
                info.update("[b] No events found[/b]")
            else:
                info.update("[b] Toggle the switches above to filter what events you'd like to see[/b]")

    def on_switch_changed(self, event: Switch.Changed):
        self.event_log_data = None
        self.levels[event.switch.id]["active"] = event.value

        where_clause = ""
        active_sql_list = []
        for level, data in self.levels.items():
            if data["active"]:
                active_sql_list.append(data["sql"])

        if active_sql_list:
            where_clause = " OR ".join(active_sql_list)

        table = Table(show_header=True, box=box.SIMPLE, style="#52608d")
        table.add_column("Time", style="#8e8f9d")
        table.add_column("Level")
        table.add_column("Event")

        if where_clause:
            query = Queries["error_log"].replace("$placeholder", f"AND ({where_clause})")
            self.db.execute(query)
            data = self.db.fetchall()

            for row in data:
                level_color = ""
                if row["level"] == "Error":
                    level_color = "[white on red]"
                elif row["level"] == "Warning":
                    level_color = "[#f1fb82]"

                level = f"{level_color}{row['level']}"
                table.add_row(row["timestamp"].strftime("%Y-%m-%d %H:%M:%S"), level, row["message"])

        if table.rows:
            self.event_log_data = table
        else:
            # We don't use None here like above since if the change is the same as the previous toggle,
            # it won't trigger watch_ function
            self.event_log_data = ""
