from __future__ import annotations

import textwrap
from typing import ClassVar, Protocol, TypedDict

from rich.markup import escape as markup_escape
from textual import on, work
from textual.app import ComposeResult
from textual.binding import Binding, BindingType
from textual.containers import Container, Horizontal
from textual.widgets import Input, Label, Switch

from dolphie.DataTypes import DatabaseRow
from dolphie.Modules.Queries import MySQLQueries
from dolphie.Modules.Theme import ThemedDataTable as DataTable
from dolphie.Widgets.DolphieScreen import DolphieScreen, ScreenContext
from dolphie.Widgets.SpinnerWidget import SpinnerWidget


class EventLogInput(Input):
    """Event-log input with a context-sensitive footer action."""

    BINDINGS: ClassVar[list[BindingType]] = [
        Binding("enter", "submit", "Apply"),
        *Input.BINDINGS,
    ]


class EventLogDatabase(Protocol):
    """Database operations used by the event log."""

    def execute(self, query: str) -> object:
        """Execute an event-log query."""
        ...

    def fetchall(self) -> list[DatabaseRow]:
        """Return the rows from the most recent query."""
        ...


class EventLogLevel(TypedDict):
    """Filter state for one event severity."""

    active: bool
    sql: str


class EventLog(DolphieScreen):
    CSS = """
        EventLog {
            & Horizontal {
                height: auto;
                align: center top;
                background: $background;
                width: 100%;

                & > Label {
                    color: $light_blue;
                    text-style: bold;
                    margin-right: -1;
                }
            }
            & DataTable {
                background: $background;
                border: none;
                overflow-x: auto;
                max-height: 100%;

                &:focus {
                    background-tint: $background;
                }
            }
            & SpinnerWidget {
                margin-top: 1;
            }
            & .input_container {
                align: left top;
                padding-left: 1;

                & > Input {
                    border: none;
                    background: $background;
                    margin: 0;
                    height: 1;
                }
            }
            & #days_container {
                & > Input {
                    width: 15;
                }
                & > Label {
                    margin-right: 2;
                }
            }
            & #info {
                padding-top: 1;
                width: 100%;
                text-align: center;
                text-style: bold;
            }
            & #search {
                width: 90%;
                margin-bottom: 1;
            }
        }
    """

    BINDINGS = [
        Binding("r", "refresh_log", "Refresh"),
        Binding("1", "first_entry", "Newest"),
        Binding("2", "last_entry", "Oldest"),
        Binding("slash", "focus_search", "", show=False),
    ]

    def __init__(self, context: ScreenContext, db_connection: EventLogDatabase):
        super().__init__(context)
        self.db_connection = db_connection

        self.levels: dict[str, EventLogLevel] = {
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

    def compose_content(self) -> ComposeResult:
        """Compose event filters, loading state, and event rows."""
        with Horizontal(classes="switch_container"):
            switch_options = [("System", "system"), ("Warning", "warning"), ("Error", "error"), ("Note", "note")]
            for label, switch_id in switch_options:
                yield Label(label)
                yield Switch(animate=False, id=switch_id, value=True)
        with Horizontal(id="days_container", classes="input_container"):
            yield Label("Days to display")
            yield EventLogInput(id="days", value="30")
        with Horizontal(id="search_container", classes="input_container"):
            yield Label("Search event text")
            yield EventLogInput(id="search", placeholder="Specify event text to display")
        yield SpinnerWidget(id="spinner", text="Loading events")
        yield Label("", id="info")
        with Container():
            yield DataTable(show_cursor=False)

    @on(Input.Submitted, "Input")
    def event_search(self):
        self.update_datatable()

    def action_refresh_log(self) -> None:
        """Reload events using the current filters."""
        self.update_datatable()

    def action_first_entry(self) -> None:
        """Move to the newest visible event."""
        if self.datatable.row_count:
            self.datatable.move_cursor(row=0)

    def action_last_entry(self) -> None:
        """Move to the oldest visible event."""
        if self.datatable.row_count:
            self.datatable.move_cursor(row=self.datatable.row_count - 1)

    def action_focus_search(self) -> None:
        """Focus the event-text search input."""
        self.search_text.focus()

    def update_datatable(self):
        # Read widget state on the main thread before spawning the worker
        for switch in self.query(Switch):
            switch_id = switch.id
            if switch_id is not None and switch_id in self.levels:
                self.levels[switch_id]["active"] = switch.value

        days_value = self.days_to_display.value
        search_value = self.search_text.value

        self._fetch_and_display(days_value, search_value)

    @work(thread=True)
    def _fetch_and_display(self, days_value: str, search_value: str):
        # Verify days is a number
        try:
            int(days_value)
        except ValueError:
            self.app.call_from_thread(self._show_error, "[$red]Days to display must be a number[/$red]")
            return

        self.app.call_from_thread(self._prepare_datatable)

        active_sql_list = [data["sql"] for data in self.levels.values() if data["active"]]
        where_clause = " OR ".join(active_sql_list)

        if search_value:
            where_clause = f"({where_clause}) AND (data LIKE '%{search_value}%')"

        if where_clause:
            query = MySQLQueries.error_log.replace("$1", f"AND ({where_clause})")
            query = query.replace("$2", f"AND logged > NOW() - INTERVAL {days_value} DAY")
            event_count = self.db_connection.execute(query)
            data = self.db_connection.fetchall()

            self.app.call_from_thread(self._populate_datatable, event_count, data)
        else:
            self.app.call_from_thread(
                self._show_error,
                "No switches selected. Toggle the switches above to filter what events you'd like to see",
            )

    def _show_error(self, message: str):
        self.datatable.display = False
        self.info.display = True
        self.info.update(message)
        self.spinner.hide()

    def _prepare_datatable(self):
        self.spinner.show()
        self.info.display = False
        self.datatable.display = False
        self.datatable.clear(columns=True)
        self.datatable.add_column("Date/Time")
        self.datatable.add_column("Subsystem")
        self.datatable.add_column("Level")
        self.datatable.add_column("Code")

    def _populate_datatable(self, event_count, data):
        if data:
            self.datatable.add_column(f"Event ({event_count})")

            for row in data:
                level_color = ""
                if row["level"] == "Error":
                    level_color = "$red"
                elif row["level"] == "Warning":
                    level_color = "$yellow"
                elif row["level"] == "Note":
                    level_color = "$dark_gray"

                level = row["level"]
                if level_color:
                    level = f"[{level_color}]{row['level']}[/{level_color}]"

                timestamp = f"[$dark_gray]{row['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}[/$dark_gray]"
                error_code = f"[$label]{row['error_code']}[/$label]"
                subsystem = markup_escape(row["subsystem"])

                # Wrap the message to 78% of console width so hopefully we don't get a scrollbar
                wrapped_lines = textwrap.wrap(markup_escape(row["message"]), width=round(self.app.console.width * 0.75))
                wrapped_message = "\n".join(wrapped_lines)

                height = max(len(wrapped_lines), 1)

                self.datatable.add_row(timestamp, subsystem, level, error_code, wrapped_message, height=height)

            self.datatable.display = True
            self.datatable.focus()
        else:
            self.info.display = True
            self.info.update("No events found")

        self.spinner.hide()
