import re

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Checkbox, Input, Label, Static

from dolphie.DataTypes import ConnectionSource, HotkeyCommands
from dolphie.Widgets.autocomplete import AutoComplete, Dropdown, DropdownItem


class CommandModal(ModalScreen):
    CSS = """
        CommandModal > Vertical {
            background: #131626;
            border: tall #384673;
            height: auto;
            width: auto;
        }
        CommandModal > Vertical > * {
            width: auto;
            height: auto;
            content-align: center middle;
        }
        CommandModal #filter_container {
            width: auto;
            height: auto;
        }
        CommandModal #kill_container {
            width: auto;
            height: auto;
        }
        CommandModal #filter_container Input {
            width: 60;
            border-title-color: #d2d2d2;
        }
        CommandModal #kill_container Input {
            width: 60;
            border-title-color: #d2d2d2;
        }
        CommandModal Label {
            width: 100%;
            content-align: center middle;
            padding-bottom: 1;
        }
        CommandModal #error_response {
            color: #fe5c5c;
            width: 100%;
            height: auto;
            content-align: center middle;
            padding-bottom: 1;
        }
        CommandModal Checkbox {
            background: #131626;
            border: none;
            content-align: center middle;
            padding-top: 1;
            width: 100%;
        }
        CommandModal #sleeping_queries {
            padding-bottom: 1;
        }
    """
    BINDINGS = [
        Binding("escape", "app.pop_screen", "", show=False),
    ]

    def __init__(
        self,
        command,
        message,
        connection_source: ConnectionSource = None,
        processlist_data=None,
        host_cache_data=None,
        max_replay_timestamp=None,
    ):
        super().__init__()
        self.command = command
        self.message = message
        self.connection_source = connection_source
        self.processlist_data = processlist_data
        self.host_cache_data = host_cache_data
        self.max_replay_timestamp = max_replay_timestamp

        self.dropdown_items = []
        if processlist_data:
            sorted_keys = sorted(processlist_data.keys(), key=lambda x: int(x))
            self.dropdown_items = [DropdownItem(thread_id) for thread_id in sorted_keys]

    def compose(self) -> ComposeResult:
        with Vertical():
            with Vertical():
                yield Label(f"[b]{self.message}[/b]")
                with Vertical(id="filter_container"):
                    yield AutoComplete(
                        Input(id="filter_by_username_input"), Dropdown(id="filter_by_username_dropdown_items", items=[])
                    )
                    yield AutoComplete(
                        Input(id="filter_by_host_input"), Dropdown(id="filter_by_host_dropdown_items", items=[])
                    )
                    yield AutoComplete(
                        Input(id="filter_by_db_input"), Dropdown(id="filter_by_db_dropdown_items", items=[])
                    )
                    yield AutoComplete(
                        Input(id="filter_by_hostgroup_input"),
                        Dropdown(id="filter_by_hostgroup_dropdown_items", items=[]),
                    )
                    yield Input(id="filter_by_query_time_input")
                    yield Input(id="filter_by_query_text_input")
                with Vertical(id="kill_container"):
                    yield AutoComplete(
                        Input(id="kill_by_username_input"), Dropdown(id="kill_by_username_dropdown_items", items=[])
                    )
                    yield AutoComplete(
                        Input(id="kill_by_host_input"), Dropdown(id="kill_by_host_dropdown_items", items=[])
                    )
                    yield Input(id="kill_by_age_range_input", placeholder="Example: 5-8")
                    yield Input(id="kill_by_query_text_input")
                    yield Checkbox("Include sleeping queries", id="sleeping_queries")
                    yield Label("[dark_gray][b]Note[/b]: This feature uses threads visible in the processlist")
                yield AutoComplete(
                    Input(id="modal_input"),
                    Dropdown(id="dropdown_items", items=self.dropdown_items),
                )

                yield Static(id="error_response")
            with Horizontal(classes="button_container"):
                yield Button("Submit", id="submit", variant="primary")
                yield Button("Cancel", id="cancel")

    def on_mount(self):
        input = self.query_one("#modal_input", Input)
        filter_container = self.query_one("#filter_container", Vertical)
        kill_container = self.query_one("#kill_container", Vertical)
        self.query_one("#error_response", Static).display = False

        filter_container.display = False
        kill_container.display = False

        if self.command == HotkeyCommands.thread_filter:
            input.display = False
            filter_container.display = True

            self.query_one("#filter_by_username_input", Input).focus()
            self.query_one("#filter_by_username_input", Input).border_title = "Username"
            self.query_one("#filter_by_username_dropdown_items", Dropdown).items = self.create_dropdown_items("user")
            self.query_one("#filter_by_host_input", Input).border_title = "Host/IP"
            self.query_one("#filter_by_host_dropdown_items", Dropdown).items = self.create_dropdown_items("host")
            self.query_one("#filter_by_db_input", Input).border_title = "Database"
            self.query_one("#filter_by_db_dropdown_items", Dropdown).items = self.create_dropdown_items("db")
            self.query_one("#filter_by_query_time_input", Input).border_title = "Minimum Query Time (seconds)"
            self.query_one("#filter_by_query_text_input", Input).border_title = "Partial Query Text"

            if self.connection_source != ConnectionSource.proxysql:
                self.query_one("#filter_by_hostgroup_input", Input).display = False
            else:
                self.query_one("#filter_by_host_input", Input).border_title = "Backend Host/IP"
                self.query_one("#filter_by_hostgroup_input", Input).border_title = "Hostgroup"
                self.query_one("#filter_by_hostgroup_dropdown_items", Dropdown).items = self.create_dropdown_items(
                    "hostgroup"
                )
        elif self.command == HotkeyCommands.thread_kill_by_parameter:
            input.display = False
            kill_container.display = True

            self.query_one("#kill_by_username_input", Input).focus()
            self.query_one("#kill_by_username_input", Input).border_title = "Username"
            self.query_one("#kill_by_username_dropdown_items", Dropdown).items = self.create_dropdown_items("user")
            self.query_one("#kill_by_host_input", Input).border_title = "Host/IP"
            self.query_one("#kill_by_host_dropdown_items", Dropdown).items = self.create_dropdown_items("host")
            self.query_one("#kill_by_age_range_input", Input).border_title = "Age Range (seconds)"
            self.query_one("#kill_by_query_text_input", Input).border_title = "Partial Query Text"

            sleeping_queries_checkbox = self.query_one("#sleeping_queries", Checkbox)
            sleeping_queries_checkbox.toggle()

            input.placeholder = "Select an option from above"
        elif self.command == HotkeyCommands.rename_tab:
            input.placeholder = "Colors can be added by wrapping them in []"
            input.styles.width = 50
            input.focus()
        elif self.command == HotkeyCommands.variable_search:
            input.placeholder = "Input 'all' to show everything"
            input.focus()
        elif self.command in [HotkeyCommands.show_thread, HotkeyCommands.thread_kill_by_id]:
            input.placeholder = "Input a Thread ID"
            input.focus()
        elif self.command == HotkeyCommands.refresh_interval:
            input.placeholder = "Input a refresh interval"
            input.focus()
        elif self.command == HotkeyCommands.replay_seek:
            if self.max_replay_timestamp:
                input.value = self.max_replay_timestamp
            input.placeholder = "Format: 2024-07-25 13:00:00"
            input.focus()
        else:
            input.focus()

    def create_dropdown_items(self, field):
        dropdown_items = []

        if field:
            # Filter out None values before sorting
            sorted_array = sorted(
                set(
                    getattr(thread, field)
                    for thread in self.processlist_data.values()
                    if getattr(thread, field) is not None
                )
            )
            dropdown_items = [DropdownItem(str(value)) for value in sorted_array]

        return dropdown_items

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id != "submit":
            self.app.pop_screen()
            return

        modal_input = self.query_one("#modal_input", Input).value
        if not modal_input and self.command not in [
            HotkeyCommands.rename_tab,
            HotkeyCommands.thread_kill_by_parameter,
            HotkeyCommands.thread_filter,
        ]:
            self.update_error_response("Input cannot be empty")
            return

        if self.command == HotkeyCommands.thread_filter:
            # Fetch all filter values
            filters = {
                "username": self.query_one("#filter_by_username_input", Input).value,
                "host": self.query_one("#filter_by_host_input", Input).value,
                "db": self.query_one("#filter_by_db_input", Input).value,
                "hostgroup": self.query_one("#filter_by_hostgroup_input", Input).value,
                "query_time": self.query_one("#filter_by_query_time_input", Input).value,
                "query_text": self.query_one("#filter_by_query_text_input", Input).value,
            }

            # Use IP address instead of hostname since that's what is used in the processlist
            if filters["host"]:
                filters["host"] = next(
                    (ip for ip, addr in self.host_cache_data.items() if filters["host"] == addr), filters["host"]
                )

            # Validate numeric fields
            for value, field_name in [(filters["query_time"], "Query time"), (filters["hostgroup"], "Hostgroup")]:
                if value and not re.search(r"^\d+$", value):
                    self.update_error_response(f"{field_name} must be an integer")
                    return

            # Ensure at least one filter is provided
            if not any(filters.values()):
                self.update_error_response("At least one field must be provided")
                return

            # Dismiss with the filter values
            self.dismiss(list(filters.values()))
        elif self.command == HotkeyCommands.thread_kill_by_parameter:
            # Get input values
            kill_by_username = self.query_one("#kill_by_username_input", Input).value
            kill_by_host = self.query_one("#kill_by_host_input", Input).value
            kill_by_age_range = self.query_one("#kill_by_age_range_input", Input).value
            kill_by_query_text = self.query_one("#kill_by_query_text_input", Input).value
            checkbox_sleeping_queries = self.query_one("#sleeping_queries", Checkbox).value

            age_range_lower_limit, age_range_upper_limit = None, None

            # Process and validate age range input
            if kill_by_age_range:
                match = re.match(r"(\d+)-(\d+)", kill_by_age_range)
                if match:
                    age_range_lower_limit, age_range_upper_limit = map(int, match.groups())
                    if age_range_lower_limit > age_range_upper_limit:
                        self.update_error_response("Invalid age range! Lower limit can't be higher than upper")
                        return
                else:
                    self.update_error_response("Invalid age range")
                    return

            # Ensure at least one parameter is provided
            if not any([kill_by_username, kill_by_host, kill_by_age_range, kill_by_query_text]):
                self.update_error_response("At least one parameter must be provided")
                return

            # Dismiss with the filter values
            self.dismiss(
                [
                    kill_by_username,
                    kill_by_host,
                    kill_by_age_range,
                    age_range_lower_limit,
                    age_range_upper_limit,
                    kill_by_query_text,
                    checkbox_sleeping_queries,
                ]
            )

        elif self.command in [HotkeyCommands.thread_kill_by_id, HotkeyCommands.show_thread]:
            value = next((thread_id for thread_id in self.processlist_data.keys() if modal_input == thread_id), None)

            if not value:
                self.update_error_response(f"Thread ID [bold red]{modal_input}[/bold red] does not exist")
            else:
                self.dismiss(modal_input)
        elif self.command == HotkeyCommands.refresh_interval:
            try:
                modal_input = float(modal_input)
            except ValueError:
                self.update_error_response("Input must be a number")
                return

            if modal_input <= 0:
                self.update_error_response("Input must be greater than 0")
                return

            self.dismiss(modal_input)
        elif self.command == HotkeyCommands.replay_seek:
            if not re.search(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", modal_input):
                self.update_error_response("Invalid timestamp format")
                return

            self.dismiss(modal_input)
        else:
            self.dismiss(modal_input)

    def update_error_response(self, message):
        error_response = self.query_one("#error_response", Static)
        error_response.display = True
        error_response.update(message)

    def on_input_submitted(self):
        if self.command not in [HotkeyCommands.thread_filter, HotkeyCommands.thread_kill_by_parameter]:
            self.query_one("#submit", Button).press()
