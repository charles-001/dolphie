import re

from textual import on
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Checkbox, Input, Label, Rule, Select, Static

from dolphie.DataTypes import ConnectionSource, HotkeyCommands
from dolphie.Widgets.AutoComplete import AutoComplete, DropdownItem


class CommandModal(ModalScreen):
    CSS = """
        CommandModal {
            & > Vertical {
                background: #131626;
                border: tall #384673;
                height: auto;
                width: auto;

                & > * {
                    width: auto;
                    height: auto;
                    content-align: center middle;
                }
            }

            & .command_container {
                width: auto;
                height: auto;

                & Input, Select {
                    width: 60;
                    border-title-color: #d2d2d2;
                }
            }

            & Label {
                width: 100%;
                content-align: center middle;
                padding-bottom: 1;
            }

            & Rule {
                width: 100%;
                margin-bottom: 1;
            }

            & #error_response {
                color: #fe5c5c;
                width: 100%;
                height: auto;
                content-align: center middle;
                padding-bottom: 1;
            }

            & Checkbox {
                background: #131626;
                border: none;
                content-align: center middle;
                padding-top: 1;
                width: 100%;
            }

            & #sleeping_queries {
                padding-bottom: 1;
            }
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
        maximize_panel_options=None,
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

        self.maximize_panel_select_options = maximize_panel_options or []

    def compose(self) -> ComposeResult:
        with Vertical():
            with Vertical():
                yield Label(f"[b]{self.message}[/b]")

                modal_input = Input(id="modal_input")
                filter_by_username_input = Input(id="filter_by_username_input")
                filter_by_host_input = Input(id="filter_by_host_input")
                filter_by_db_input = Input(id="filter_by_db_input")
                filter_by_hostgroup_input = Input(id="filter_by_hostgroup_input")
                kill_by_id_input = Input(id="kill_by_id_input")
                kill_by_username_input = Input(id="kill_by_username_input")
                kill_by_host_input = Input(id="kill_by_host_input")

                with Vertical(id="maximize_panel_container", classes="command_container"):
                    yield Select(
                        options=self.maximize_panel_select_options, id="maximize_panel_select", prompt="Select a Panel"
                    )
                    yield Label("[b]Note[/b]: Press [b][$yellow]ESC[/b][/$yellow] to exit maximized panel")
                with Vertical(id="filter_container", classes="command_container"):
                    yield filter_by_username_input
                    yield filter_by_host_input
                    yield filter_by_db_input
                    yield filter_by_hostgroup_input
                    yield AutoComplete(filter_by_username_input, id="filter_by_username_dropdown_items", candidates=[])
                    yield AutoComplete(filter_by_host_input, id="filter_by_host_dropdown_items", candidates=[])
                    yield AutoComplete(filter_by_db_input, id="filter_by_db_dropdown_items", candidates=[])
                    yield AutoComplete(
                        filter_by_hostgroup_input,
                        id="filter_by_hostgroup_dropdown_items",
                        candidates=[],
                    )

                    yield Input(id="filter_by_query_time_input")
                    yield Input(id="filter_by_query_text_input")
                with Vertical(id="kill_container", classes="command_container"):
                    yield kill_by_id_input
                    yield AutoComplete(kill_by_id_input, id="kill_by_id_dropdown_items", candidates=[])

                    yield Rule(line_style="heavy")

                    yield kill_by_username_input
                    yield kill_by_host_input
                    yield AutoComplete(kill_by_username_input, id="kill_by_username_dropdown_items", candidates=[])
                    yield AutoComplete(kill_by_host_input, id="kill_by_host_dropdown_items", candidates=[])

                    yield Input(id="kill_by_age_range_input", placeholder="Example: 5-8")
                    yield Input(id="kill_by_query_text_input")
                    yield Checkbox("Include sleeping queries", id="sleeping_queries")
                    yield Label(
                        "[$dark_gray][b]Note:[/b] Only threads visible and executing (or sleeping)\n"
                        "in the Processlist panel can be killed in this section"
                    )

                yield modal_input
                yield AutoComplete(
                    modal_input, id="dropdown_items", candidates=self.dropdown_items, prevent_default_enter=False
                )

                yield Static(id="error_response")
            with Horizontal(classes="button_container"):
                yield Button("Submit", id="submit", variant="primary")
                yield Button("Cancel", id="cancel")

    def on_mount(self):
        input = self.query_one("#modal_input", Input)
        maximize_panel_container = self.query_one("#maximize_panel_container", Vertical)
        filter_container = self.query_one("#filter_container", Vertical)
        kill_container = self.query_one("#kill_container", Vertical)
        self.query_one("#error_response", Static).display = False

        maximize_panel_container.display = False
        filter_container.display = False
        kill_container.display = False

        if self.command == HotkeyCommands.thread_filter:
            input.display = False
            filter_container.display = True

            self.query_one("#filter_by_username_input", Input).focus()
            self.query_one("#filter_by_username_input", Input).border_title = "Username"
            self.query_one("#filter_by_username_dropdown_items", AutoComplete).candidates = self.create_dropdown_items(
                "user"
            )
            self.query_one("#filter_by_host_input", Input).border_title = "Host/IP"
            self.query_one("#filter_by_host_dropdown_items", AutoComplete).candidates = self.create_dropdown_items(
                "host"
            )
            self.query_one("#filter_by_db_input", Input).border_title = "Database"
            self.query_one("#filter_by_db_dropdown_items", AutoComplete).candidates = self.create_dropdown_items("db")
            self.query_one("#filter_by_query_time_input", Input).border_title = (
                "Minimum Query Time [$dark_gray](seconds)"
            )
            self.query_one("#filter_by_query_text_input", Input).border_title = (
                "Partial Query Text [$dark_gray](case-sensitive)"
            )

            if self.connection_source != ConnectionSource.proxysql:
                self.query_one("#filter_by_hostgroup_input", Input).display = False
            else:
                self.query_one("#filter_by_host_input", Input).border_title = "Backend Host/IP"
                self.query_one("#filter_by_hostgroup_input", Input).border_title = "Hostgroup"
                self.query_one("#filter_by_hostgroup_dropdown_items", AutoComplete).candidates = (
                    self.create_dropdown_items("hostgroup")
                )
        elif self.command == HotkeyCommands.thread_kill_by_parameter:
            input.display = False
            kill_container.display = True

            self.query_one("#kill_by_id_input", Input).focus()
            self.query_one("#kill_by_id_dropdown_items", AutoComplete).candidates = self.dropdown_items
            self.query_one("#kill_by_id_input", Input).border_title = "Thread ID [$dark_gray](enter submits)"
            self.query_one("#kill_by_username_input", Input).border_title = "Username"
            self.query_one("#kill_by_username_dropdown_items", AutoComplete).candidates = self.create_dropdown_items(
                "user"
            )
            self.query_one("#kill_by_host_input", Input).border_title = "Host/IP"
            self.query_one("#kill_by_host_dropdown_items", AutoComplete).candidates = self.create_dropdown_items("host")
            self.query_one("#kill_by_age_range_input", Input).border_title = "Age Range [$dark_gray](seconds)"
            self.query_one("#kill_by_query_text_input", Input).border_title = (
                "Partial Query Text [$dark_gray](case-sensitive)"
            )

            sleeping_queries_checkbox = self.query_one("#sleeping_queries", Checkbox)
            sleeping_queries_checkbox.toggle()

            input.placeholder = "Select an option from above"
        elif self.command == HotkeyCommands.maximize_panel:
            input.display = False
            maximize_panel_container.display = True
        elif self.command == HotkeyCommands.rename_tab:
            input.placeholder = "Colors can be added by wrapping them in []"
            input.styles.width = 50
            input.focus()
        elif self.command == HotkeyCommands.variable_search:
            input.placeholder = "Input 'all' to show everything"
            input.focus()
        elif self.command in [HotkeyCommands.show_thread]:
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
            HotkeyCommands.maximize_panel,
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
                    (ip for ip, addr in self.host_cache_data.candidates() if filters["host"] == addr), filters["host"]
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
            kill_by_id = self.query_one("#kill_by_id_input", Input).value
            kill_by_username = self.query_one("#kill_by_username_input", Input).value
            kill_by_host = self.query_one("#kill_by_host_input", Input).value
            kill_by_age_range = self.query_one("#kill_by_age_range_input", Input).value
            kill_by_query_text = self.query_one("#kill_by_query_text_input", Input).value
            checkbox_sleeping_queries = self.query_one("#sleeping_queries", Checkbox).value

            age_range_lower_limit, age_range_upper_limit = None, None

            if kill_by_id and not kill_by_id.isdigit():
                self.update_error_response("Thread ID must be a number")
                return

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

            # Ensure thread ID or at least one parameter is provided
            if not any([kill_by_id, kill_by_username, kill_by_host, kill_by_age_range, kill_by_query_text]):
                self.update_error_response("Thread ID or at least one parameter must be provided")
                return

            # Dismiss with the filter values
            self.dismiss(
                [
                    kill_by_id,
                    kill_by_username,
                    kill_by_host,
                    kill_by_age_range,
                    age_range_lower_limit,
                    age_range_upper_limit,
                    kill_by_query_text,
                    checkbox_sleeping_queries,
                ]
            )

        elif self.command in {HotkeyCommands.show_thread}:
            if modal_input not in self.processlist_data:
                self.update_error_response(f"Thread ID [b]{modal_input}[/b] does not exist")
                return

            if not modal_input.isdigit():
                self.update_error_response("Thread ID must be a number")
                return

            self.dismiss(modal_input)

        elif self.command == HotkeyCommands.refresh_interval:
            try:
                # Convert input to float and check if it's a number at same time
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
        elif self.command == HotkeyCommands.maximize_panel:
            maximize_panel = self.query_one("#maximize_panel_select", Select).value
            if maximize_panel == Select.BLANK:
                self.update_error_response("Please select a panel to maximize")
                return

            self.dismiss(maximize_panel)
        else:
            self.dismiss(modal_input)

    def update_error_response(self, message):
        error_response = self.query_one("#error_response", Static)
        error_response.display = True
        error_response.update(message)

    @on(Input.Submitted, "Input")
    def on_input_submitted(self, event: Input.Submitted):
        if self.command not in [HotkeyCommands.thread_filter, HotkeyCommands.thread_kill_by_parameter]:
            self.query_one("#submit", Button).press()

    @on(Input.Submitted, "#kill_by_id_input")
    def on_kill_by_id_input_submitted(self, event: Input.Submitted):
        self.query_one("#submit", Button).press()
