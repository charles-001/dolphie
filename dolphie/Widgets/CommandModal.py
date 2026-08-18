from __future__ import annotations

import re
from collections.abc import Mapping

from textual import on
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Checkbox, Input, Label, Rule, Select, Static

from dolphie.DataTypes import BaseProcesslistThread, ConnectionSource, HotkeyCommands
from dolphie.Modules.Functions import is_valid_integer_filter, parse_filter
from dolphie.Widgets.AutoComplete import AutoComplete, DropdownItem, TargetState


class FilterAutoComplete(AutoComplete):
    """AutoComplete that ignores a leading ! so suggestions still work when excluding a value."""

    def get_search_string(self, target_state: TargetState) -> str:
        value, _ = parse_filter(super().get_search_string(target_state))

        return value

    def apply_completion(self, value: str, state: TargetState) -> None:
        _, negate = parse_filter(state.text)

        super().apply_completion(f"!{value}" if negate else value, state)


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
                }
            }

            & Input {
                width: 40;
            }
            & Label {
                width: 100%;
                content-align: center middle;
                padding-bottom: 1;
            }

            & Rule {
                width: 100%;
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
        command: str,
        message: str,
        connection_source: str | None = None,
        processlist_data: Mapping[int, BaseProcesslistThread] | Mapping[str, BaseProcesslistThread] | None = None,
        maximize_panel_options: list[tuple[str, str]] | None = None,
        host_cache_data: Mapping[str, str] | None = None,
        max_replay_timestamp: str | None = None,
        current_filters: Mapping[str, str | int | None] | None = None,
        filter_dropdown_values: Mapping[str, set] | None = None,
    ):
        super().__init__()
        self.command = command
        self.message = message
        self.connection_source = connection_source
        self.processlist_data = {str(thread_id): thread for thread_id, thread in (processlist_data or {}).items()}
        self.host_cache_data = host_cache_data or {}
        self.max_replay_timestamp = max_replay_timestamp
        self.current_filters = current_filters or {}
        self.filter_dropdown_values = filter_dropdown_values or {}

        self.dropdown_items = []
        if self.processlist_data:
            sorted_keys = sorted(self.processlist_data, key=int)
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
                    yield Select(options=self.maximize_panel_select_options, id="maximize_panel_select")
                    yield Label("[b]Note[/b]: Press [b][$yellow]ESC[/b][/$yellow] to exit maximized panel")
                with Vertical(id="filter_container", classes="command_container"):
                    yield filter_by_username_input
                    yield filter_by_host_input
                    yield filter_by_db_input
                    yield filter_by_hostgroup_input
                    yield FilterAutoComplete(
                        filter_by_username_input, id="filter_by_username_dropdown_items", candidates=[]
                    )
                    yield FilterAutoComplete(filter_by_host_input, id="filter_by_host_dropdown_items", candidates=[])
                    yield FilterAutoComplete(filter_by_db_input, id="filter_by_db_dropdown_items", candidates=[])
                    yield FilterAutoComplete(
                        filter_by_hostgroup_input, id="filter_by_hostgroup_dropdown_items", candidates=[]
                    )

                    yield Input(id="filter_by_query_time_input")
                    yield Input(id="filter_by_query_text_input")
                    yield Label("[$dark_gray][b]Note:[/b] Prefix a value with [b]![/b] to exclude what it matches")
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
        maximize_panel_select = self.query_one("#maximize_panel_select", Select)
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
                "user", include_filtered_out=True
            )
            self.query_one("#filter_by_host_input", Input).border_title = "Host/IP"
            self.query_one("#filter_by_host_dropdown_items", AutoComplete).candidates = self.create_dropdown_items(
                "host", include_filtered_out=True
            )
            self.query_one("#filter_by_db_input", Input).border_title = "Database"
            self.query_one("#filter_by_db_dropdown_items", AutoComplete).candidates = self.create_dropdown_items(
                "db", include_filtered_out=True
            )
            self.query_one(
                "#filter_by_query_time_input", Input
            ).border_title = "Minimum Query Time [$dark_gray](seconds)"
            self.query_one(
                "#filter_by_query_text_input", Input
            ).border_title = "Partial Query Text [$dark_gray](case-sensitive)"

            if self.connection_source != ConnectionSource.proxysql:
                self.query_one("#filter_by_hostgroup_input", Input).display = False
            else:
                self.query_one("#filter_by_host_input", Input).border_title = "Backend Host/IP"
                self.query_one("#filter_by_hostgroup_input", Input).border_title = "Hostgroup"
                self.query_one(
                    "#filter_by_hostgroup_dropdown_items", AutoComplete
                ).candidates = self.create_dropdown_items("hostgroup", include_filtered_out=True)

            # Show the filters in effect so they can be changed/removed instead of retyped
            for field, filter_value in self.current_filters.items():
                if filter_value:
                    filter_input = self.query_one(f"#filter_by_{field}_input", Input)
                    filter_input.value = str(filter_value)
                    filter_input.cursor_position = len(filter_input.value)
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
            self.query_one(
                "#kill_by_query_text_input", Input
            ).border_title = "Partial Query Text [$dark_gray](case-sensitive)"

            sleeping_queries_checkbox = self.query_one("#sleeping_queries", Checkbox)
            sleeping_queries_checkbox.toggle()

            input.placeholder = "Select an option from above"
        elif self.command == HotkeyCommands.maximize_panel:
            input.display = False
            maximize_panel_container.display = True
            maximize_panel_select.border_title = "Select a Panel"
        elif self.command == HotkeyCommands.rename_tab:
            input.border_title = "Tab Name"
            input.styles.width = 50
            input.focus()
        elif self.command == HotkeyCommands.variable_search:
            input.border_title = "Variable Name"
            input.placeholder = "Input 'all' to show everything"
            input.focus()
        elif self.command in [HotkeyCommands.show_thread]:
            input.border_title = "Thread ID"
            input.focus()
        elif self.command == HotkeyCommands.refresh_interval:
            input.border_title = "Refresh Interval [$dark_gray](seconds)"
            input.focus()
        elif self.command == HotkeyCommands.replay_seek:
            if self.max_replay_timestamp:
                input.value = self.max_replay_timestamp
            input.border_title = "Timestamp"
            input.placeholder = "Format: 2024-07-25 13:00:00"
            input.focus()
        else:
            input.focus()

    def create_dropdown_items(self, field, include_filtered_out=False):
        dropdown_items = []

        if field:
            # Filter out None values before sorting
            values = {
                getattr(thread, field)
                for thread in self.processlist_data.values()
                if getattr(thread, field) is not None
            }

            # The processlist only has what the filters in effect allow, so include the values
            # they're hiding to keep every option available to filter by
            if include_filtered_out:
                values.update(self.filter_dropdown_values.get(field, set()))

            dropdown_items = [DropdownItem(str(value)) for value in sorted(values)]

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
                host, negate = parse_filter(filters["host"])
                host = next((ip for ip, addr in self.host_cache_data.items() if host == addr), host)
                filters["host"] = f"!{host}" if negate else host

            # Query time is a minimum, so excluding a value from it doesn't mean anything
            if filters["query_time"].startswith("!"):
                self.update_error_response("Query time doesn't support [b]![/b] exclusion")
                return

            # Validate numeric fields (hostgroup can be prefixed with ! to exclude it)
            for value, field_name, allow_negation in [
                (filters["query_time"], "Query time", False),
                (filters["hostgroup"], "Hostgroup", True),
            ]:
                if value and not is_valid_integer_filter(value, allow_negation=allow_negation):
                    self.update_error_response(f"{field_name} must be an integer")
                    return

            # Ensure at least one filter is provided, unless there are filters in effect to remove
            if not any(filters.values()) and not any(self.current_filters.values()):
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
            if maximize_panel == Select.NULL:
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
