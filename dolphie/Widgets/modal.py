import re

from dolphie.DataTypes import HotkeyCommands
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import (
    Button,
    Checkbox,
    Input,
    Label,
    RadioButton,
    RadioSet,
    Static,
)
from textual_autocomplete import AutoComplete, Dropdown, DropdownItem


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
        }
        CommandModal #filter_radio_buttons {
            margin-bottom: 1;
        }
        CommandModal #kill_container {
            width: 100%;
            height: 6;
        }
        CommandModal Label {
            text-style: bold;
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
    """
    BINDINGS = [
        Binding("escape", "app.pop_screen", "", show=False),
    ]

    def __init__(self, command, message, processlist_data=None, host_cache_data=None):
        super().__init__()
        self.command = command
        self.message = message
        self.processlist_data = processlist_data
        self.host_cache_data = host_cache_data

        self.dropdown_items = []
        if processlist_data:
            sorted_keys = sorted(processlist_data.keys(), key=lambda x: int(x))
            self.dropdown_items = [DropdownItem(thread_id) for thread_id in sorted_keys]

    def compose(self) -> ComposeResult:
        with Vertical():
            with Vertical():
                yield Label(self.message)
                with RadioSet(id="filter_radio_buttons"):
                    yield RadioButton("User", id="user")
                    yield RadioButton("Host/IP", id="host")
                    yield RadioButton("Database", id="db")
                    yield RadioButton("Query Text", id="query_text")
                    yield RadioButton("Query Time", id="query_time")
                with Vertical(id="kill_container"):
                    with RadioSet(id="kill_radio_buttons"):
                        yield RadioButton("Username", id="username")
                        yield RadioButton("Host/IP", id="host")
                        yield RadioButton("Time range", id="time_range")
                    yield Checkbox("Include sleeping queries", id="sleeping_queries")
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
        filter_radio_buttons = self.query_one("#filter_radio_buttons", RadioSet)
        kill_container = self.query_one("#kill_container", Vertical)
        self.query_one("#error_response", Static).display = False

        filter_radio_buttons.display = False
        kill_container.display = False

        if self.command == HotkeyCommands.thread_filter:
            filter_radio_buttons.display = True
            filter_radio_buttons.focus()

            input.placeholder = "Select an option from above"
        elif self.command == HotkeyCommands.thread_kill_by_parameter:
            sleeping_queries_checkbox = self.query_one("#sleeping_queries", Checkbox)
            sleeping_queries_checkbox.toggle()

            kill_container.display = True

            kill_radio_buttons = self.query_one("#kill_radio_buttons", RadioSet)
            kill_radio_buttons.focus()

            input.placeholder = "Select an option from above"
        elif self.command == HotkeyCommands.rename_tab:
            input.placeholder = "Colors can be added by wrapping them in []"
            input.styles.width = 50
            input.focus()
        elif self.command == HotkeyCommands.variable_search:
            input.placeholder = "Input 'all' to show everything"
            input.focus()
        elif self.command in [HotkeyCommands.show_thread, HotkeyCommands.thread_kill_by_id]:
            input.placeholder = "Input a Process ID"
            input.focus()
        elif self.command == HotkeyCommands.refresh_interval:
            input.placeholder = "Input a refresh interval (seconds)"
            input.focus()
        else:
            input.focus()

    def on_input_submitted(self):
        self.query_one("#submit", Button).press()

    def create_dropdown_items(self, field):
        self.dropdown_items = []

        if field:
            sorted_array = sorted(set(getattr(thread, field) for thread in self.processlist_data.values()))
            self.dropdown_items = [DropdownItem(value) for value in sorted_array]

        self.query_one("#dropdown_items", Dropdown).items = self.dropdown_items

    def on_radio_set_changed(self, event: RadioSet.Changed) -> None:
        modal_input = self.query_one("#modal_input", Input)

        self.create_dropdown_items(None)  # empty string to clear dropdown items

        if self.command == HotkeyCommands.thread_filter:
            if event.pressed.id == "db":
                self.create_dropdown_items("db")
                modal_input.placeholder = "Database name"
            elif event.pressed.id == "host":
                self.create_dropdown_items("host")
                modal_input.placeholder = "Hostname or IP address"
            elif event.pressed.id == "query_text":
                modal_input.placeholder = "Partial query text"
            elif event.pressed.id == "query_time":
                modal_input.placeholder = "Minimum query time (in seconds)"
            elif event.pressed.id == "user":
                self.create_dropdown_items("user")
                modal_input.placeholder = "Username"
        elif self.command == HotkeyCommands.thread_kill_by_parameter:
            if event.pressed.id == "username":
                self.create_dropdown_items("user")
                modal_input.placeholder = "Username"
            elif event.pressed.id == "host":
                self.create_dropdown_items("host")
                modal_input.placeholder = "Hostname or IP address"
            elif event.pressed.id == "time_range":
                modal_input.placeholder = "Time range (ex. 10-20)"

        modal_input.focus()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id != "submit":
            self.app.pop_screen()
            return

        modal_input = self.query_one("#modal_input", Input).value
        if not modal_input:
            self.update_error_response("Input cannot be empty")
            return

        if self.command == HotkeyCommands.thread_filter:
            filter_options = {
                "user": "User",
                "db": "Database",
                "host": "Host",
                "query_time": "Query time",
                "query_text": "Query text",
            }

            for rb_id, label in filter_options.items():
                if self.query_one(f"#filter_radio_buttons #{rb_id}", RadioButton).value:
                    filter_id = rb_id
                    filter_label = label
                    break

            if filter_label:
                if filter_id == "query_time":
                    if not modal_input.isdigit():
                        self.update_error_response("Query time must be an integer")
                    else:
                        self.dismiss([filter_label, int(modal_input)])
                elif filter_id == "query_text":
                    self.dismiss([filter_label, modal_input])
                else:
                    if filter_id == "host":
                        value = next((ip for ip, addr in self.host_cache_data.items() if modal_input == addr), None)
                        modal_input = value
                    else:
                        value = next(
                            (
                                getattr(data, filter_id)
                                for data in self.processlist_data.values()
                                if modal_input == getattr(data, filter_id)
                            ),
                            None,
                        )

                    self.dismiss([filter_label, modal_input])
            else:
                self.update_error_response("Please select a filter option")
        elif self.command == HotkeyCommands.thread_kill_by_parameter:
            kill_type = None
            lower_limit = None
            upper_limit = None

            checkbox_sleeping_queries = self.query_one("#sleeping_queries", Checkbox).value
            for rb in self.query("#kill_container RadioButton"):
                if rb.value:
                    kill_type = rb.id
                    break

            if kill_type:
                if kill_type == "time_range":
                    if re.search(r"(\d+-\d+)", modal_input):
                        time_range = modal_input.split("-")
                        lower_limit = int(time_range[0])
                        upper_limit = int(time_range[1])

                        if lower_limit > upper_limit:
                            self.update_error_response("Invalid time range! Lower limit can't be higher than upper")
                            return
                    else:
                        self.update_error_response("Invalid time range")
                        return

                self.dismiss([rb.id, modal_input, checkbox_sleeping_queries, lower_limit, upper_limit])
            else:
                self.update_error_response("Please select a kill option")
        elif self.command in [HotkeyCommands.thread_kill_by_id, HotkeyCommands.show_thread]:
            value = next((thread_id for thread_id in self.processlist_data.keys() if modal_input == thread_id), None)

            if not value:
                self.update_error_response(f"Thread ID [b red]{modal_input}[/b red] does not exist")
            else:
                self.dismiss(modal_input)
        elif self.command == HotkeyCommands.refresh_interval:
            if not modal_input.isnumeric():
                self.update_error_response("Input must be an integer")
                return

            modal_input = int(modal_input)
            if modal_input < 1:
                self.update_error_response("Input must be greater than 0")
                return

            self.dismiss(modal_input)
        else:
            self.dismiss(modal_input)

    def update_error_response(self, message):
        error_response = self.query_one("#error_response", Static)
        error_response.display = True
        error_response.update(message)
