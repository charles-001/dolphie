from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Checkbox, Input, Label, RadioButton, RadioSet
from textual_autocomplete import AutoComplete, Dropdown, DropdownItem


class CommandModal(ModalScreen):
    CSS = """
        CommandModal > Vertical {
            background: #121626;
            border: thick #20263d;
            height: auto;
            width: auto;
        }
        CommandModal > Vertical > * {
            width: auto;
            height: auto;
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
    """
    BINDINGS = [
        Binding("escape", "app.pop_screen", "", show=False),
    ]

    def __init__(
        self, message, variable=None, processlist_data=None, show_filter_options=False, show_kill_options=False
    ):
        super().__init__()
        self.variable = variable
        self.message = message
        self.show_filter_options = show_filter_options
        self.show_kill_options = show_kill_options
        self.processlist_data = processlist_data

        self.dropdown_items = []
        if processlist_data and (not show_filter_options and not show_kill_options):
            sorted_keys = sorted(processlist_data.keys(), key=lambda x: int(x))
            self.dropdown_items = [DropdownItem(id) for id in sorted_keys]

    def compose(self) -> ComposeResult:
        with Vertical():
            with Vertical():
                yield Label(self.message)
                with RadioSet(id="filter_radio_buttons"):
                    yield RadioButton("User", id="user")
                    yield RadioButton("Host/IP", id="host")
                    yield RadioButton("Database", id="database")
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
            with Horizontal(classes="button_container"):
                yield Button("Submit", id="submit", variant="primary")
                yield Button("Cancel", id="cancel")

    def on_mount(self):
        input = self.query_one("#modal_input", Input)
        filter_radio_buttons = self.query_one("#filter_radio_buttons", RadioSet)
        kill_container = self.query_one("#kill_container", Vertical)

        filter_radio_buttons.display = False
        kill_container.display = False

        if self.show_filter_options:
            filter_radio_buttons.display = True
            filter_radio_buttons.focus()

            input.placeholder = "Select an option from above"
        elif self.show_kill_options:
            sleeping_queries_checkbox = self.query_one("#sleeping_queries", Checkbox)
            sleeping_queries_checkbox.toggle()

            kill_container.display = True

            kill_radio_buttons = self.query_one("#kill_radio_buttons", RadioSet)
            kill_radio_buttons.focus()

            input.placeholder = "Select an option from above"
        else:
            input.focus()

    def on_input_submitted(self):
        self.query_one("#submit", Button).press()

    def create_dropdown_items(self, field):
        self.dropdown_items = []

        if field:
            sorted_array = sorted(set(data.get(field) for _, data in self.processlist_data.items()))
            self.dropdown_items = [DropdownItem(value) for value in sorted_array]

        self.query_one("#dropdown_items", Dropdown).items = self.dropdown_items

    def on_radio_set_changed(self, event: RadioSet.Changed) -> None:
        modal_input = self.query_one("#modal_input", Input)

        self.create_dropdown_items(None)  # empty string to clear dropdown items

        if self.show_filter_options:
            if event.pressed.id == "database":
                self.create_dropdown_items("db")
                modal_input.placeholder = "Database name"
            elif event.pressed.id == "host":
                self.create_dropdown_items("host")
                modal_input.placeholder = "Hostname or IP address"
            elif event.pressed.id == "query_text":
                modal_input.placeholder = "Partial query text"
            elif event.pressed.id == "query_time":
                modal_input.placeholder = "Query time (in seconds)"
            elif event.pressed.id == "user":
                self.create_dropdown_items("user")
                modal_input.placeholder = "Username"
        elif self.show_kill_options:
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
        if event.button.id == "submit":
            modal_input = self.query_one("#modal_input", Input)

            if self.variable:
                self.dismiss([self.variable, modal_input.value])
            elif self.show_filter_options:
                for rb in self.query("#filter_radio_buttons RadioButton"):
                    if rb.value:
                        self.dismiss([rb.id, modal_input.value])
            elif self.show_kill_options:
                checkbox_sleeping_queries = self.query_one("#sleeping_queries", Checkbox)
                for rb in self.query("#kill_container RadioButton"):
                    if rb.value:
                        self.dismiss([rb.id, modal_input.value, checkbox_sleeping_queries.value])
            else:
                self.dismiss(modal_input.value)
        else:
            self.app.pop_screen()
