from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Input, Label, RadioButton, RadioSet


class CommandPopup(ModalScreen):
    BINDINGS = [
        Binding("escape", "app.pop_screen", "", show=False),
    ]

    def __init__(self, message, variable=None, show_filter_options=False):
        super().__init__()
        self.variable = variable
        self.message = message
        self.show_filter_options = show_filter_options

    def compose(self) -> ComposeResult:
        with Vertical():
            with Vertical():
                yield Label(self.message)
                with RadioSet(id="radio_buttons"):
                    yield RadioButton("Database", id="database")
                    yield RadioButton("Host/IP", id="host")
                    yield RadioButton("Query Text", id="query_text")
                    yield RadioButton("Query Time", id="query_time")
                    yield RadioButton("User", id="user")
                yield Input(id="popup_input")

            with Horizontal(id="buttons"):
                yield Button("Submit", id="submit", variant="primary")
                yield Button("Cancel", id="cancel")

    def on_mount(self):
        input = self.query_one("#popup_input")
        input.focus()

        if not self.show_filter_options:
            radio_buttons = self.query_one("#radio_buttons")
            radio_buttons.display = False

    def on_input_submitted(self):
        self.query_one("#submit").press()

    def on_radio_set_changed(self, event: RadioSet.Changed) -> None:
        popup_input = self.query_one("#popup_input")
        if event.pressed.id == "database":
            popup_input.placeholder = "Database name"
        elif event.pressed.id == "host":
            popup_input.placeholder = "Hostname or IP address"
        elif event.pressed.id == "query_text":
            popup_input.placeholder = "Partial query text"
        elif event.pressed.id == "query_time":
            popup_input.placeholder = "Query time (in seconds)"
        elif event.pressed.id == "user":
            popup_input.placeholder = "Username"

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "submit":
            popup_input = self.query_one("#popup_input")

            if self.variable:
                self.dismiss([self.variable, popup_input.value])
            elif self.show_filter_options:
                for rb in self.query(RadioButton):
                    if rb.value:
                        self.dismiss([rb.id, popup_input.value])
            else:
                self.dismiss(popup_input.value)
        else:
            self.app.pop_screen()
