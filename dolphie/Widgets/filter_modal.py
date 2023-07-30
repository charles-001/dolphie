from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Input, Label, RadioButton, RadioSet


class FilterModal(ModalScreen):
    CSS = """
        
    """

    BINDINGS = [
        Binding("escape", "app.pop_screen", "", show=False),
    ]

    def __init__(self):
        super().__init__()

    def compose(self) -> ComposeResult:
        with Vertical():
            with Vertical():
                yield Label("Select which filter you'd like to apply")
                with Horizontal():
                    with RadioSet():
                        yield RadioButton("Database", id="database")
                        yield RadioButton("Host/IP", id="host")
                        yield RadioButton("Query Text", id="query_text")
                        yield RadioButton("Query Time", id="query_time")
                        yield RadioButton("User", id="user")
                yield Input(id="filter_input")
            with Horizontal(id="buttons"):
                yield Button("Submit", id="submit", variant="primary")
                yield Button("Cancel", id="cancel")

    def on_input_submitted(self, event):
        if self.variable:
            self.dismiss([self.variable, event.value])
        else:
            self.dismiss(event.value)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "submit":
            filter_input = self.query_one("#filter_input")

            if self.variable:
                self.dismiss([self.variable, filter_input.value])
            else:
                self.dismiss(filter_input.value)
        else:
            self.app.pop_screen()
