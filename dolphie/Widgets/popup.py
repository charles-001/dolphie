# from textual.containers import Container, Horizontal
# from textual.message import Message
# from textual.widgets import Button, Input

from textual.app import ComposeResult
from textual.containers import Grid
from textual.screen import ModalScreen
from textual.widgets import Button, Input, Label


class CommandPopup(ModalScreen):
    def __init__(self, popup):
        super().__init__()
        self.popup = popup

    def compose(self) -> ComposeResult:
        yield Grid(
            Label(self.popup, id="popup_message"),
            Input(id="popup_input"),
            Grid(
                Button("Submit", variant="primary", id="submit"),
                Button("Cancel", variant="default", id="cancel"),
                id="popup_button_grid",
            ),
            id="popup",
        )

    def on_input_submitted(self, event):
        self.dismiss(event.value)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        # if event.button.id == "submit":

        self.app.pop_screen()
