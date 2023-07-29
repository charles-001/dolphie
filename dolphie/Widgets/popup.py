from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Input, Label


class CommandPopup(ModalScreen):
    CSS = """
        CommandPopup > Vertical {
            background: #181e2e;
            border: thick #212941;
            height: auto;
            width: auto;
        }

        CommandPopup > Vertical > * {
            width: auto;
            height: auto;
        }

        CommandPopup Input {
            width: 40;
            margin: 1 2;
            background: #181e2e;
            border: solid #94a9e3;
        }

        CommandPopup Button {
            margin-right: 1;
        }

        CommandPopup Label {
            text-style: bold;
            width: 100%;
            content-align: center middle;
        }

        CommandPopup #buttons {
            width: 100%;
            align-horizontal: center;
        }
    """

    BINDINGS = [
        Binding("escape", "app.pop_screen", "", show=False),
    ]

    def __init__(self, message, variable=None):
        super().__init__()
        self.variable = variable
        self.message = message

    def compose(self) -> ComposeResult:
        with Vertical():
            with Vertical():
                yield Label(self.message)
                yield Input(id="popup_input")
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
            popup_input = self.query_one("#popup_input")

            if self.variable:
                self.dismiss([self.variable, popup_input.value])
            else:
                self.dismiss(popup_input.value)
        else:
            self.app.pop_screen()
