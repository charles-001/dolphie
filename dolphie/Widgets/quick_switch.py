from textual import on
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Input, Label
from textual_autocomplete import AutoComplete, Dropdown, DropdownItem


class QuickSwitchHostModal(ModalScreen):
    CSS = """
        QuickSwitchHostModal > Vertical {
            background: #121626;
            border: thick #212941;
            height: auto;
            width: 70;
        }
        QuickSwitchHostModal > Vertical > * {
            width: auto;
            height: auto;
            align: center middle;
        }
        QuickSwitchHostModal Label {
            text-style: bold;
            width: 100%;
            content-align: center middle;
            padding-bottom: 1;
        }
        QuickSwitchHostModal Input {
            width: 100% !important;
            content-align: center middle;
        }
        QuickSwitchHostModal .main_container {
            width: 100%;
            content-align: center middle;
        }
        QuickSwitchHostModal AutoComplete {
            width: 100%;
            height: auto;
        }
    """
    BINDINGS = [
        Binding("escape", "app.pop_screen", "", show=False),
    ]

    def __init__(self, quick_switch_hosts):
        super().__init__()

        self.dropdown_items = []
        if quick_switch_hosts:
            self.dropdown_items = [DropdownItem(id) for id in sorted(quick_switch_hosts)]

    def compose(self) -> ComposeResult:
        with Vertical():
            with Vertical(classes="main_container"):
                yield Label("Quick Switch Host")
                yield AutoComplete(
                    Input(id="host", placeholder="Start typing to search for a host"),
                    Dropdown(id="dropdown_items", items=self.dropdown_items),
                )
                yield Input(id="password", placeholder="Password (empty for current)", password=True)
            with Horizontal(classes="button_container"):
                yield Button("Submit", id="submit", variant="primary")
                yield Button("Cancel", id="cancel")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "submit":
            host = self.query_one("#host", Input)
            password = self.query_one("#password", Input)

            if host.value:
                self.dismiss({"host": host.value, "password": password.value})
        else:
            self.app.pop_screen()

    @on(Input.Submitted, "#password")
    def on_input_submitted(self):
        self.query_one("#submit", Button).press()
