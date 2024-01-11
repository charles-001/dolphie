from textual import on
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Input, Label, Static
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
        QuickSwitchHostModal #modal_footer {
            color: #d3565c;
            width: 100%;
            padding-bottom: 0;
            padding-top: 1;
            margin: 0 2;
        }
    """
    BINDINGS = [
        Binding("escape", "app.pop_screen", "", show=False),
    ]

    def __init__(self, host, port, quick_switch_hosts, error_message=None):
        super().__init__()

        self.host = host
        self.port = port

        if self.host and self.port:
            self.host = f"{self.host}:{self.port}"

        self.dropdown_items = []
        if quick_switch_hosts:
            self.dropdown_items = [DropdownItem(id) for id in sorted(quick_switch_hosts)]

        self.error_message = error_message

    def on_mount(self) -> None:
        footer = self.query_one("#modal_footer", Static)

        footer.display = False
        if self.error_message:
            footer.update(self.error_message)
            footer.display = True

    def compose(self) -> ComposeResult:
        with Vertical():
            with Vertical(classes="main_container"):
                yield Label("Quick Switch Host")
                yield AutoComplete(
                    Input(value=self.host, id="host", placeholder="Start typing to search for a host"),
                    Dropdown(id="dropdown_items", items=self.dropdown_items),
                )
                yield Input(id="password", placeholder="Password (empty for current)", password=True)
            with Horizontal(classes="button_container"):
                yield Button("Submit", id="submit", variant="primary")
                yield Button("Cancel", id="cancel")
            yield Label(id="modal_footer")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "submit":
            host = self.query_one("#host", Input)
            password = self.query_one("#password", Input)

            if not host.value:
                self.query_one("#modal_footer", Static).update("Host cannot be empty")
                self.query_one("#modal_footer", Static).display = True
                return

            self.dismiss({"host": host.value, "password": password.value})
        else:
            self.app.pop_screen()

    @on(Input.Submitted, "#password")
    def on_input_submitted(self):
        self.query_one("#submit", Button).press()
