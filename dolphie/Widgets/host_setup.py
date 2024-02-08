from textual import on
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Input, Label, Static
from textual_autocomplete import AutoComplete, Dropdown, DropdownItem


class HostSetupModal(ModalScreen):
    CSS = """
        HostSetupModal > Vertical {
            background: #131626;
            border: tall #384673;
            height: auto;
            width: 70;
        }
        HostSetupModal > Vertical > * {
            width: auto;
            height: auto;
            align: center middle;
        }
        HostSetupModal Label {
            text-style: bold;
            width: 100%;
            content-align: center middle;
            padding-bottom: 1;
        }
        HostSetupModal Input {
            width: 100%;
            content-align: center middle;
        }
        HostSetupModal .main_container {
            width: 100%;
            content-align: center middle;
        }
        HostSetupModal AutoComplete {
            width: 100%;
            height: auto;
        }
        HostSetupModal #password {
            width: 84%;
        }
        HostSetupModal #show_password {
            max-width: 12%;
            background:  #262c4b;
            border: blank #344063;
        }
        HostSetupModal #show_password:hover {
            background:  #313960;
            border: blank #344063;
        }
        HostSetupModal #show_password:focus {
            background:  #313960;
            border: blank #344063;
        }
        HostSetupModal #modal_footer {
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

    def __init__(self, host, port, username, password, available_hosts, error_message=None):
        super().__init__()

        self.host = host
        self.port = port
        self.username = username
        self.password = password

        if self.host and self.port:
            self.host = f"{self.host}:{self.port}"

        self.dropdown_items = []
        if available_hosts:
            self.dropdown_items = [DropdownItem(id) for id in sorted(available_hosts)]

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
                yield Label("Host Setup")
                yield AutoComplete(
                    Input(
                        value=self.host,
                        id="host",
                        placeholder="Start typing to search for a host - format is host:port",
                    ),
                    Dropdown(id="dropdown_items", items=self.dropdown_items),
                )
                yield Input(id="username", value=self.username, placeholder="Username")
                with Horizontal():
                    yield Input(id="password", value=self.password, placeholder="Password", password=True)
                    yield Button("Show", id="show_password")
            with Horizontal(classes="button_container"):
                yield Button("Submit", id="submit", variant="primary")
                yield Button("Cancel", id="cancel")
            yield Label(id="modal_footer")

    @on(Button.Pressed, "#show_password")
    def on_show_password_pressed(self, event: Button.Pressed) -> None:
        password = self.query_one("#password", Input)
        show_password_button = self.query_one("#show_password", Button)

        if password.password:
            show_password_button.label = "Hide"
        else:
            show_password_button.label = "Show"

        password.password = not password.password

    @on(Button.Pressed, "#submit")
    def on_submit_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "submit":
            error_message = None

            host = self.query_one("#host", Input)
            username = self.query_one("#username", Input)
            password = self.query_one("#password", Input)

            modal_footer = self.query_one("#modal_footer", Label)
            host_split = host.value.split(":")
            if len(host_split) == 2:
                try:
                    int(host_split[1])
                except ValueError:
                    error_message = "Port must be a valid integer"
            elif len(host_split) > 2:
                error_message = "Host cannot contain more than one colon"

            if not host.value:
                error_message = "Host cannot be empty"

            if error_message:
                modal_footer.update(error_message)
                modal_footer.display = True
                return

            self.dismiss({"host": host.value, "username": username.value, "password": password.value})
        else:
            self.app.pop_screen()

    @on(Input.Submitted, "#password")
    def on_input_submitted(self):
        self.query_one("#submit", Button).press()

    @on(Button.Pressed, "#cancel")
    def on_cancel_pressed(self, event: Button.Pressed) -> None:
        self.app.pop_screen()
