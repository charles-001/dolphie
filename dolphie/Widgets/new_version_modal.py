from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Label


class NewVersionModal(ModalScreen):
    CSS = """
        NewVersionModal > Vertical {
            background: #121626;
            border: thick #20263d;
            height: auto;
            width: auto;
        }
        NewVersionModal > Vertical > * {
            width: auto;
            height: auto;
        }
        NewVersionModal #title {
            text-style: bold;
            width: 100%;
            margin: 0;
        }
        NewVersionModal Label {
            margin: 1 2;
            content-align: center middle;
        }
        NewVersionModal Horizontal {
            width: 100%;
            align-horizontal: center;
        }
    """
    BINDINGS = [
        Binding("escape", "app.pop_screen", "", show=False),
    ]

    def __init__(self, current_version, latest_version):
        super().__init__()

        self.current_version = current_version
        self.latest_version = latest_version

    def compose(self) -> ComposeResult:
        with Vertical():
            yield Label("[green]New version available!", id="title")
            yield Label(f"Current version: [highlight]{self.current_version}")
            yield Label(f"Latest version:  [highlight]{self.latest_version}")
            yield Label("Please update to the latest version at your convenience")
            yield Label("Find more details at [light_blue]https://github.com/charles-001/dolphie")
            with Horizontal():
                yield Button("OK", variant="primary")

    def on_button_pressed(self) -> None:
        self.app.pop_screen()
