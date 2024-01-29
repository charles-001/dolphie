from dolphie.Widgets.topbar import TopBar
from textual import events
from textual.app import ComposeResult
from textual.containers import Center
from textual.screen import Screen
from textual.widgets import Static


class CommandScreen(Screen):
    CSS = """
        CommandScreen Center {
            padding: 1;
        }
        CommandScreen Center > Static {
            padding-left: 1;
            padding-right: 1;
            background: #0b1221;
            border: tall #1c2238;
            width: auto;
        }
    """

    def __init__(self, read_only, app_version, host, data):
        super().__init__()
        self.read_only = read_only
        self.app_version = app_version
        self.host = host
        self.data = data

    def on_key(self, event: events.Key):
        if event.key == "q" and self.screen.is_attached:
            self.app.pop_screen()

    def compose(self) -> ComposeResult:
        yield TopBar(read_only=self.read_only, app_version=self.app_version, host=self.host)
        yield Center(Static(self.data, shrink=True))
