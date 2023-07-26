from dolphie.Widgets.topbar import TopBar
from textual import events
from textual.app import ComposeResult
from textual.screen import Screen
from textual.widgets import Static


class InfoScreen(Screen):
    def __init__(self, app_version, host, data):
        super().__init__()
        self.app_version = app_version
        self.host = host
        self.data = data

        self.topbar = TopBar(app_version=self.app_version, host=self.host, help="press any key to return")

    def on_key(self, event: events.Key):
        exclude_events = ["up", "down", "left", "right", "pageup", "pagedown"]
        if event.key not in exclude_events:
            self.app.pop_screen()

    def compose(self) -> ComposeResult:
        yield self.topbar
        yield Static("")
        yield self.data
