from dolphie.Widgets.topbar import TopBar
from textual import events
from textual.app import ComposeResult
from textual.containers import VerticalScroll
from textual.screen import Screen
from textual.widgets import Static


class CommandScreen(Screen):
    CSS = """
        CommandScreen VerticalScroll {
            padding: 1;
        }
    """

    def __init__(self, read_only, app_version, host, data):
        super().__init__()
        self.read_only = read_only
        self.app_version = app_version
        self.host = host
        self.data = data

    def on_key(self, event: events.Key):
        exclude_events = ["up", "down", "left", "right", "pageup", "pagedown", "home", "end", "tab", "enter"]
        if event.key not in exclude_events:
            if self.screen.is_attached:
                self.app.pop_screen()

    def compose(self) -> ComposeResult:
        yield TopBar(read_only=self.read_only, app_version=self.app_version, host=self.host)
        yield VerticalScroll(Static(self.data))
