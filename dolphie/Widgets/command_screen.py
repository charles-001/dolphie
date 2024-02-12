from dolphie.Widgets.topbar import TopBar
from textual import events
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
            background: #101626;
            border: tall #1d253e;
            width: auto;
        }
    """

    def __init__(self, connection_status, app_version, host, data):
        super().__init__()
        self.connection_status = connection_status
        self.app_version = app_version
        self.host = host
        self.data = data

    def on_key(self, event: events.Key):
        if event.key == "q" and self.screen.is_attached:
            self.app.pop_screen()

    def compose(self):
        yield TopBar(connection_status=self.connection_status, app_version=self.app_version, host=self.host)
        yield Center(Static(self.data, shrink=True))
