from textual.binding import Binding
from textual.containers import Center
from textual.screen import Screen
from textual.widgets import Static

from dolphie.Widgets.topbar import TopBar


class CommandScreen(Screen):
    CSS = """
        CommandScreen {
            & Center {
                padding: 1;

                & > Static {
                    padding-left: 1;
                    padding-right: 1;
                    background: #101626;
                    border: tall #1d253e;
                    width: auto;
                }
            }
        }
    """

    BINDINGS = [
        Binding("q", "app.pop_screen", "", show=False),
    ]

    def __init__(self, connection_status, app_version, host, data):
        super().__init__()
        self.connection_status = connection_status
        self.app_version = app_version
        self.host = host
        self.data = data

    def compose(self):
        yield TopBar(connection_status=self.connection_status, app_version=self.app_version, host=self.host)
        yield Center(Static(self.data, shrink=True))
