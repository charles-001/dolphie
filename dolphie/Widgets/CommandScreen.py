from textual.app import ComposeResult
from textual.containers import Center
from textual.widgets import Static

from dolphie.Widgets.DolphieScreen import DolphieScreen, ScreenContext


class CommandScreen(DolphieScreen):
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

    def __init__(self, context: ScreenContext, data):
        super().__init__(context)
        self.data = data

    def compose_content(self) -> ComposeResult:
        """Compose the command result."""
        yield Center(Static(self.data, shrink=True))
