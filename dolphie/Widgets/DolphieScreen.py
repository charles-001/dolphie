from dataclasses import dataclass
from typing import ClassVar

from textual.app import ComposeResult
from textual.binding import Binding, BindingType
from textual.screen import Screen
from textual.widgets import Footer

from dolphie.Widgets.TopBar import TopBar


@dataclass(frozen=True)
class ScreenContext:
    """Connection identity displayed by a secondary screen."""

    connection_status: str
    app_version: str
    host: str


class DolphieScreen(Screen[None]):
    """Full-screen Dolphie view with consistent application chrome."""

    BINDINGS: ClassVar[list[BindingType]] = [
        Binding("q", "close", "Close"),
        Binding("escape", "close", "", show=False),
    ]

    def __init__(self, context: ScreenContext) -> None:
        super().__init__()
        self.context = context

    def compose(self) -> ComposeResult:
        """Compose shared identity and command chrome around screen content."""
        yield TopBar(
            connection_status=self.context.connection_status,
            app_version=self.context.app_version,
            host=self.context.host,
        )
        yield from self.compose_content()
        yield Footer(compact=True, show_command_palette=False)

    def compose_content(self) -> ComposeResult:
        """Compose the screen-specific content between the shared bars."""
        raise NotImplementedError

    def action_close(self) -> None:
        """Return to the previous screen."""
        self.dismiss(None)
