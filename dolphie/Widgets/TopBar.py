from __future__ import annotations

from rich.text import Text
from textual.app import ComposeResult
from textual.containers import Container
from textual.reactive import reactive
from textual.widgets import Label

from dolphie.Modules.Functions import format_bytes
from dolphie.Modules.Theme import FOREGROUND, LIGHT_BLUE, RECORDING, themed_content, themed_text


class TopBar(Container):
    connection_status: reactive[str | None] = reactive(None)
    host: reactive[str] = reactive("", init=False)
    replay_file_size: reactive[int | None] = reactive(None)

    def __init__(
        self,
        connection_status: str | None = None,
        app_version: str = "",
        host: str = "",
        help: str = "",
    ):
        super().__init__()

        self.app_title = Text(" 🐬 ")
        self.app_title.append("Dolphie", style=f"bold {LIGHT_BLUE}")
        self.app_title.append(f" v{app_version}", style=LIGHT_BLUE)

        self.topbar_title = Label(self.app_title, id="topbar_title")
        self.topbar_host = Label("", id="topbar_host")
        self.topbar_help = Label(themed_content(help), id="topbar_help")

        self.connection_status = connection_status
        self.host = host
        self.replay_file_size = None

    def _update_topbar_host(self):
        host_text = Text()
        if self.connection_status:
            host_text.append("[")
            host_text.append(str(self.connection_status), style=FOREGROUND)
            host_text.append(f"] {self.host}")
            if self.replay_file_size:
                host_text.append(" | ")
                host_text.append("RECORDING", style=f"bold {RECORDING}")
                host_text.append(": ")
                host_text.append_text(themed_text(format_bytes(self.replay_file_size)))
        self.topbar_host.update(host_text)

    def watch_replay_file_size(self):
        self._update_topbar_host()

    def watch_connection_status(self):
        self._update_topbar_host()

    def watch_host(self):
        self._update_topbar_host()

    def compose(self) -> ComposeResult:
        yield self.topbar_title
        yield self.topbar_host
        yield self.topbar_help
