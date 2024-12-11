from dolphie.Modules.Functions import format_bytes
from textual.app import ComposeResult
from textual.containers import Container
from textual.reactive import reactive
from textual.widgets import Label


class TopBar(Container):
    host = reactive("", init=False, always_update=True)
    replay_file_size = reactive("", always_update=True)

    def __init__(
        self, connection_status="", app_version="", host="", help="press [b highlight]q[/b highlight] to return"
    ):
        super().__init__()

        self.app_title = f" :dolphin: [b light_blue]Dolphie[/b light_blue] [light_blue]v{app_version}"
        self.topbar_title = Label(self.app_title, id="topbar_title")
        self.topbar_host = Label("", id="topbar_host")
        self.topbar_help = Label(help, id="topbar_help")

        self.connection_status = connection_status
        self.host = host
        self.replay_file_size = None

    def _update_topbar_host(self):
        recording_text = (
            f"| [b recording]RECORDING[/b recording]: {format_bytes(self.replay_file_size)}"
            if self.replay_file_size
            else ""
        )
        self.topbar_host.update(
            f"[[white]{self.connection_status}[/white]] {self.host} {recording_text}" if self.connection_status else ""
        )

    def watch_replay_file_size(self):
        self._update_topbar_host()

    def watch_host(self):
        self._update_topbar_host()

    def compose(self) -> ComposeResult:
        yield self.topbar_title
        yield self.topbar_host
        yield self.topbar_help
