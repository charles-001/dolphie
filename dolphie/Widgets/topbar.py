from textual.app import ComposeResult
from textual.containers import Container
from textual.reactive import reactive
from textual.widgets import Label


class TopBar(Container):
    host = reactive("", init=False, always_update=True)

    def __init__(
        self, connection_status="", app_version="", host="", help="press [b highlight]q[/b highlight] to return"
    ):
        super().__init__()

        self.topbar_title = Label(
            f" [b light_blue]Dolphie[/b light_blue] :dolphin: [light_blue]v{app_version}", id="topbar_title"
        )
        self.topbar_host = Label(self.host, id="topbar_host")
        self.topbar_help = Label(help, id="topbar_help")

        self.connection_status = connection_status

        self.host = host if host is not None else ""

    def watch_host(self):
        if self.connection_status:
            self.topbar_host.update(f"[[white]{self.connection_status}[/white]] {self.host}")
        else:
            self.topbar_host.update("")

    def compose(self) -> ComposeResult:
        yield self.topbar_title
        yield self.topbar_host
        yield self.topbar_help
