from textual.app import ComposeResult
from textual.containers import Container
from textual.reactive import reactive
from textual.widgets import Label


class TopBar(Container):
    host = reactive("", init=False)

    def __init__(self, read_only="", app_version="", host="", help="press [b highlight]q[/b highlight] to return"):
        super().__init__()

        self.topbar_title = Label(
            f" [b light_blue]dolphie[/b light_blue] :dolphin: [light_blue]v{app_version}", id="topbar_title"
        )
        self.topbar_host = Label(self.host, id="topbar_host")
        self.topbar_help = Label(help, id="topbar_help")

        self.read_only = read_only

        if host is None:
            host = ""
        self.host = host

    def watch_host(self):
        if self.read_only:
            self.topbar_host.update(f"[[white]{self.read_only}[/white]] {self.host}")
        else:
            self.topbar_host.update(self.host)

    def compose(self) -> ComposeResult:
        yield self.topbar_title
        yield self.topbar_host
        yield self.topbar_help
