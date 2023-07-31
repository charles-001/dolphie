from textual.app import ComposeResult
from textual.containers import Container
from textual.reactive import reactive
from textual.widgets import Label


class TopBar(Container):
    app_version = reactive("", init=False)
    host = reactive("", init=False)

    def __init__(self, app_version="", host="", help=""):
        super().__init__()

        self.help = "press any key to return (except navigation keys)"
        if help:
            self.help = help

        self.topbar_title = Label(
            f" [b #bbc8e8]dolphie[/b #bbc8e8] :dolphin: [#bbc8e8]v{app_version}", id="topbar_title"
        )
        self.topbar_host = Label(self.host, id="topbar_host")
        self.topbar_help = Label(self.help, id="topbar_help")

        self.app_version = app_version
        self.host = host

    def watch_app_version(self):
        self.topbar_title.update(f" [b #bbc8e8]dolphie[/b #bbc8e8] :dolphin: [#bbc8e8]v{self.app_version}")

    def watch_host(self):
        self.topbar_host.update(self.host)

    def compose(self) -> ComposeResult:
        yield self.topbar_title
        yield self.topbar_host
        yield self.topbar_help
