from rich.spinner import Spinner
from rich.text import Text
from textual.widgets import Static

from dolphie.Modules.Theme import LABEL


class SpinnerWidget(Static):
    def __init__(self, id, text):
        super().__init__("")
        self._id = id
        self._spinner = Spinner("bouncingBar", text=Text(text, style=LABEL), speed=0.7)

    def on_mount(self) -> None:
        self.update_render = self.set_interval(1 / 60, self.update_spinner)

    def hide(self) -> None:
        self.display = False

    def show(self) -> None:
        self.display = True

    def update_spinner(self) -> None:
        self.update(self._spinner)
