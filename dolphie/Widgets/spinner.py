from rich.spinner import Spinner
from textual.widgets import Static


class SpinnerWidget(Static):
    def __init__(self, id, text):
        super().__init__("")
        self._id = id
        self._spinner = Spinner("bouncingBar", text=f"[label]{text}", speed=0.7)

    def on_mount(self) -> None:
        self.update_render = self.set_interval(1 / 60, self.update_spinner)

    def hide(self) -> None:
        self.display = False

    def show(self) -> None:
        self.display = True

    def update_spinner(self) -> None:
        self.update(self._spinner)
