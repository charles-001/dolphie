from __future__ import annotations

from typing import Protocol, runtime_checkable

from textual import on
from textual.app import ComposeResult
from textual.containers import Container, Horizontal
from textual.reactive import reactive
from textual.widgets import Button, ProgressBar, Static


@runtime_checkable
class ReplayControlsApp(Protocol):
    """Replay actions supplied by Dolphie's Textual app."""

    def action_replay_back(self) -> None: ...

    def action_replay_forward(self) -> None: ...

    def action_replay_pause(self) -> None: ...

    def action_replay_seek(self) -> None: ...


class ReplayControls(Container):
    """Encapsulates the replay playback controls: step back/forward, pause/resume,
    seek, the progress bar, and the start/current/end timestamp labels.

    The actual behavior lives in the app's ``action_replay_*`` methods so that the
    keyboard shortcuts (handled in KeyEventManager) and these buttons share a single
    code path instead of the buttons being driven by simulated ``.press()`` calls.
    """

    # Symbols are emoji-free BMP geometric glyphs (no Variation Selector, no emoji
    # presentation), so the terminal's glyph advance matches rich.cell_len exactly.
    # Emoji glyphs (⏮ ⏸ ⏭ 🕒) render wider than their 2-cell allotment in many
    # terminals, and that drift desyncs the bar's background fill (the "notch").
    BACK_LABEL = "◄◄ Back"
    PAUSE_LABEL = "▮▮ Pause"
    RESUME_LABEL = "► Resume"
    FORWARD_LABEL = "►► Forward"
    SEEK_LABEL = "◎ Seek"

    paused: reactive[bool] = reactive(False, init=False)

    def compose(self) -> ComposeResult:
        yield Static(id="dashboard_replay", classes="dashboard_replay")
        yield Static(id="dashboard_replay_start_end", classes="dashboard_replay")
        yield Horizontal(
            Button(self.BACK_LABEL, id="back_button", classes="replay_button"),
            Button(self.PAUSE_LABEL, id="pause_button", classes="replay_button"),
            Button(self.FORWARD_LABEL, id="forward_button", classes="replay_button"),
            Button(self.SEEK_LABEL, id="seek_button", classes="replay_button"),
            classes="replay_buttons",
        )
        yield ProgressBar(id="dashboard_replay_progressbar", total=100, show_percentage=False, show_eta=False)

    def watch_paused(self, paused: bool) -> None:
        self.query_one("#pause_button", Button).label = self.RESUME_LABEL if paused else self.PAUSE_LABEL

    def set_boundary_states(self, at_start: bool, at_end: bool) -> None:
        """Disable Back at the start of the replay and Forward at the end so the
        buttons reflect what's actually possible instead of silently no-op'ing.
        """
        self.query_one("#back_button", Button).disabled = at_start
        self.query_one("#forward_button", Button).disabled = at_end

    def _replay_app(self) -> ReplayControlsApp:
        app = self.app
        assert isinstance(app, ReplayControlsApp)
        return app

    @on(Button.Pressed, "#back_button")
    def _back_pressed(self) -> None:
        self._replay_app().action_replay_back()

    @on(Button.Pressed, "#forward_button")
    def _forward_pressed(self) -> None:
        self._replay_app().action_replay_forward()

    @on(Button.Pressed, "#pause_button")
    def _pause_pressed(self) -> None:
        self._replay_app().action_replay_pause()

    @on(Button.Pressed, "#seek_button")
    def _seek_pressed(self) -> None:
        self._replay_app().action_replay_seek()
