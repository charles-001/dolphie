"""SVG snapshots of every panel, rendered from committed daemon recordings so no database is needed.

A failing snapshot means the rendering changed. Open the report pytest links to, and if the change
is intended, run `uv run pytest tests/dolphie/test_Snapshots.py --snapshot-update`.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
from textual.pilot import Pilot

from dolphie.Modules.ArgumentParser import Config
from tests.integration.harness import UNREACHABLE_PYPI, DolphieHarness, HarnessApp

REPLAYS = Path(__file__).parent / "replays"
TERMINAL_SIZE = (180, 55)
# The frame the screenshot shows. Earlier frames are applied first so delta-based panels have data.
FRAME = 3


class SnapshotApp(HarnessApp):
    """Toasts would land in the screenshot, so record notifications without showing them."""

    SHOW_TOASTS = False


def show_panels(app: SnapshotApp, *keys: str) -> Callable[[Pilot[Any]], Any]:
    async def run_before(pilot: Pilot[Any]) -> None:
        harness = DolphieHarness(app, pilot)
        await harness.wait_for_replay_frame()
        await harness.click_button("#pause_button")
        tab = harness.tab
        replay_manager = harness.replay_manager

        # The replay worker is exclusive: starting one while another is still applying a frame
        # cancels the first mid-frame. Every step below waits for the worker to finish first.
        async def worker_idle() -> None:
            await harness.wait_for(lambda: tab.worker is None or not tab.worker.is_running, message="replay worker")

        await worker_idle()
        for key in keys:
            await harness.press(key)
            await worker_idle()

        # Seek through the app's own replay actions. The bracket keys are debounced and accelerate
        # when repeated quickly, so key presses cannot land on an exact frame.
        async def seek(frame: int) -> None:
            if replay_manager.current_replay_id == frame:
                return
            assert replay_manager.seek_relative(frame - replay_manager.current_replay_id), f"no frame {frame}"
            app.force_refresh_for_replay()
            await harness.wait_for(lambda: replay_manager.current_replay_id == frame, message=f"frame {frame}")
            await worker_idle()

        for frame in range(1, FRAME + 1):
            await seek(frame)

    return run_before


@pytest.fixture
def pinned_rendering(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Textual renders monochrome under NO_COLOR, and the graph's time axis follows the local zone."""
    monkeypatch.delenv("NO_COLOR", raising=False)
    monkeypatch.setenv("TZ", "UTC")
    time.tzset()
    yield
    monkeypatch.undo()
    time.tzset()


@pytest.mark.parametrize(
    ("source", "keys"),
    [
        ("mysql", ()),
        ("mysql", ("3",)),
        ("mysql", ("5", "7", "8")),
        ("mariadb", ()),
        ("mariadb", ("3",)),
        ("mariadb", ("5", "7", "8")),
        ("proxysql", ()),
        ("proxysql", ("4",)),
    ],
    ids=lambda value: ("".join(value) or "dashboard") if isinstance(value, tuple) else value,
)
@pytest.mark.usefixtures("pinned_rendering")
def test_panel_snapshot(snap_compare: Any, source: str, keys: tuple[str, ...]) -> None:
    # A paused replay still re-arms a worker timer every refresh interval. A long interval keeps
    # that timer from racing the seeks in show_panels, and nothing renders the interval itself.
    config = Config(
        app_version="snapshot",
        replay_file=str(REPLAYS / f"{source}.db"),
        pypi_repository=UNREACHABLE_PYPI,
        refresh_interval=3600,
    )
    app = SnapshotApp(config)
    assert snap_compare(app, terminal_size=TERMINAL_SIZE, run_before=show_panels(app, *keys))
