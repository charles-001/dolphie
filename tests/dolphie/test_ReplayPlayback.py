"""Playback against a committed daemon recording: stepping, seeking, and surviving a bad frame."""

from __future__ import annotations

import asyncio
import shutil
import sqlite3
from pathlib import Path
from typing import Any

import orjson
import pytest
import zstandard as zstd
from textual.widgets import Button

from dolphie.Modules.ArgumentParser import Config
from dolphie.Panels import Dashboard
from tests.integration.harness import UNREACHABLE_PYPI, DolphieHarness, HarnessApp

REPLAYS = Path(__file__).parent / "replays"
# The committed recordings hold eight frames, two seconds apart
FRAMES = 8


def replay_config(replay_file: Path, **overrides: Any) -> Config:
    values: dict[str, Any] = {
        "replay_file": str(replay_file),
        "pypi_repository": UNREACHABLE_PYPI,
        "refresh_interval": 0.2,
        **overrides,
    }
    return Config(app_version="test", **values)


def metric_datetimes(replay_file: Path) -> list[list[str]]:
    """The graph timestamps each frame recorded, read without the code under test."""
    connection = sqlite3.connect(f"{replay_file.resolve().as_uri()}?mode=ro", uri=True)
    try:
        (dictionary,) = connection.execute("SELECT compression_dict FROM metadata").fetchone()
        decompressor = zstd.ZstdDecompressor(dict_data=zstd.ZstdCompressionDict(dictionary) if dictionary else None)
        rows = connection.execute("SELECT data FROM replay_data ORDER BY id").fetchall()
    finally:
        connection.close()
    return [orjson.loads(decompressor.decompress(blob))["metric_manager"]["datetimes"] for (blob,) in rows]


async def test_forward_and_back_step_one_frame_and_rebuild_the_metric_window() -> None:
    recorded = metric_datetimes(REPLAYS / "mysql.db")
    assert len(recorded) == FRAMES

    # A long interval keeps the paused replay's timer from racing the manual steps
    app = HarnessApp(replay_config(REPLAYS / "mysql.db", refresh_interval=3600))
    async with app.run_test(size=(180, 55)) as pilot:
        harness = DolphieHarness(app, pilot)
        await harness.wait_for_replay_frame()
        await harness.click_button("#pause_button")
        await harness.wait_for_worker_idle()
        replay_manager = harness.replay_manager
        metric_manager = harness.dolphie.metric_manager
        assert replay_manager.current_replay_id == 1
        assert replay_manager.max_replay_id == FRAMES

        async def press(key: str) -> None:
            # A frame loads faster than the key debounce, so taps must be spaced like a human's
            await asyncio.sleep(app.key_event_manager.key_debounce_intervals[key].total_seconds())
            await harness.press(key)

        async def step(key: str, frame: int) -> None:
            await press(key)
            await harness.wait_for(lambda: replay_manager.current_replay_id == frame, message=f"frame {frame}")
            await harness.wait_for_worker_idle()
            # Delta rows inside one window: the graph history is every point recorded up to this
            # frame, appended when stepping forward and rebuilt from the file when stepping back
            assert metric_manager.snapshot_datetimes() == [point for row in recorded[:frame] for point in row]

        await step("right_square_bracket", 2)
        await step("right_square_bracket", 3)
        await step("left_square_bracket", 2)
        await step("left_square_bracket", 1)

        await press("left_square_bracket")
        assert harness.notifications_with("already at the beginning")

        assert replay_manager.seek_relative(100)
        app.force_refresh_for_replay()
        await harness.wait_for(lambda: replay_manager.current_replay_id == FRAMES, message="the last frame")
        await harness.wait_for_worker_idle()
        assert metric_manager.snapshot_datetimes() == [point for row in recorded for point in row]

        await press("right_square_bracket")
        assert harness.notifications_with("already at the end")
        assert replay_manager.current_replay_id == FRAMES

    assert harness.error_notifications == []


async def test_a_frame_that_fails_to_render_pauses_the_replay_instead_of_crashing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def broken_panel(_tab: object) -> None:
        raise RuntimeError("panel cannot render this frame")

    monkeypatch.setattr(Dashboard, "create_panel", broken_panel)

    app = HarnessApp(replay_config(REPLAYS / "mysql.db"))
    async with app.run_test(size=(180, 55)) as pilot:
        harness = DolphieHarness(app, pilot)
        await harness.wait_for(
            lambda: bool(harness.notifications_with("Replay Error")), message="the render error", expect_errors=True
        )
        tab = harness.tab
        replay_manager = harness.replay_manager

        assert tab.dolphie.pause_refresh
        assert tab.replay_controls.paused
        assert any("Failed to render replay frame" in line for line in app.log_errors)
        paused_at = replay_manager.current_replay_id

        # With the panel fixed, resuming plays on from the frame that failed
        monkeypatch.undo()
        app.query_one("#pause_button", Button).press()
        await harness.wait_for(
            lambda: replay_manager.current_replay_id > paused_at, message="the replay to resume", expect_errors=True
        )

    assert app.errors == []
    assert len(harness.error_notifications) == 1


async def test_a_row_that_cannot_be_read_is_skipped_during_playback(tmp_path: Path) -> None:
    replay_file = tmp_path / "mysql.db"
    shutil.copy(REPLAYS / "mysql.db", replay_file)
    connection = sqlite3.connect(replay_file)
    try:
        connection.execute("UPDATE replay_data SET data = ? WHERE id = 2", (b"not a zstd frame",))
        connection.commit()
    finally:
        connection.close()

    app = HarnessApp(replay_config(replay_file))
    async with app.run_test(size=(180, 55)) as pilot:
        harness = DolphieHarness(app, pilot)
        replay_manager = harness.replay_manager
        await harness.wait_for(
            lambda: replay_manager.current_replay_id >= 3, message="playback past the bad row", expect_errors=True
        )

        assert harness.dolphie.global_status
        assert [n.title for n in harness.error_notifications] == ["Unreadable replay data"]

    assert app.errors == []
