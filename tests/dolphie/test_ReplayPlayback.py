"""Playback against a committed daemon recording: stepping, seeking, and surviving a bad frame."""

from __future__ import annotations

import asyncio
import shutil
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import orjson
import pytest
import zstandard as zstd
from textual import events
from textual.widgets import Button

from dolphie.Modules.ArgumentParser import Config
from dolphie.Modules.KeyEventManager import KeyEventManager
from dolphie.Modules.ReplayManager import ReplayManager
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


async def test_a_held_key_scrubs_the_cursor_and_renders_one_frame_when_released(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded_frames: list[int] = []
    load_frame = ReplayManager.get_next_refresh_interval

    def counting_load(self: ReplayManager) -> Any:
        data = load_frame(self)
        loaded_frames.append(self.current_replay_id)
        return data

    monkeypatch.setattr(ReplayManager, "get_next_refresh_interval", counting_load)

    app = HarnessApp(replay_config(REPLAYS / "mysql.db", refresh_interval=3600))
    async with app.run_test(size=(180, 55)) as pilot:
        harness = DolphieHarness(app, pilot)
        await harness.wait_for_replay_frame()
        await harness.click_button("#pause_button")
        await harness.wait_for_worker_idle()
        replay_manager = harness.replay_manager
        last_timestamp = replay_manager.max_replay_timestamp
        assert last_timestamp
        loaded_frames.clear()

        # Key auto-repeat: an event every 33 ms for about a second, which runs off the end
        # of the eight-frame file and keeps going
        for _ in range(30):
            app.post_message(events.Key("right_square_bracket", "]"))
            await asyncio.sleep(0.033)
        await pilot._wait_for_screen()  # pyright: ignore[reportPrivateUsage]

        # The first event is a tap and loads its frame. The repeats only move the cursor and
        # the replay section, so the panels never paint a frame nobody sees
        assert loaded_frames == [2]
        assert replay_manager.current_replay_timestamp == last_timestamp
        assert str(harness.tab.dashboard_replay_start_end.content).count(last_timestamp) == 2
        assert harness.notifications_with("already at the end") == []

        await harness.wait_for(lambda: loaded_frames == [2, FRAMES], message="the frame under the cursor")
        await harness.wait_for_worker_idle()
        assert harness.dolphie.global_status
        assert app.query_one("#forward_button", Button).disabled

    assert harness.error_notifications == []


def test_a_held_key_doubles_its_step_up_to_a_share_of_the_file() -> None:
    app = HarnessApp(replay_config(REPLAYS / "mysql.db"))

    def hold(direction: int, actions: int, total_rows: int) -> list[int]:
        return [app._replay_nav_step(True, direction, total_rows) for _ in range(actions)]  # pyright: ignore[reportPrivateUsage]

    # Four actions per doubling, capped at 25 rows for a small file
    assert hold(1, 20, 1_000) == [1, 1, 1, 2, 2, 2, 2, 4, 4, 4, 4, 8, 8, 8, 8, 16, 16, 16, 16, 25]
    # A five-day daemon recording caps at 1% of its rows
    assert hold(1, 40, 200_000)[-1] == 2_000
    # Reversing direction or releasing the key restarts the ramp
    assert hold(-1, 4, 200_000) == [1, 1, 1, 2]
    assert app._replay_nav_step(False, -1, 200_000) == 1  # pyright: ignore[reportPrivateUsage]
    assert hold(-1, 4, 200_000) == [1, 1, 1, 2]


def test_a_tap_after_a_scrub_is_not_held() -> None:
    app = HarnessApp(replay_config(REPLAYS / "mysql.db"))
    manager = KeyEventManager(app)
    start = datetime(2026, 9, 9, 22, 0, tzinfo=timezone.utc)

    def event_at(milliseconds: int) -> bool:
        manager._update_replay_held_state(  # pyright: ignore[reportPrivateUsage]
            "right_square_bracket", start + timedelta(milliseconds=milliseconds)
        )
        return manager._replay_key_held  # pyright: ignore[reportPrivateUsage]

    assert event_at(0) is False
    assert [event_at(ms) for ms in (33, 66, 99)] == [True, True, True]
    # The rest between releasing and tapping again is longer than any auto-repeat gap
    assert event_at(99 + 200) is False


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
