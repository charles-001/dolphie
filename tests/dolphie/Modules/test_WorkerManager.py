from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, cast

import pytest
from textual.worker import WorkerState

from dolphie.DataTypes import ConnectionSource
from dolphie.Modules.ManualException import ManualException
from dolphie.Modules.WorkerManager import WorkerManager


async def test_a_failed_poll_leaves_the_rate_interval_spanning_to_the_next_good_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Counters keep their baseline across a poll that raised, so the seconds must too."""
    start = datetime(2026, 9, 1, 12, 0, 0, tzinfo=timezone.utc)
    # The failed poll reads the clock once at its start. The good poll reads it at start and end.
    clock = iter(start + timedelta(seconds=offset) for offset in (2, 4, 4.1))

    class FakeDatetime:
        @staticmethod
        def now() -> datetime:
            return next(clock)

    monkeypatch.setattr("dolphie.Modules.WorkerManager.datetime", FakeDatetime)
    monkeypatch.setattr("dolphie.Modules.WorkerManager.get_current_worker", lambda: SimpleNamespace(name=""))

    polls = iter([ManualException("gone"), None])
    latencies: list[float] = []

    def process_mysql_data(_tab: Any) -> None:
        if (outcome := next(polls)) is not None:
            raise outcome

    def refresh_data(**kwargs: Any) -> None:
        latencies.append(kwargs["polling_latency"])

    dolphie = SimpleNamespace(
        replay_file=None,
        main_db_connection=SimpleNamespace(is_connected=lambda: True),
        worker_previous_start_time=start,
        polling_latency=0.0,
        worker_processing_time=0.0,
        connection_source=ConnectionSource.mysql,
        collect_system_utilization=lambda: None,
        metric_manager=SimpleNamespace(refresh_data=refresh_data),
        system_utilization={},
        global_variables={},
        global_status={},
        innodb_metrics={},
        disk_io_metrics={},
        metadata_locks=[],
        replication_status=[],
        proxysql_command_stats={},
    )
    tab = SimpleNamespace(dolphie=dolphie, worker=None, replay_manager=SimpleNamespace(capture_state=lambda: None))
    app = SimpleNamespace(
        tab_manager=SimpleNamespace(get_tab=lambda tab_id: tab, disconnect_tab=lambda *a, **k: None),
        worker_data_processor=SimpleNamespace(process_mysql_data=process_mysql_data),
        call_from_thread=lambda fn, *args, **kwargs: fn(*args, **kwargs),
    )
    manager = WorkerManager(cast(Any, app))

    await manager.run_worker_main("tab-1")
    assert latencies == []
    assert dolphie.worker_previous_start_time == start

    await manager.run_worker_main("tab-1")
    assert latencies == [4.0]
    assert dolphie.worker_previous_start_time == start + timedelta(seconds=4)


def test_a_worker_finishing_during_shutdown_is_ignored() -> None:
    def get_tab(tab_id: str) -> None:
        raise AssertionError(f"rendered into tab {tab_id} while the app was shutting down")

    app = SimpleNamespace(is_running=False, tab_manager=SimpleNamespace(get_tab=get_tab))
    event = SimpleNamespace(state=WorkerState.SUCCESS, worker=SimpleNamespace(group="replay", name="tab-1"))

    WorkerManager(cast(Any, app)).on_worker_state_changed(cast(Any, event))
