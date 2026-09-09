from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

from textual.worker import WorkerState

from dolphie.Modules.WorkerManager import WorkerManager


def test_a_worker_finishing_during_shutdown_is_ignored() -> None:
    def get_tab(tab_id: str) -> None:
        raise AssertionError(f"rendered into tab {tab_id} while the app was shutting down")

    app = SimpleNamespace(is_running=False, tab_manager=SimpleNamespace(get_tab=get_tab))
    event = SimpleNamespace(state=WorkerState.SUCCESS, worker=SimpleNamespace(group="replay", name="tab-1"))

    WorkerManager(cast(Any, app)).on_worker_state_changed(cast(Any, event))
