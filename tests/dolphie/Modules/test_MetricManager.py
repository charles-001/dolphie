from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from threading import Lock, Thread

import pytest

from dolphie.DataTypes import ConnectionSource
from dolphie.Modules.MetricDefinitions import MetricData, MetricValue
from dolphie.Modules.MetricManager import MetricManager

BASE_TIME = datetime(2026, 1, 1, tzinfo=timezone.utc)


def refresh(
    manager: MetricManager,
    *,
    at: datetime,
    polling_latency: float = 1,
    queries: int | None = None,
    ahi_enabled: bool = True,
    ahi_hits: int = 100,
    ahi_misses: int = 10,
) -> None:
    """Refresh a manager with the smallest useful MySQL status snapshot."""
    global_status: dict[str, int | float | str] = {"Queries": queries} if queries is not None else {}
    manager.refresh_data(
        at,
        polling_latency=polling_latency,
        global_status=global_status,
        global_variables={"innodb_adaptive_hash_index": "ON" if ahi_enabled else "OFF"},
        innodb_metrics={
            "adaptive_hash_searches": ahi_hits,
            "adaptive_hash_searches_btree": ahi_misses,
        },
    )


def metric_values(metric_data: MetricData) -> list[MetricValue]:
    """Return a metric's values from its atomic snapshot."""
    return metric_data.snapshot()[1]


def test_first_poll_only_establishes_counter_baseline() -> None:
    manager = MetricManager(None)

    refresh(manager, at=BASE_TIME, queries=100)

    assert manager.snapshot_datetimes() == []
    assert metric_values(manager.metrics.dml.Queries) == []
    assert manager.metrics.dml.Queries.last_value == 100


def test_per_second_rates_preserve_fractional_values() -> None:
    manager = MetricManager(None)
    refresh(manager, at=BASE_TIME, polling_latency=5, queries=100)

    refresh(manager, at=BASE_TIME + timedelta(seconds=5), polling_latency=5, queries=101)

    assert metric_values(manager.metrics.dml.Queries) == pytest.approx([0.2])


def test_counter_reset_emits_zero_instead_of_negative_rate() -> None:
    manager = MetricManager(None)
    refresh(manager, at=BASE_TIME, queries=100)

    refresh(manager, at=BASE_TIME + timedelta(seconds=1), queries=10)

    assert metric_values(manager.metrics.dml.Queries) == [0]
    assert manager.metrics.dml.Queries.last_value == 10


def test_missing_source_key_clears_baseline_without_reusing_stale_data() -> None:
    manager = MetricManager(None)
    refresh(manager, at=BASE_TIME, queries=100)

    refresh(manager, at=BASE_TIME + timedelta(seconds=1), queries=None)

    assert "Queries" not in manager.global_status
    assert manager.metrics.dml.Queries.last_value is None
    assert metric_values(manager.metrics.dml.Queries) == []


def test_sparse_metric_history_uses_its_own_timestamps_when_trimming() -> None:
    manager = MetricManager(None, rolling_window_minutes=1)
    refresh(manager, at=BASE_TIME, ahi_enabled=False)
    refresh(manager, at=BASE_TIME + timedelta(seconds=120), ahi_enabled=False)

    refresh(
        manager,
        at=BASE_TIME + timedelta(seconds=241),
        ahi_enabled=True,
        ahi_hits=200,
        ahi_misses=20,
    )

    datetimes, values, _ = manager.metrics.adaptive_hash_index_hit_ratio.hit_ratio.snapshot()
    assert datetimes == ["01/01/26 00:04:01"]
    assert values == pytest.approx([1000 / 11])
    assert manager.snapshot_datetimes() == ["01/01/26 00:04:01"]


def test_replay_refresh_does_not_trim_historical_timestamps_against_wall_clock() -> None:
    manager = MetricManager("replay.db", rolling_window_minutes=10)
    manager.replace_replay_history(
        ["01/01/24 00:00:00"],
        [(manager.metrics.dml.Queries, [1])],
    )

    refresh(manager, at=BASE_TIME, queries=1)

    assert manager.snapshot_datetimes() == ["01/01/24 00:00:00"]
    assert metric_values(manager.metrics.dml.Queries) == [1]


def test_replay_history_tail_aligns_sparse_metric_values() -> None:
    metric = MetricData(label="Sparse", color=(1, 2, 3))

    metric.replace_history(
        [10, 20],
        ["01/01/26 00:00:00", "01/01/26 00:00:01", "01/01/26 00:00:02"],
    )

    datetimes, values, intervals = metric.snapshot()
    assert datetimes == ["01/01/26 00:00:01", "01/01/26 00:00:02"]
    assert values == [10, 20]
    assert intervals == [0, 1]


def test_clear_history_clears_global_and_per_metric_sample_metadata() -> None:
    manager = MetricManager(None)
    refresh(manager, at=BASE_TIME, queries=100)
    refresh(manager, at=BASE_TIME + timedelta(seconds=1), queries=101)

    manager.clear_history()

    assert manager.snapshot_datetimes() == []
    assert manager.metrics.dml.Queries.snapshot() == ([], [], [])
    assert manager.metrics.dml.Queries.last_value is None


def test_reset_preserves_per_host_visibility() -> None:
    first_manager = MetricManager(None)
    second_manager = MetricManager(None)
    first_manager.metrics.dml.Com_select.visible = False

    first_manager.reset()

    assert first_manager.metrics.dml.Com_select.visible is False
    assert second_manager.metrics.dml.Com_select.visible is True


def test_replay_replacement_clears_metrics_missing_from_new_snapshot() -> None:
    manager = MetricManager("replay.db")
    manager.replace_replay_history(
        ["01/01/24 00:00:00"],
        [
            (manager.metrics.dml.Queries, [1]),
            (manager.metrics.dml.Com_select, [2]),
        ],
    )

    manager.replace_replay_history(
        ["01/01/24 00:00:01"],
        [(manager.metrics.dml.Queries, [3])],
    )

    assert metric_values(manager.metrics.dml.Queries) == [3]
    assert manager.metrics.dml.Com_select.snapshot() == ([], [], [])
    assert manager.metrics.dml.Com_select.last_value is None


def test_proxysql_command_stats_aggregate_numeric_buckets() -> None:
    manager = MetricManager(None)
    manager.connection_source = ConnectionSource.proxysql

    manager.update_proxysql_command_stats(
        [
            {"Command": "SELECT", "cnt_1ms": "2", "cnt_10ms": 3},
            {"Command": "INSERT", "cnt_1ms": "4", "cnt_10ms": "invalid"},
        ]
    )

    assert manager.proxysql_select_command_stats["cnt_1ms"] == 2
    assert manager.proxysql_select_command_stats["cnt_10ms"] == 3
    assert manager.proxysql_total_command_stats == {"cnt_1ms": 6, "cnt_10ms": 3}


def test_naive_worker_timestamp_is_consistently_treated_as_utc() -> None:
    manager = MetricManager(None)
    naive_time = BASE_TIME.replace(tzinfo=None)
    refresh(manager, at=naive_time, queries=100)

    refresh(manager, at=naive_time + timedelta(seconds=1), queries=101)

    assert manager.snapshot_datetimes() == ["01/01/26 00:00:01"]
    assert manager.metrics.dml.Queries.snapshot()[0] == ["01/01/26 00:00:01"]


def assert_waits_for_writer(lock: Lock, read: Callable[[], object]) -> None:
    """The reader must block while a writer holds the lock, and finish once it is released."""
    reader = Thread(target=read)
    with lock:
        reader.start()
        reader.join(timeout=0.05)
        assert reader.is_alive()
    reader.join(timeout=1)
    assert not reader.is_alive()


def test_snapshots_wait_for_a_writer_holding_the_lock() -> None:
    metric = MetricData(label="Concurrent", color=(1, 2, 3))
    assert_waits_for_writer(metric._lock, metric.snapshot)

    manager = MetricManager("replay.db")
    assert_waits_for_writer(
        manager._state_lock, lambda: manager.snapshot_history(ConnectionSource.mysql, latest_only=False)
    )


def refresh_row_locks(manager: MetricManager, *, at: datetime, waits: int, waited_ms: int) -> None:
    manager.refresh_data(
        at,
        polling_latency=1,
        global_status={"Innodb_row_lock_waits": waits, "Innodb_row_lock_time": waited_ms},
    )


def test_row_lock_average_wait_divides_new_time_by_new_waits() -> None:
    manager = MetricManager(None)
    refresh_row_locks(manager, at=BASE_TIME, waits=10, waited_ms=1000)

    refresh_row_locks(manager, at=BASE_TIME + timedelta(seconds=1), waits=14, waited_ms=1300)
    refresh_row_locks(manager, at=BASE_TIME + timedelta(seconds=2), waits=14, waited_ms=1300)

    assert metric_values(manager.metrics.row_lock_wait.avg_wait_ms) == pytest.approx([75.0, 0.0])
    assert metric_values(manager.metrics.row_locks.Innodb_row_lock_waits) == pytest.approx([4.0, 0.0])


def test_row_lock_average_wait_treats_counter_reset_as_a_new_baseline() -> None:
    manager = MetricManager(None)
    refresh_row_locks(manager, at=BASE_TIME, waits=10, waited_ms=1000)
    refresh_row_locks(manager, at=BASE_TIME + timedelta(seconds=1), waits=2, waited_ms=50)

    refresh_row_locks(manager, at=BASE_TIME + timedelta(seconds=2), waits=4, waited_ms=250)

    assert metric_values(manager.metrics.row_lock_wait.avg_wait_ms) == pytest.approx([0.0, 100.0])


def test_row_lock_average_wait_is_skipped_when_the_server_lacks_the_counters() -> None:
    manager = MetricManager(None)
    refresh(manager, at=BASE_TIME, queries=100)

    refresh(manager, at=BASE_TIME + timedelta(seconds=1), queries=101)

    assert metric_values(manager.metrics.row_lock_wait.avg_wait_ms) == []


def test_returning_counter_establishes_new_baseline_after_missing_sample() -> None:
    manager = MetricManager(None)
    refresh(manager, at=BASE_TIME, queries=100)
    refresh(manager, at=BASE_TIME + timedelta(seconds=1), queries=None)

    refresh(manager, at=BASE_TIME + timedelta(seconds=2), queries=200)

    assert metric_values(manager.metrics.dml.Queries) == []
    assert manager.metrics.dml.Queries.last_value == 200


def cpu_polls(readings: list[float]) -> list[MetricValue]:
    manager = MetricManager(None)
    for index, cpu in enumerate(readings):
        manager.refresh_data(
            BASE_TIME + timedelta(seconds=index),
            polling_latency=1,
            system_utilization={"CPU_Percent": cpu},
            global_status={},
            global_variables={},
        )
    return metric_values(manager.metrics.system_cpu.CPU_Percent)


def test_cpu_keeps_a_pegged_host_at_100_and_an_idle_one_at_0() -> None:
    # A reading equal to 0 or 100 is a real reading, not a glitch to smooth into the recent mean.
    assert cpu_polls([0.0, 12.0, 15.0, 100.0, 100.0, 100.0, 0.0]) == [12.0, 15.0, 100.0, 100.0, 100.0, 0.0]


def test_cpu_replaces_the_meaningless_first_zero_reading_with_the_second() -> None:
    # The first poll only sets a baseline, so psutil's first zero reaches the history when the
    # second poll reads zero too. The third reading stands in for it.
    assert cpu_polls([0.0, 0.0, 37.5, 40.0]) == [37.5, 37.5, 40.0]
