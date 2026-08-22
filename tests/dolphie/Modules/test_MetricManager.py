from __future__ import annotations

from dataclasses import fields
from datetime import datetime, timedelta, timezone
from threading import Thread

import pytest

from dolphie.DataTypes import ConnectionSource
from dolphie.Modules.MetricDefinitions import MetricData, MetricValue
from dolphie.Modules.MetricGraph import calculate_hourly_rate
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


def test_metric_group_processing_metadata_is_shared_and_not_dataclass_state() -> None:
    first = MetricManager(None)
    second = MetricManager(None)

    field_names = {metric_field.name for metric_field in fields(first.metrics.dml)}
    assert field_names == {
        "Queries",
        "Com_select",
        "Com_insert",
        "Com_update",
        "Com_delete",
        "Com_replace",
        "Com_commit",
        "Com_rollback",
    }
    assert first.metrics.dml.connection_source is second.metrics.dml.connection_source
    assert not hasattr(first.metrics.dml, "graphs")


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


def test_hourly_redo_rate_is_weighted_by_observed_intervals() -> None:
    assert calculate_hourly_rate([10, 20], [1, 9]) == 68_400


def test_naive_worker_timestamp_is_consistently_treated_as_utc() -> None:
    manager = MetricManager(None)
    naive_time = BASE_TIME.replace(tzinfo=None)
    refresh(manager, at=naive_time, queries=100)

    refresh(manager, at=naive_time + timedelta(seconds=1), queries=101)

    assert manager.snapshot_datetimes() == ["01/01/26 00:00:01"]
    assert manager.metrics.dml.Queries.snapshot()[0] == ["01/01/26 00:00:01"]


def test_metric_snapshot_remains_aligned_during_concurrent_appends() -> None:
    metric = MetricData(label="Concurrent", color=(1, 2, 3))

    def append_samples() -> None:
        for second in range(1_000):
            metric.append_sample(second, f"01/01/26 00:{second // 60:02}:{second % 60:02}", 1)

    writer = Thread(target=append_samples)
    writer.start()
    while writer.is_alive():
        datetimes, values, intervals = metric.snapshot()
        assert len(datetimes) == len(values) == len(intervals)
    writer.join()

    datetimes, values, intervals = metric.snapshot()
    assert len(datetimes) == len(values) == len(intervals) == 1_000


def test_manager_snapshot_remains_aligned_during_replay_appends() -> None:
    manager = MetricManager("replay.db")
    query_metric = manager.metrics.dml.Queries

    def append_samples() -> None:
        for second in range(1_000):
            timestamp = f"01/01/26 00:{second // 60:02}:{second % 60:02}"
            manager.append_replay_history(timestamp, [(query_metric, second)])

    writer = Thread(target=append_samples)
    writer.start()

    while writer.is_alive():
        datetimes, metric_history = manager.snapshot_history(ConnectionSource.mysql, latest_only=False)
        dml_history = dict(metric_history)["dml"]
        query_values = dict(dml_history).get("Queries", [])
        assert len(datetimes) == len(query_values)

    writer.join()


def test_returning_counter_establishes_new_baseline_after_missing_sample() -> None:
    manager = MetricManager(None)
    refresh(manager, at=BASE_TIME, queries=100)
    refresh(manager, at=BASE_TIME + timedelta(seconds=1), queries=None)

    refresh(manager, at=BASE_TIME + timedelta(seconds=2), queries=200)

    assert metric_values(manager.metrics.dml.Queries) == []
    assert manager.metrics.dml.Queries.last_value == 200
