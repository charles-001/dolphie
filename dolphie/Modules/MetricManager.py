from __future__ import annotations

from collections import defaultdict, deque
from collections.abc import Iterator, Mapping, Sequence
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import TypeVar

from dolphie.DataTypes import ConnectionSource, ConnectionSourceType, DatabaseRow
from dolphie.Modules.Functions import coerce_int, coerce_str
from dolphie.Modules.MetricDefinitions import (
    METRIC_DATETIME_FORMAT,
    MetricData,
    MetricInstance,
    MetricInstances,
    MetricSource,
    MetricValue,
    create_metric_instances,
    iter_metric_data,
    iter_metric_instances,
    parse_metric_datetime,
)

_SourceValue = TypeVar("_SourceValue")


class MetricManager:
    """Manage the state, collection, and processing of all metrics."""

    DEFAULT_ROLLING_WINDOW_MINUTES: int = 10
    CHECKPOINT_SYNC_FLUSH_RATIO: float = 0.825
    AHI_SMOOTHING_FACTOR: float = 0.5
    EXTREME_VALUES: frozenset[int] = frozenset({0, 100})
    SMOOTHING_THRESHOLD: int = 10
    SMOOTHING_SAMPLE_COUNT: int = 3

    def __init__(
        self,
        replay_file: str | None,
        rolling_window_minutes: int | None = None,
    ) -> None:
        """Initialize the MetricManager.

        Args:
            replay_file: Path to a replay file, if one is being used.
            rolling_window_minutes: How many minutes of history to keep in graph
                data. 0 disables trimming (accumulate forever). None falls back
                to DEFAULT_ROLLING_WINDOW_MINUTES.
        """
        self.connection_source: ConnectionSourceType = ConnectionSource.mysql
        self.replay_file: str | None = replay_file
        self.rolling_window_minutes: int = (
            rolling_window_minutes if rolling_window_minutes is not None else self.DEFAULT_ROLLING_WINDOW_MINUTES
        )

        # Attributes populated by refresh_data
        self.worker_start_time: datetime | None = None
        # Cached formatted worker_start_time so each metric appended in one
        # poll cycle doesn't re-run astimezone/strftime.
        self._worker_start_timestamp: str | None = None
        self.system_utilization: dict[str, int | float | tuple[float, float, float]] = {}
        self.innodb_metrics: dict[str, int | str] = {}
        self.disk_io_metrics: dict[str, int | str] = {}
        self.metadata_lock_metrics: list[DatabaseRow] = []
        self.replication_status: list[DatabaseRow] = []
        self.proxysql_total_command_stats: dict[str, int] = {}
        self.proxysql_select_command_stats: dict[str, int] = {}

        # State attributes
        self.initialized: bool = False
        self.polling_latency: float = 0
        self.global_variables: dict[str, int | str] = {}
        self.global_status: dict[str, int | float | str] = {}
        self.redo_log_size: int = 0
        # Use a deque for O(1) appends and pops
        self.datetimes: deque[str] = deque()
        self._state_lock = Lock()

        # The authoritative structure of all metrics
        self.metrics: MetricInstances

        # Optimized lookup tables for processing
        # For fast, source-based processing in update_..._values
        self._source_to_metrics_processing: dict[
            MetricSource,
            list[tuple[str, MetricData, tuple[ConnectionSourceType, ...]]],
        ] = defaultdict(list)
        # For fast rolling-window history cleanup
        self._all_metrics_data_history: list[MetricData] = []

        # Setup the dispatch map for metric sources
        self._metric_source_map: dict[MetricSource, Mapping[str, object] | None] = {
            MetricSource.SYSTEM_UTILIZATION: self.system_utilization,
            MetricSource.GLOBAL_STATUS: self.global_status,
            MetricSource.INNODB_METRICS: self.innodb_metrics,
            MetricSource.DISK_IO_METRICS: self.disk_io_metrics,
            MetricSource.PROXYSQL_SELECT_COMMAND_STATS: self.proxysql_select_command_stats,
            MetricSource.PROXYSQL_TOTAL_COMMAND_STATS: self.proxysql_total_command_stats,
            MetricSource.NONE: None,
        }

        self.reset()

    def get_metric_instance(self, name: str) -> MetricInstance | None:
        """Return a named metric instance from the catalog."""
        if name not in MetricInstances.__dataclass_fields__:
            return None
        return getattr(self.metrics, name)

    def get_metric_data(self, metric_instance: MetricInstance, name: str) -> MetricData | None:
        """Return a named MetricData field from an instance."""
        value = getattr(metric_instance, name, None)
        return value if isinstance(value, MetricData) else None

    def snapshot_visibility(self) -> dict[tuple[str, str], bool]:
        """Return this manager's per-host graph visibility state."""
        with self._state_lock:
            return self._snapshot_visibility()

    def _snapshot_visibility(self) -> dict[tuple[str, str], bool]:
        """Capture per-series visibility while the manager lock is held."""
        return {
            (instance_name, metric_name): metric_data.visible
            for instance_name, metric_instance in iter_metric_instances(self.metrics)
            for metric_name, metric_data in iter_metric_data(metric_instance)
        }

    def restore_visibility(self, visibility: Mapping[tuple[str, str], bool]) -> None:
        """Restore visibility without reading the shared graph controls."""
        with self._state_lock:
            self._restore_visibility(visibility)

    def _restore_visibility(self, visibility: Mapping[tuple[str, str], bool]) -> None:
        """Apply captured visibility while the manager lock is held."""
        for (instance_name, metric_name), visible in visibility.items():
            metric_instance = self.get_metric_instance(instance_name)
            if metric_instance is None:
                continue
            metric_data = self.get_metric_data(metric_instance, metric_name)
            if metric_data is not None:
                metric_data.visible = visible

    def clear_history(self) -> None:
        """Clear all timestamp and metric value history."""
        with self._state_lock:
            self._clear_history()

    def _clear_history(self) -> None:
        """Clear all history while the manager lock is held."""
        self.datetimes.clear()
        for _, metric_instance in iter_metric_instances(self.metrics):
            for _, metric_data in iter_metric_data(metric_instance):
                metric_data.clear_history()
                metric_data.last_value = None
        self.metrics.adaptive_hash_index_hit_ratio.smoothed_hit_ratio = None

    def snapshot_datetimes(self) -> list[str]:
        """Return an atomic copy of global timestamps for replay serialization."""
        with self._state_lock:
            return list(self.datetimes)

    def snapshot_history(
        self,
        connection_source: ConnectionSourceType,
        latest_only: bool,
    ) -> tuple[list[str], list[tuple[str, list[tuple[str, list[MetricValue]]]]]]:
        """Return one manager-wide atomic history snapshot for replay storage."""
        with self._state_lock:
            datetimes = list(self.datetimes)
            if latest_only:
                datetimes = datetimes[-1:]

            metric_history = []
            for instance_name, metric_instance in iter_metric_instances(self.metrics):
                if connection_source not in metric_instance.connection_source:
                    continue
                if connection_source == ConnectionSource.mysql and not metric_instance.use_with_replay:
                    continue

                series_history = []
                for metric_name, metric_data in iter_metric_data(metric_instance):
                    if latest_only:
                        latest = metric_data.latest_value()
                        values = [latest] if latest is not None else []
                    else:
                        values = metric_data.values_snapshot()
                    if values:
                        series_history.append((metric_name, values))
                metric_history.append((instance_name, series_history))

            return datetimes, metric_history

    def latest_datetime(self) -> str | None:
        """Return the newest global timestamp, if present."""
        with self._state_lock:
            return self.datetimes[-1] if self.datetimes else None

    def append_replay_history(
        self,
        timestamp: str,
        metric_values: Sequence[tuple[MetricData, MetricValue]],
        reference_time: datetime | None = None,
    ) -> None:
        """Atomically append one replay event and optionally trim its window."""
        with self._state_lock:
            self.datetimes.append(timestamp)
            for metric_data, value in metric_values:
                metric_data.append_sample(value, timestamp, 0)
                metric_data.last_value = value

            if reference_time is not None and self.rolling_window_minutes > 0:
                self._trim_datetimes_to_window(reference_time)

    def rebuild_replay_history(
        self,
        entries: Sequence[tuple[list[str], Sequence[tuple[MetricData, list[MetricValue]]]]],
    ) -> None:
        """Atomically rebuild replay history from complete and delta events."""
        with self._state_lock:
            self._clear_history()
            for datetimes, metric_values in entries:
                self.datetimes.extend(datetimes)
                for metric_data, values in metric_values:
                    metric_data.extend_history(values, datetimes)
                    if values:
                        metric_data.last_value = values[-1]

    def replace_replay_history(
        self,
        datetimes: list[str],
        metric_values: Sequence[tuple[MetricData, list[MetricValue]]],
    ) -> None:
        """Atomically replace all replay history with one complete snapshot."""
        with self._state_lock:
            self._clear_history()
            self.datetimes = deque(datetimes)
            for metric_data, values in metric_values:
                metric_data.replace_history(values, datetimes)
                metric_data.last_value = values[-1] if values else None

    def reset(self) -> None:
        """Reset all metrics and state to their default values."""
        with self._state_lock:
            visibility = self._snapshot_visibility() if hasattr(self, "metrics") else {}
            self.initialized = False
            self.polling_latency = 0
            self.redo_log_size = 0
            self.datetimes.clear()

            # Note: raw data stores (global_variables, global_status, replication_status, etc.)
            # are intentionally NOT cleared here — they are owned by dolphie and shared by
            # reference. Mutating them would cause bugs (e.g. false errant TRX detection).
            # They are overwritten every poll cycle by refresh_data().

            # Clear performance lookup tables
            self._source_to_metrics_processing.clear()
            self._all_metrics_data_history.clear()

            self.metrics = create_metric_instances()

            # Build the optimized lookup tables
            for _, metric_instance in iter_metric_instances(self.metrics):
                source = metric_instance.metric_source
                conn_source = metric_instance.connection_source

                for attr_name, metric_data in iter_metric_data(metric_instance):
                    if metric_data.save_history:
                        self._all_metrics_data_history.append(metric_data)

                    # Add to processing list if it has a valid source
                    if source != MetricSource.NONE:
                        self._source_to_metrics_processing[source].append((attr_name, metric_data, conn_source))

            self._restore_visibility(visibility)

    def refresh_data(
        self,
        worker_start_time: datetime,
        polling_latency: float = 0,
        system_utilization: dict[str, int | float | tuple[float, float, float]] | None = None,
        global_variables: dict[str, int | str] | None = None,
        global_status: dict[str, int | float | str] | None = None,
        innodb_metrics: dict[str, int | str] | None = None,
        proxysql_command_stats: list[dict[str, int | str]] | None = None,
        disk_io_metrics: dict[str, int | str] | None = None,
        metadata_lock_metrics: list[DatabaseRow] | None = None,
        replication_status: list[DatabaseRow] | None = None,
    ) -> None:
        """Atomically ingest one complete polling-worker snapshot."""
        with self._state_lock:
            if replication_status is None:
                replication_status = []
            if metadata_lock_metrics is None:
                metadata_lock_metrics = []
            if disk_io_metrics is None:
                disk_io_metrics = {}
            if proxysql_command_stats is None:
                proxysql_command_stats = []
            if innodb_metrics is None:
                innodb_metrics = {}
            if global_status is None:
                global_status = {}
            if global_variables is None:
                global_variables = {}
            if system_utilization is None:
                system_utilization = {}
            self.worker_start_time = worker_start_time
            self._worker_start_timestamp = self._format_timestamp(worker_start_time)
            self.polling_latency = polling_latency
            self._replace_source_data(self.system_utilization, system_utilization)
            self.global_variables = global_variables
            self._replace_source_data(self.global_status, global_status)
            self._replace_source_data(self.innodb_metrics, innodb_metrics)
            self._replace_source_data(self.disk_io_metrics, disk_io_metrics)
            self.metadata_lock_metrics = metadata_lock_metrics
            self.replication_status = replication_status

            self.proxysql_total_command_stats.clear()
            self.proxysql_select_command_stats.clear()

            # Calculate redo log size
            innodb_redo_log_capacity = coerce_int(self.global_variables.get("innodb_redo_log_capacity"))
            innodb_log_file_size_value = coerce_int(self.global_variables.get("innodb_log_file_size"))
            innodb_log_files_in_group = coerce_int(self.global_variables.get("innodb_log_files_in_group"), default=1)
            innodb_log_file_size = round(innodb_log_file_size_value * innodb_log_files_in_group)
            self.redo_log_size = max(innodb_redo_log_capacity, innodb_log_file_size)

            if not self.replay_file:
                self.update_proxysql_command_stats(proxysql_command_stats)
                self.update_metrics_per_second_values()
                self.update_metrics_replication_lag()
                self.update_metrics_adaptive_hash_index_hit_ratio()
                self.update_metrics_locks()
                self.update_metrics_last_value()  # Must be last

            self.update_metrics_checkpoint()
            self.metrics.redo_log.redo_log_size = self.redo_log_size

            self._add_metric_datetime()

            # Replay owns trimming because its timestamps can be historical.
            if not self.replay_file and self.rolling_window_minutes > 0:
                _ = self._trim_datetimes_to_window(worker_start_time)

            # The first poll establishes counter baselines. History starts on the
            # second poll so rate metrics never fabricate a startup data point.
            self.initialized = True

    @staticmethod
    def _replace_source_data(target: dict[str, _SourceValue], source: Mapping[str, _SourceValue]) -> None:
        """Replace a source snapshot without invalidating dispatch-map references."""
        target.clear()
        target.update(source)

    def add_metric(self, metric_data: MetricData, value: MetricValue) -> None:
        """Add a new data point to a metric's value list."""
        if not self.initialized or self._worker_start_timestamp is None:
            return

        metric_data.append_sample(value, self._worker_start_timestamp, self.polling_latency)

    def _add_metric_datetime(self) -> None:
        """Add the current worker timestamp while the manager lock is held."""
        if self.initialized and not self.replay_file and self._worker_start_timestamp:
            self.datetimes.append(self._worker_start_timestamp)

    @staticmethod
    def _as_utc(value: datetime) -> datetime:
        """Normalize an aware or naive-as-UTC datetime to aware UTC."""
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)

    @classmethod
    def _format_timestamp(cls, value: datetime) -> str:
        """Format an aware or naive-as-UTC datetime for metric history and replay."""
        return cls._as_utc(value).strftime(METRIC_DATETIME_FORMAT)

    def get_metric_source_data(self, metric_source: MetricSource) -> Mapping[str, object] | None:
        """Retrieve the raw data dictionary for a given MetricSource."""
        return self._metric_source_map.get(metric_source)

    def _iter_source_metric_values(self) -> Iterator[tuple[str, MetricData, MetricValue]]:
        """Yield available source values for metrics used by this connection."""
        for source, metric_tuples in self._source_to_metrics_processing.items():
            metric_source_data = self.get_metric_source_data(source)
            if metric_source_data is None:
                continue

            for metric_name, metric_data, connection_sources in metric_tuples:
                if self.connection_source not in connection_sources:
                    continue
                if metric_name not in metric_source_data:
                    metric_data.last_value = None
                    continue

                raw_value = metric_source_data[metric_name]
                value = raw_value if isinstance(raw_value, (int, float)) else coerce_int(raw_value)
                yield metric_name, metric_data, value

    def update_metrics_per_second_values(self) -> None:
        """Calculate new per-second values using the optimized lookup table."""
        for metric_name, metric_data, current_value in self._iter_source_metric_values():
            if metric_data.last_value is None:
                metric_data.last_value = current_value
                continue

            if metric_data.per_second_calculation:
                metric_diff = current_value - metric_data.last_value
                # Counters can decrease after a server restart or reset. Treat
                # that sample as a new baseline instead of graphing a negative rate.
                metric_value = (
                    metric_diff / self.polling_latency if metric_diff >= 0 and self.polling_latency > 0 else 0
                )
            else:
                metric_value = current_value

            if metric_data.smooth_extreme_values:
                recent_values, total_count = metric_data.recent_values(self.SMOOTHING_SAMPLE_COUNT)
                if total_count == 1 and recent_values[0] == 0:
                    metric_data.replace_latest_sample(metric_value)
                elif (
                    metric_value in self.EXTREME_VALUES
                    and abs(metric_value - (recent_values[-1] if recent_values else 0)) > self.SMOOTHING_THRESHOLD
                ):
                    if recent_values:
                        metric_value = sum(recent_values) / len(recent_values)

            self.add_metric(metric_data, metric_value)

    def update_metrics_last_value(self) -> None:
        """Update the last_value for all metrics using the optimized lookup table."""
        for _, metric_data, current_value in self._iter_source_metric_values():
            metric_data.last_value = current_value

    def update_proxysql_command_stats(self, proxysql_command_stats: list[dict[str, int | str]]) -> None:
        """Parse and aggregate ProxySQL command stats."""
        if self.connection_source != ConnectionSource.proxysql:
            return

        for row in proxysql_command_stats:
            if coerce_str(row.get("Command")) == "SELECT":
                # Mutate in place to preserve _metric_source_map reference
                self.proxysql_select_command_stats.update(
                    {key: int(value_text) for key, value in row.items() if (value_text := coerce_str(value)).isdigit()}
                )

            for key, value in row.items():
                value_text = coerce_str(value)
                if key.startswith("cnt_") and value_text.isdigit():
                    int_value = int(value_text)
                    self.proxysql_total_command_stats[key] = self.proxysql_total_command_stats.get(key, 0) + int_value

    def update_metrics_replication_lag(self) -> None:
        """Update the replication lag metric using the max lag across all channels."""
        if self.replication_status:
            max_lag = max(coerce_int(ch.get("Seconds_Behind")) for ch in self.replication_status)
        else:
            max_lag = 0
        self.add_metric(self.metrics.replication_lag.lag, max_lag)

    def update_metrics_adaptive_hash_index_hit_ratio(self) -> None:
        """Update the AHI hit ratio metric from its calculated value."""
        hit_ratio = self.calculate_ahi_ratio()
        if hit_ratio is not None:
            self.add_metric(self.metrics.adaptive_hash_index_hit_ratio.hit_ratio, hit_ratio)

    def update_metrics_checkpoint(self) -> None:
        """Update the checkpoint metric instance with max/sync flush values."""
        (max_age, sync_flush, _) = self.calculate_checkpoint_age_data()
        self.metrics.checkpoint.checkpoint_age_max = max_age
        self.metrics.checkpoint.checkpoint_age_sync_flush = sync_flush

    def update_metrics_locks(self) -> None:
        """Update the metadata lock count metric."""
        self.add_metric(self.metrics.locks.metadata_lock_count, len(self.metadata_lock_metrics))

    def calculate_checkpoint_age_data(self) -> tuple[int, int, int]:
        """Calculate raw checkpoint age data."""
        current_age = coerce_int(self.global_status.get("Innodb_checkpoint_age"))
        max_age = self.redo_log_size

        if max_age == 0:
            return 0, 0, 0

        sync_flush_age = round(max_age * self.CHECKPOINT_SYNC_FLUSH_RATIO)
        return max_age, sync_flush_age, current_age

    def get_formatted_checkpoint_age(self) -> str:
        """Get a color-formatted string for the checkpoint age percentage."""
        (max_age, _, current_age) = self.calculate_checkpoint_age_data()

        if current_age == 0 or max_age == 0:
            return "N/A"

        checkpoint_age_ratio = round(current_age / max_age * 100, 2)
        if checkpoint_age_ratio >= 80:
            color_code = "$red"
        elif checkpoint_age_ratio >= 60:
            color_code = "$yellow"
        else:
            color_code = "$green"
        return f"[{color_code}]{checkpoint_age_ratio}%"

    def calculate_ahi_ratio(self) -> float | None:
        """Calculate the smoothed Adaptive Hash Index hit ratio."""
        if self.global_variables.get("innodb_adaptive_hash_index") == "OFF":
            return None

        current_hits = coerce_int(self.innodb_metrics.get("adaptive_hash_searches"))
        current_misses = coerce_int(self.innodb_metrics.get("adaptive_hash_searches_btree"))

        last_hits = self.metrics.adaptive_hash_index.adaptive_hash_searches.last_value
        last_misses = self.metrics.adaptive_hash_index.adaptive_hash_searches_btree.last_value

        if last_hits is None or last_misses is None:
            return None

        hits = current_hits - last_hits
        misses = current_misses - last_misses
        total_hits_misses = hits + misses

        if total_hits_misses <= 0:
            return 0.0

        hit_ratio = (hits / total_hits_misses) * 100
        smoothed_hit_ratio = self.metrics.adaptive_hash_index_hit_ratio.smoothed_hit_ratio

        if smoothed_hit_ratio is None:
            smoothed_hit_ratio = hit_ratio
        else:
            smoothed_hit_ratio = (
                1 - self.AHI_SMOOTHING_FACTOR
            ) * smoothed_hit_ratio + self.AHI_SMOOTHING_FACTOR * hit_ratio

        self.metrics.adaptive_hash_index_hit_ratio.smoothed_hit_ratio = smoothed_hit_ratio
        return smoothed_hit_ratio

    def get_formatted_ahi_status(self) -> str:
        """Get a color-formatted string for the AHI status."""
        if self.global_variables.get("innodb_adaptive_hash_index") == "OFF":
            return "OFF"

        smoothed_hit_ratio: float | None = None
        if self.replay_file:
            smoothed_hit_ratio = self.metrics.adaptive_hash_index_hit_ratio.hit_ratio.latest_value()
        else:
            smoothed_hit_ratio = self.metrics.adaptive_hash_index_hit_ratio.smoothed_hit_ratio

        if smoothed_hit_ratio is None:
            return "N/A"
        if smoothed_hit_ratio <= 0.01:
            return "Inactive"

        if smoothed_hit_ratio > 70:
            color_code = "$green"
        elif smoothed_hit_ratio > 50:
            color_code = "$yellow"
        else:
            color_code = "$red"
        return f"[{color_code}]{smoothed_hit_ratio:.2f}%[/{color_code}]"

    def trim_datetimes_to_window(self, reference_time: datetime) -> bool:
        """Trim datetimes and metric values to the rolling window.

        Args:
            reference_time: The reference time to calculate the window from.

        Returns:
            True if any entries were trimmed, False otherwise.
        """
        with self._state_lock:
            return self._trim_datetimes_to_window(reference_time)

    def _trim_datetimes_to_window(self, reference_time: datetime) -> bool:
        """Trim history while the manager lock is held."""
        threshold = self._as_utc(reference_time) - timedelta(minutes=self.rolling_window_minutes)
        trimmed = False

        while self.datetimes:
            first_dt = parse_metric_datetime(self.datetimes[0])
            if first_dt is None:
                self.datetimes.popleft()
                trimmed = True
                continue

            if first_dt >= threshold:
                break

            self.datetimes.popleft()
            trimmed = True

        for metric_data in self._all_metrics_data_history:
            trimmed = metric_data.trim_before(threshold) or trimmed

        return trimmed
