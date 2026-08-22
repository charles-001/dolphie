from __future__ import annotations

import json
import os
import sqlite3
from collections import OrderedDict
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, TypeVar, cast

import orjson
import zstandard as zstd
from loguru import logger

from dolphie.DataTypes import (
    ConnectionSource,
    DatabaseRow,
    DatabaseScalar,
    ProcesslistThread,
    ProxySQLProcesslistThread,
    ReplicaRow,
)
from dolphie.Dolphie import Dolphie
from dolphie.Modules import MetricManager
from dolphie.Modules.Functions import coerce_int, coerce_str, format_bytes, minify_query
from dolphie.Modules.PerformanceSchemaMetrics import PerformanceSchemaMetrics


@dataclass
class MySQLReplayData:
    timestamp: str
    system_utilization: dict[str, int | float | tuple[float, float, float]]
    global_status: DatabaseRow
    global_variables: DatabaseRow
    binlog_status: DatabaseRow
    innodb_metrics: DatabaseRow
    replica_manager: list[ReplicaRow]
    replication_status: list[DatabaseRow]
    replication_applier_status: dict[str, dict[str, Any]]
    processlist: dict[int, ProcesslistThread]
    metric_manager: dict[str, Any]
    metadata_locks: list[DatabaseRow]
    file_io_data: PerformanceSchemaMetrics
    table_io_waits_data: PerformanceSchemaMetrics
    statements_summary_data: PerformanceSchemaMetrics
    group_replication_data: DatabaseRow
    group_replication_members: list[DatabaseRow]
    clusterset_instances: list[DatabaseRow]
    galera_cluster_members: list[DatabaseRow]


@dataclass
class ProxySQLReplayData:
    timestamp: str
    system_utilization: dict[str, int | float | tuple[float, float, float]]
    global_status: DatabaseRow
    global_variables: DatabaseRow
    command_stats: list[DatabaseRow]
    hostgroup_summary: list[DatabaseRow]
    processlist: dict[int, ProxySQLProcesslistThread]
    metric_manager: dict[str, Any]


ProcesslistThreadType = TypeVar("ProcesslistThreadType", ProcesslistThread, ProxySQLProcesslistThread)


class ReplayManager:
    """ReplayManager class for capturing and replaying Dolphie instance states."""

    # Constants
    PURGE_CHECK_INTERVAL_HOURS = 1
    COMPRESSION_DICT_SIZE = 10 * 1024 * 1024  # 10MB
    COMPRESSION_LEVEL = 5
    COMPRESSION_DICT_SAMPLES = 10
    # Floor for the number of decompressed metric_manager payloads kept in memory to
    # speed up backward/seek navigation. The effective cap scales with the rolling
    # window so a single window always fits (see fetch_delta_metrics_for_window).
    METRIC_WINDOW_CACHE_SIZE = 1500

    def __init__(self, dolphie: Dolphie):
        """Initializes the ReplayManager with Dolphie instance and SQLite database settings.

        Args:
            dolphie: The Dolphie instance.
        """
        self.dolphie = dolphie
        # We will increment this to force a new replay file if the schema changes in future versions
        self.schema_version: int = 2
        self.connection: sqlite3.Connection | None = None
        self.current_replay_id: int = 0  # This is used to keep track of the last primary key read from the database
        self.min_replay_id: int = 0
        self.max_replay_id: int = 0
        self.current_replay_timestamp: str | None = None  # Only used for dashboard replay section
        self.min_replay_timestamp: str | None = None
        self.max_replay_timestamp: str | None = None
        self.total_replay_rows: int = 0
        self._replay_metadata_change_token: tuple[int, int] | None = None
        self.last_purge_time = datetime.now().astimezone() - timedelta(
            hours=self.PURGE_CHECK_INTERVAL_HOURS
        )  # Initialize to an hour ago
        self.replay_file_size: int = 0
        self.dict_samples: list[bytes] = []
        self.global_variable_change_ids: list[int] = []

        # Cache of decompressed metric_manager payloads keyed by replay id. Row data is
        # immutable for a given id, so entries never go stale; consecutive backward/seek
        # steps reuse the overlapping window instead of re-decompressing every row.
        self._metric_window_cache: OrderedDict[int, dict[str, Any]] = OrderedDict()
        # Effective cache cap, grown to fit the rolling window by fetch_delta_metrics_for_window.
        self._metric_window_cap: int = self.METRIC_WINDOW_CACHE_SIZE

        self._compression_dict: zstd.ZstdCompressionDict | None = None
        self._compressor: zstd.ZstdCompressor
        self._decompressor: zstd.ZstdDecompressor

        # Initialize compressor/decompressor without a dictionary
        self._rebuild_compressor_decompressor()

        # Determine filename used for replay file
        hostname = f"{dolphie.host}_{dolphie.port}"
        if dolphie.replay_file:
            self.replay_file = dolphie.replay_file
        elif dolphie.daemon_mode:
            self.replay_file = f"{dolphie.replay_dir}/{hostname}/daemon.db"
        elif dolphie.record_for_replay:
            timestamp = datetime.now().astimezone().strftime("%Y_%m_%d_%H_%M_%S")
            self.replay_file = f"{dolphie.replay_dir}/{hostname}/{timestamp}.db"
            dolphie.app.notify(
                f"File: [$highlight]{self.replay_file}[/$highlight]",
                title="Recording data",
                timeout=10,
            )
        else:
            # No options specified for replaying, skip initialization
            return

        os.makedirs(os.path.dirname(self.replay_file), mode=0o770, exist_ok=True)
        logger.info(f"Replay SQLite file: {self.replay_file} ({self.dolphie.replay_retention_hours} hours retention)")

        self._initialize_sqlite()
        self._manage_metadata()

    @property
    def compression_dict(self) -> zstd.ZstdCompressionDict | None:
        return self._compression_dict

    @compression_dict.setter
    def compression_dict(self, value: zstd.ZstdCompressionDict | None):
        self._compression_dict = value
        self._rebuild_compressor_decompressor()

    def _rebuild_compressor_decompressor(self):
        """Rebuilds the cached compressor and decompressor with the current compression dictionary."""
        self._compressor = zstd.ZstdCompressor(level=self.COMPRESSION_LEVEL, dict_data=self._compression_dict)
        self._decompressor = zstd.ZstdDecompressor(dict_data=self._compression_dict)

    def _begin_transaction(self) -> None:
        """Begins an immediate transaction for write operations."""
        with closing(self._get_connection().cursor()) as cursor:
            cursor.execute("BEGIN IMMEDIATE")

    def _commit_transaction(self) -> None:
        """Commits the current transaction."""
        with closing(self._get_connection().cursor()) as cursor:
            cursor.execute("COMMIT")

    def _rollback_transaction(self) -> None:
        """Rolls back the current transaction."""
        with closing(self._get_connection().cursor()) as cursor:
            cursor.execute("ROLLBACK")

    def _get_connection(self) -> sqlite3.Connection:
        """Return the initialized replay connection."""
        if self.connection is None:
            raise RuntimeError("Replay database is not initialized")
        return self.connection

    def close(self) -> None:
        """Close the underlying SQLite connection, if open."""
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    def _execute_select_one(self, query: str, params: tuple[Any, ...] = ()) -> tuple[Any, ...] | None:
        """Executes a SELECT query and returns a single row.

        Args:
            query: The SQL SELECT query to execute.
            params: The parameters to bind to the query.

        Returns:
            Optional[Tuple[Any, ...]]: A single row or None if no results.

        Raises:
            sqlite3.Error: If the query execution fails.
        """
        try:
            with closing(self._get_connection().cursor()) as cursor:
                cursor.execute(query, params)
                return cursor.fetchone()
        except sqlite3.Error as e:
            logger.error(f"Error executing SQLite SELECT query: {e}")
            self.dolphie.app.notify(
                f"Query: {query}\n{e}",
                title="Error executing SQLite query",
                severity="error",
            )
            raise

    def _execute_select_all(self, query: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
        """Executes a SELECT query and returns all rows.

        Args:
            query: The SQL SELECT query to execute.
            params: The parameters to bind to the query.

        Returns:
            List[Tuple[Any, ...]]: A list of rows.

        Raises:
            sqlite3.Error: If the query execution fails.
        """
        try:
            with closing(self._get_connection().cursor()) as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()
        except sqlite3.Error as e:
            logger.error(f"Error executing SQLite SELECT query: {e}")
            self.dolphie.app.notify(
                f"Query: {query}\n{e}",
                title="Error executing SQLite query",
                severity="error",
            )
            raise

    def _execute_insert(self, query: str, params: tuple[Any, ...] = ()) -> int:
        """Executes an INSERT query and returns the last inserted row ID.

        Args:
            query: The SQL INSERT query to execute.
            params: The parameters to bind to the query.

        Returns:
            int: The last inserted row ID.

        Raises:
            sqlite3.Error: If the query execution fails.
        """
        try:
            with closing(self._get_connection().cursor()) as cursor:
                cursor.execute(query, params)
                if cursor.lastrowid is None:
                    raise RuntimeError("SQLite insert did not return a row ID")
                return cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error executing SQLite INSERT query: {e}")
            self.dolphie.app.notify(
                f"Query: {query}\n{e}",
                title="Error executing SQLite query",
                severity="error",
            )
            raise

    def _execute_modify(self, query: str, params: tuple[Any, ...] = ()) -> int:
        """Executes an UPDATE or DELETE query and returns the number of affected rows.

        Args:
            query: The SQL UPDATE or DELETE query to execute.
            params: The parameters to bind to the query.

        Returns:
            int: The number of affected rows.

        Raises:
            sqlite3.Error: If the query execution fails.
        """
        try:
            with closing(self._get_connection().cursor()) as cursor:
                cursor.execute(query, params)
                return cursor.rowcount
        except sqlite3.Error as e:
            logger.error(f"Error executing SQLite UPDATE/DELETE query: {e}")
            self.dolphie.app.notify(
                f"Query: {query}\n{e}",
                title="Error executing SQLite query",
                severity="error",
            )
            raise

    def _execute_many(self, query: str, params: list[tuple[Any, ...]]) -> int:
        """Executes a batch of queries and returns the number of affected rows.

        Args:
            query: The SQL query to execute.
            params: A list of parameter tuples for batch execution.

        Returns:
            int: The number of affected rows.

        Raises:
            sqlite3.Error: If the query execution fails.
        """
        try:
            with closing(self._get_connection().cursor()) as cursor:
                cursor.executemany(query, params)
                return cursor.rowcount
        except sqlite3.Error as e:
            logger.error(f"Error executing SQLite batch query: {e}")
            self.dolphie.app.notify(
                f"Query: {query}\n{e}",
                title="Error executing SQLite query",
                severity="error",
            )
            raise

    def _initialize_sqlite(self):
        """Initializes the SQLite database and creates the necessary tables."""
        database_exists = bool(os.path.exists(self.replay_file))

        self.connection = sqlite3.connect(self.replay_file, isolation_level=None, check_same_thread=False)

        # Lock down the permissions of the replay file
        os.chmod(self.replay_file, 0o660)

        if not database_exists:
            logger.info("Created new SQLite database and connected to it")
        else:
            logger.info("Connected to SQLite")

        # Create replay_data table if it doesn't exist
        self._execute_modify(
            """
            CREATE TABLE IF NOT EXISTS replay_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME,
                data BLOB
            )"""
        )
        self._execute_modify("CREATE INDEX IF NOT EXISTS idx_replay_data_timestamp ON replay_data (timestamp)")

        # Create metadata table if it doesn't exist
        self._execute_modify(
            """
            CREATE TABLE IF NOT EXISTS metadata (
                schema_version INTEGER DEFAULT 1,
                host VARCHAR(255),
                port INTEGER,
                host_distro VARCHAR(255),
                connection_source VARCHAR(255),
                dolphie_version VARCHAR(255),
                compression_dict BLOB
            )"""
        )

        # Create variable_changes table if it doesn't exist
        self._execute_modify(
            """
            CREATE TABLE IF NOT EXISTS variable_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                replay_id INTEGER,
                timestamp DATETIME,
                variable_name VARCHAR(255),
                old_value VARCHAR(255),
                new_value VARCHAR(255)
            )"""
        )
        self._execute_modify(
            "CREATE INDEX IF NOT EXISTS idx_variable_changes_timestamp ON variable_changes (timestamp)"
        )
        self._execute_modify(
            "CREATE INDEX IF NOT EXISTS idx_variable_changes_replay_id ON variable_changes (replay_id)"
        )

        # Enable auto-vacuum if it's not already enabled. This will help keep the database file size down.
        result = self._execute_select_one("PRAGMA auto_vacuum")
        if result and result[0] != 1:
            self._execute_modify("PRAGMA auto_vacuum = FULL")
            self._execute_modify("VACUUM")

        self.purge_old_data()

    def purge_old_data(self):
        """Purges data older than the retention period specified by hours_of_retention.
        Only runs if at least an hour has passed since the last purge.
        """
        # Don't purge when not recording, or when loading a replay file (read-only mode)
        if not self.dolphie.record_for_replay or self.dolphie.replay_file:
            return

        current_time = datetime.now().astimezone()
        if (current_time - self.last_purge_time) < timedelta(hours=self.PURGE_CHECK_INTERVAL_HOURS):
            return  # Skip purging if less than an hour has passed

        retention_date = (current_time - timedelta(hours=self.dolphie.replay_retention_hours)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        self._execute_modify("DELETE FROM replay_data WHERE timestamp < ?", (retention_date,))
        self._execute_modify("DELETE FROM variable_changes WHERE timestamp < ?", (retention_date,))

        self.last_purge_time = current_time

    def seek_relative(self, offset: int) -> bool:
        """Moves the replay cursor by ``offset`` rows. Gap-safe and range-clamped.

        Negative moves backward, positive forward. If fewer than ``abs(offset)`` rows
        exist in that direction, the cursor clamps to the first/last row. Used by the
        Back/Forward actions, which accelerate the offset while the key is held.

        Args:
            offset: Number of rows to move (negative = backward, positive = forward).

        Returns:
            bool: True if a target row exists and the cursor moved, False otherwise.
        """
        if offset == 0:
            return False

        if offset < 0:
            row = self._execute_select_one(
                "SELECT id, timestamp FROM replay_data WHERE id < ? ORDER BY id DESC LIMIT 1 OFFSET ?",
                (self.current_replay_id, -offset - 1),
            )
            if not row:
                # Fewer than |offset| rows behind; clamp to the earliest row.
                row = self._execute_select_one(
                    "SELECT id, timestamp FROM replay_data WHERE id < ? ORDER BY id LIMIT 1",
                    (self.current_replay_id,),
                )
        else:
            row = self._execute_select_one(
                "SELECT id, timestamp FROM replay_data WHERE id > ? ORDER BY id LIMIT 1 OFFSET ?",
                (self.current_replay_id, offset - 1),
            )
            if not row:
                # Fewer than offset rows ahead; clamp to the latest row.
                row = self._execute_select_one(
                    "SELECT id, timestamp FROM replay_data WHERE id > ? ORDER BY id DESC LIMIT 1",
                    (self.current_replay_id,),
                )

        if not row:
            return False

        # Land one before the target so _load_and_parse_replay_data (WHERE id > ?) picks it up
        self.current_replay_id = row[0] - 1
        self.current_replay_timestamp = row[1]
        return True

    def seek_to_timestamp(self, timestamp: str):
        """Seeks to the specified timestamp in the SQLite database.

        Args:
            timestamp: The timestamp to seek to.
        """
        row = self._execute_select_one(
            "SELECT id, timestamp FROM replay_data WHERE timestamp <= ? ORDER BY timestamp DESC LIMIT 1",
            (timestamp,),
        )
        if not row:
            self.dolphie.app.notify(
                f"No timestamps found on or before [$light_blue]{timestamp}[/$light_blue]",
                severity="error",
                timeout=10,
            )
            return False

        # Set to one before the target so _load_and_parse_replay_data (WHERE id > ?) picks it up
        self.current_replay_id = row[0] - 1
        found_timestamp = row[1]

        if found_timestamp == timestamp:
            self.dolphie.app.notify(
                f"Seeking to timestamp [$light_blue]{timestamp}[/$light_blue]",
                severity="information",
                timeout=10,
            )
        else:
            self.dolphie.app.notify(
                f"Timestamp not found, seeking to closest timestamp [$light_blue]{found_timestamp}[/$light_blue]",
                timeout=10,
            )

        return True

    def _create_new_replay_file(self, new_replay_file: str):
        logger.info(f"Renaming replay file to: {new_replay_file}")

        os.rename(self.replay_file, new_replay_file)

        # Reset compression dict if it's already been set or else the replay file will be corrupted
        self.compression_dict = None

        self._initialize_sqlite()
        self._manage_metadata()

    def _manage_metadata(self):
        """Manages the metadata table with information we care about."""
        # Don't manage metadata when not recording, or when loading a replay file (read-only mode)
        if not self.dolphie.record_for_replay or self.dolphie.replay_file:
            return

        row = self._execute_select_one("SELECT * FROM metadata")
        if row is None:
            self._execute_insert(
                "INSERT INTO metadata (schema_version, host, port, host_distro, connection_source, dolphie_version)"
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    self.schema_version,
                    self.dolphie.host,
                    self.dolphie.port,
                    self.dolphie.host_distro,
                    self.dolphie.connection_source,
                    self.dolphie.app_version,
                ),
            )
        else:
            schema_version = row[0]
            if self.dolphie.daemon_mode and schema_version != self.schema_version:
                new_replay_file = f"{self.replay_file}_old_schema_v{schema_version}"
                logger.warning(
                    f"The schema version of the replay file ({schema_version}) differs from this version "
                    f"of Dolphie's schema version ({self.schema_version}). To avoid potential issues, the "
                    f"replay file will be renamed and a new one will be created"
                )

                self._create_new_replay_file(new_replay_file)

                return

            connection_source = row[4]
            if self.dolphie.daemon_mode:
                # Avoid mixing connection sources in the same replay file
                if connection_source != self.dolphie.connection_source:
                    logger.critical(
                        f"The connection source of the daemon's replay file ({connection_source}) "
                        f"differs from the current connection source ({self.dolphie.connection_source}). "
                        "You should never mix connection sources in the same replay file. Please rename "
                        "the daemon's replay file and restart the daemon"
                    )

            host = row[1]
            port = row[2]
            # Add the host's distro to the metadata if it's different than the connection source
            host_distro = f" ({row[3]})" if connection_source != row[3] else ""
            app_version = row[5]
            compress_dict = row[6]

            logger.info(
                f"Replay database metadata - Host: {host}, Port: {port}, Source: {connection_source}{host_distro}, "
                f"Dolphie: {app_version}"
            )

            if compress_dict:
                self.compression_dict = zstd.ZstdCompressionDict(compress_dict)
                logger.info(
                    f"ZSTD compression dictionary loaded (size: {format_bytes(len(compress_dict), color=False)})"
                )

    def verify_replay_file(self):
        """Verifies that the replay file has data to replay and that the schema version matches."""
        if not self.dolphie.replay_file:
            return

        return self._get_replay_file_metadata() and self._verify_replay_has_data()

    def _get_replay_file_metadata(self):
        """Retrieves the replay's metadata from the metadata table.

        Returns:
            bool: True if metadata is found and schema matches; False otherwise.
        """
        row = self._execute_select_one("SELECT * FROM metadata")
        if not row:
            self._notify_error("Metadata not found in replay file", "Error reading replay file")
            return False

        schema_version = row[0]
        if schema_version != self.schema_version:
            self._notify_error(
                f"The schema version of the replay file ({schema_version}) differs from Dolphie's schema version "
                f"({self.schema_version}). Use a compatible version of Dolphie to replay this file",
                "Schema version mismatch",
            )
            return False

        (
            self.dolphie.host,
            self.dolphie.port,
            self.dolphie.host_distro,
            self.dolphie.connection_source,
        ) = row[1:5]
        self.dolphie.host_with_port = f"{self.dolphie.host}:{self.dolphie.port}"

        if row[6]:
            self.compression_dict = zstd.ZstdCompressionDict(row[6])

        return True

    def _verify_replay_has_data(self):
        """Verifies that the replay file has data to replay.

        Returns:
            bool: True if data is found, False if not.
        """
        row = self._execute_select_one("SELECT COUNT(*) FROM replay_data")
        if row and row[0] == 0:
            self._notify_error("File has no data to replay", "No replay data found")
            return False

        return True

    def _notify_error(self, message, title):
        """Helper method to display error notifications."""
        self.dolphie.app.notify(
            f"[b]Replay file[/b]: [$highlight]{self.replay_file}[/$highlight]\n{message}",
            title=title,
            severity="error",
            timeout=10,
        )

    def _train_compression_dict(self) -> zstd.ZstdCompressionDict:
        """Creates a compression dictionary based on sample data to help with better compression.

        Returns:
            bytes: The created compression dictionary.
        """
        compression_dict = zstd.train_dictionary(
            self.COMPRESSION_DICT_SIZE,
            # zstandard's stub wants list[bytes | bytearray | memoryview]; our samples are always bytes.
            cast(list[bytes | bytearray | memoryview], self.dict_samples),
            level=self.COMPRESSION_LEVEL,
        )

        logger.info(
            f"ZSTD compression dictionary trained with {len(self.dict_samples)} samples "
            f"(size: {format_bytes(len(compression_dict), color=False)})"
        )

        # Store the compression dictionary in the metadata table to be used with decompression
        self._execute_modify("UPDATE metadata SET compression_dict = ?", (compression_dict.as_bytes(),))

        return compression_dict

    def _condition_metrics(self, metric_manager: MetricManager.MetricManager) -> dict[str, Any]:
        """Captures the metrics from the metric manager and returns them in a structured format.

        In daemon mode, only the latest value per metric is stored (delta format) to avoid
        serializing the full 10-minute history on every cycle. The replay player detects the
        _delta flag and accumulates values during playback.

        Args:
            metric_manager: The metric manager to capture metrics from.

        Returns:
            dict: A dictionary of captured metrics.
        """
        daemon_mode = self.dolphie.daemon_mode
        connection_source = self.dolphie.connection_source
        datetimes, metric_history = metric_manager.snapshot_history(
            connection_source,
            latest_only=daemon_mode,
        )

        if daemon_mode:
            metrics: dict[str, Any] = {
                "datetimes": datetimes,
                "_delta": True,
            }
        else:
            metrics = {"datetimes": datetimes}

        for metric_instance_name, series_history in metric_history:
            metric_entry = metrics.setdefault(metric_instance_name, {})
            for metric_name, values in series_history:
                metric_entry[metric_name] = values

        return metrics

    def _prepare_processlist(self) -> list[DatabaseRow]:
        """Prepares the processlist data by extracting thread data and minifying queries.

        Returns:
            list: A list of processlist thread dictionaries with minified queries.
        """
        return [
            {**thread_data, "query": minify_query(coerce_str(thread_data["query"]))}
            if "query" in thread_data
            else thread_data
            for thread_data in (v.thread_data for v in self.dolphie.processlist_threads.values())
        ]

    def _build_base_data_dict(self, processlist: list[DatabaseRow]) -> dict[str, Any]:
        """Builds the base data dictionary with common data for all connection sources.

        Args:
            processlist: The prepared processlist data.

        Returns:
            dict: The base data dictionary.
        """
        data_dict = {
            "global_status": self.dolphie.global_status,
            "global_variables": self.dolphie.global_variables,
            "processlist": processlist,
            "metric_manager": self._condition_metrics(self.dolphie.metric_manager),
        }

        data_dict["global_status"]["replay_polling_latency"] = self.dolphie.worker_processing_time

        if self.dolphie.system_utilization:
            data_dict["system_utilization"] = self.dolphie.system_utilization

        return data_dict

    def _add_mysql_specific_data(self, data_dict: dict[str, Any]) -> None:
        """Adds MySQL-specific data to the data dictionary.

        Args:
            data_dict: The data dictionary to update.
        """
        # Add the replay_pfs_metrics_last_reset_time to the global status dictionary
        if self.dolphie.pfs_metrics_last_reset_time:
            data_dict["global_status"]["replay_pfs_metrics_last_reset_time"] = (
                datetime.now().astimezone().timestamp() - self.dolphie.pfs_metrics_last_reset_time.timestamp()
            )
        else:
            data_dict["global_status"]["replay_pfs_metrics_last_reset_time"] = 0

        # Add MySQL specific data to the dictionary
        data_dict.update(
            {
                "binlog_status": self.dolphie.binlog_status,
                "innodb_metrics": self.dolphie.innodb_metrics,
                "metadata_locks": self.dolphie.metadata_locks,
            }
        )

        if self.dolphie.replication_status:
            data_dict["replication_status"] = self.dolphie.replication_status

        if self.dolphie.replication_applier_status:
            data_dict["replication_applier_status"] = self.dolphie.replication_applier_status

        available_replicas = self.dolphie.replica_manager.available_replicas
        if available_replicas:
            data_dict["replica_manager"] = available_replicas

        if self.dolphie.group_replication or self.dolphie.innodb_cluster or self.dolphie.innodb_cluster_read_replica:
            data_dict.update(
                {
                    "group_replication_data": self.dolphie.group_replication_data,
                    "group_replication_members": self.dolphie.group_replication_members,
                    "clusterset_instances": self.dolphie.clusterset_instances,
                }
            )

        if self.dolphie.galera_cluster:
            data_dict["galera_cluster_members"] = self.dolphie.galera_cluster_members

        if self.dolphie.file_io_data and self.dolphie.file_io_data.filtered_data:
            data_dict["file_io_data"] = self.dolphie.file_io_data.filtered_data

        if self.dolphie.table_io_waits_data and self.dolphie.table_io_waits_data.filtered_data:
            data_dict["table_io_waits_data"] = self.dolphie.table_io_waits_data.filtered_data

        if self.dolphie.statements_summary_data and self.dolphie.statements_summary_data.filtered_data:
            data_dict["statements_summary_data"] = self.dolphie.statements_summary_data.filtered_data

    def _serialize_data_dict(self, data_dict: dict[str, Any]) -> bytes:
        """Serializes the data dictionary to bytes using orjson or json as fallback.

        Args:
            data_dict: The data dictionary to serialize.

        Returns:
            bytes: The serialized data.
        """
        # For large numbers, we need to use json instead of orjson to serialize the data
        # to avoid exceeding 64-bit integer limit
        # https://github.com/ijl/orjson/issues/301
        try:
            return orjson.dumps(data_dict)
        except TypeError as e:
            if str(e) == "Integer exceeds 64-bit range":
                return json.dumps(data_dict).encode()
            raise

    def _handle_compression_training(self, data_dict_bytes: bytes) -> None:
        """Handles compression dictionary training by collecting samples and training when ready.

        Args:
            data_dict_bytes: The serialized data to use as a training sample.
        """
        if not self.compression_dict:
            if len(self.dict_samples) < self.COMPRESSION_DICT_SAMPLES:
                self.dict_samples.append(data_dict_bytes)
            else:
                self.compression_dict = self._train_compression_dict()
                # Remove the samples to save memory
                del self.dict_samples

    def _insert_replay_data(self, timestamp: str, data_dict_bytes: bytes) -> None:
        """Inserts the replay data into the database and handles variable change linkage.

        Args:
            timestamp: The timestamp of the capture.
            data_dict_bytes: The serialized and compressed data to insert.
        """
        try:
            # Begin transaction for atomic insert and update
            self._begin_transaction()

            # Execute the SQL insert using the constructed dictionary
            self.current_replay_id = self._execute_insert(
                "INSERT INTO replay_data (timestamp, data) VALUES (?, ?)",
                (
                    timestamp,
                    self._compressor.compress(data_dict_bytes),
                ),
            )

            # Update the variable_changes table with the data of the replay row so they're linked
            if self.global_variable_change_ids:
                self._execute_many(
                    "UPDATE variable_changes SET replay_id = ?, timestamp = ? WHERE id = ?",
                    [(self.current_replay_id, timestamp, id) for id in self.global_variable_change_ids],
                )

                # Clear the list of global variable change IDs now that they've been linked
                self.global_variable_change_ids = []

            # Commit the transaction
            self._commit_transaction()

        except Exception as e:
            # Rollback on any error
            self._rollback_transaction()
            logger.error(f"Error inserting replay data: {e}")
            raise

        self.purge_old_data()

        if not self.dolphie.daemon_mode:
            self.replay_file_size = os.path.getsize(self.replay_file)

    def capture_state(self):
        """Captures the current state of the Dolphie instance and stores it in the SQLite database."""
        # Don't capture when not recording, or when loading a replay file (read-only mode)
        if not self.dolphie.record_for_replay or self.dolphie.replay_file:
            return

        # Prepare processlist data
        processlist = self._prepare_processlist()
        timestamp = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S")

        # Build base data dictionary
        data_dict = self._build_base_data_dict(processlist)

        # Add connection-source specific data
        if self.dolphie.connection_source == ConnectionSource.mysql:
            self._add_mysql_specific_data(data_dict)
        else:
            data_dict.update(
                {
                    "command_stats": self.dolphie.proxysql_command_stats,
                    "hostgroup_summary": self.dolphie.proxysql_hostgroup_summary,
                }
            )

        # Serialize and compress the data
        data_dict_bytes = self._serialize_data_dict(data_dict)
        self._handle_compression_training(data_dict_bytes)

        # Insert into database
        self._insert_replay_data(timestamp, data_dict_bytes)

    def _update_replay_metadata_cache(self) -> bool:
        """Updates the replay metadata (min/max timestamps and IDs, total rows).

        SQLite's data version detects commits from another connection, while total_changes
        detects writes on this connection. This keeps static replay navigation cheap while
        refreshing both boundaries after appends or retention purges.

        total_replay_rows is derived from the ID range since IDs are contiguous
        (rows are only ever purged from the beginning).

        Returns:
            bool: True if metadata was successfully updated, False otherwise.
        """
        connection = self._get_connection()
        data_version_row = self._execute_select_one("PRAGMA data_version")
        if data_version_row is None:
            return False

        change_token = (int(data_version_row[0]), connection.total_changes)
        if change_token == self._replay_metadata_change_token and self.min_replay_id:
            return True

        max_row = self._execute_select_one("SELECT id, timestamp FROM replay_data ORDER BY id DESC LIMIT 1")
        if not max_row:
            return False

        min_row = self._execute_select_one("SELECT id, timestamp FROM replay_data ORDER BY id LIMIT 1")
        if min_row is None:
            return False

        self.min_replay_id = min_row[0]
        self.min_replay_timestamp = min_row[1]
        self.max_replay_id = max_row[0]
        self.max_replay_timestamp = max_row[1]
        self.total_replay_rows = self.max_replay_id - self.min_replay_id + 1
        self._replay_metadata_change_token = change_token

        return True

    def _load_and_parse_replay_data(self) -> tuple[str, dict] | None:
        """Loads the next replay data row from the database and parses it.

        Returns:
            Optional[Tuple[str, dict]]: A tuple of (timestamp, data_dict) or None if no data available.
        """
        # Get the next row
        row = self._execute_select_one(
            "SELECT id, timestamp, data FROM replay_data WHERE id > ? ORDER BY id LIMIT 1",
            (self.current_replay_id,),
        )
        if not row:
            return None

        self.current_replay_id = row[0]
        self.current_replay_timestamp = row[1]

        # Decompress and parse the JSON data
        try:
            data = orjson.loads(self._decompressor.decompress(row[2]))

            # Warm the window cache with this row's metric_manager so a later backward or
            # seek over rows we've already played (e.g. after forward auto-play) reuses
            # this decompression instead of redoing it. Only delta rows are small enough
            # to be worth keeping; full-snapshot rows (interactive recordings) are skipped.
            metric_manager = data.get("metric_manager")
            if metric_manager and metric_manager.get("_delta"):
                self._remember_metric_manager(self.current_replay_id, metric_manager)

            return row[1], data
        except Exception as e:
            self.dolphie.app.notify(str(e), title="Error parsing replay data", severity="error")
            return None

    def _build_processlist_from_data(
        self,
        processlist_data: list[DatabaseRow],
        thread_class: type[ProcesslistThreadType],
    ) -> dict[int, ProcesslistThreadType]:
        """Builds a processlist dictionary from raw data using the specified thread class.

        Args:
            processlist_data: List of thread data dictionaries.
            thread_class: The class to use for creating thread objects (ProcesslistThread or ProxySQLProcesslistThread).

        Returns:
            dict: Dictionary mapping thread IDs to thread objects.
        """
        return {coerce_int(thread_data["id"]): thread_class(thread_data) for thread_data in processlist_data}

    def _create_mysql_replay_data(self, timestamp: str, data: dict[str, Any]) -> MySQLReplayData:
        """Creates a MySQLReplayData object from parsed replay data.

        Args:
            timestamp: The timestamp of the replay data.
            data: The parsed data dictionary.

        Returns:
            MySQLReplayData: The constructed replay data object.
        """
        processlist = self._build_processlist_from_data(data["processlist"], ProcesslistThread)

        # Create Performance Schema metrics objects
        file_io_data = PerformanceSchemaMetrics([], "file_io", "FILE_NAME")
        file_io_data.filtered_data = data.get("file_io_data", {})

        table_io_waits = PerformanceSchemaMetrics([], "table_io", "OBJECT_TABLE")
        table_io_waits.filtered_data = data.get("table_io_waits_data", {})

        statements_summary_data = PerformanceSchemaMetrics([], "statements_summary", "digest")
        statements_summary_data.filtered_data = data.get("statements_summary_data", {})

        return MySQLReplayData(
            timestamp=timestamp,
            system_utilization=data.get("system_utilization", {}),
            global_status=data.get("global_status", {}),
            global_variables=data.get("global_variables", {}),
            metric_manager=data.get("metric_manager", {}),
            binlog_status=data.get("binlog_status", {}),
            innodb_metrics=data.get("innodb_metrics", {}),
            replica_manager=data.get("replica_manager", []),
            replication_status=self._migrate_replication_status(data.get("replication_status", [])),
            replication_applier_status=self._migrate_replication_applier_status(
                data.get("replication_applier_status", {})
            ),
            metadata_locks=data.get("metadata_locks", []),
            processlist=processlist,
            group_replication_data=data.get("group_replication_data", {}),
            group_replication_members=data.get("group_replication_members", []),
            clusterset_instances=data.get("clusterset_instances", []),
            galera_cluster_members=data.get("galera_cluster_members", []),
            file_io_data=file_io_data,
            table_io_waits_data=table_io_waits,
            statements_summary_data=statements_summary_data,
        )

    @staticmethod
    def _migrate_replication_applier_status(value: object) -> dict[str, dict[str, Any]]:
        """Handle backward compatibility: old replay files store applier status as a flat dict."""
        if isinstance(value, dict) and "data" in value:
            return {"": value}
        return value if isinstance(value, dict) else {}

    @staticmethod
    def _migrate_replication_status(value: object) -> list[DatabaseRow]:
        """Handle backward compatibility: old replay files store replication_status as a dict."""
        if isinstance(value, dict):
            return [value] if value else []
        return value if isinstance(value, list) else []

    def _create_proxysql_replay_data(self, timestamp: str, data: dict[str, Any]) -> ProxySQLReplayData:
        """Creates a ProxySQLReplayData object from parsed replay data.

        Args:
            timestamp: The timestamp of the replay data.
            data: The parsed data dictionary.

        Returns:
            ProxySQLReplayData: The constructed replay data object.
        """
        processlist = self._build_processlist_from_data(data["processlist"], ProxySQLProcesslistThread)

        return ProxySQLReplayData(
            timestamp=timestamp,
            system_utilization=data.get("system_utilization", {}),
            global_status=data.get("global_status", {}),
            global_variables=data.get("global_variables", {}),
            metric_manager=data.get("metric_manager", {}),
            command_stats=data.get("command_stats", []),
            hostgroup_summary=data.get("hostgroup_summary", []),
            processlist=processlist,
        )

    def get_next_refresh_interval(
        self,
    ) -> MySQLReplayData | ProxySQLReplayData | None:
        """Gets the next refresh interval's data from the SQLite database and returns it as a ReplayData object.

        Returns:
            ReplayData: The next replay data.
        """
        # Always update metadata cache when replaying to account for new data being recorded
        # This ensures max_replay_timestamp and max_replay_id reflect the latest state
        if not self._update_replay_metadata_cache():
            return None

        # Load and parse the next replay data
        result = self._load_and_parse_replay_data()
        if not result:
            return None

        timestamp, data = result

        # Create and return the appropriate replay data object based on connection source
        if self.dolphie.connection_source == ConnectionSource.mysql:
            return self._create_mysql_replay_data(timestamp, data)
        elif self.dolphie.connection_source == ConnectionSource.proxysql:
            return self._create_proxysql_replay_data(timestamp, data)
        else:
            self.dolphie.app.notify("Invalid connection source for replay data", severity="error")
            return None

    def fetch_delta_metrics_for_window(self, target_id: int, window_minutes: int) -> list[dict[str, Any]]:
        """Fetches metric_manager deltas for the time window ending at the target replay ID.

        This is used during replay seek/backward navigation in delta mode to rebuild
        the configured rolling metric window up to the target position.

        Args:
            target_id: The replay ID to build the window up to.
            window_minutes: The size of the rolling window in minutes.

        Returns:
            list[dict]: A list of metric_manager dicts from oldest to newest within the window.
        """
        if not self.current_replay_timestamp:
            return []

        # Fetch only the row ids in the window first. Selecting just the id lets SQLite
        # answer from the timestamp index without reading the (large) data blobs, so rows
        # we've already decompressed on a previous step cost nothing here.
        if window_minutes > 0:
            target_dt = datetime.fromisoformat(self.current_replay_timestamp)
            window_start = (target_dt - timedelta(minutes=window_minutes)).strftime("%Y-%m-%d %H:%M:%S")
            id_rows = self._execute_select_all(
                "SELECT id FROM replay_data WHERE timestamp >= ? AND id <= ? ORDER BY id",
                (window_start, target_id),
            )
        else:
            id_rows = self._execute_select_all(
                "SELECT id FROM replay_data WHERE id <= ? ORDER BY id",
                (target_id,),
            )
        window_ids = [row[0] for row in id_rows]

        # Keep at least twice the current window so a single window always fits and
        # back-and-forth navigation stays warm; this cap is shared with the warm-on-load
        # path (_remember_metric_manager) so both agree on what to evict.
        self._metric_window_cap = max(self.METRIC_WINDOW_CACHE_SIZE, len(window_ids) * 2)

        # Decompress only the rows we haven't cached yet (typically just the new front
        # edge when stepping back, or the whole window on the first non-sequential jump).
        missing_ids = [replay_id for replay_id in window_ids if replay_id not in self._metric_window_cache]
        if missing_ids:
            self._cache_metric_managers(missing_ids)

        # Build the result oldest-to-newest, marking each window id as recently used so
        # eviction only ever drops rows outside the current window.
        metrics_list = []
        for replay_id in window_ids:
            metric_manager = self._metric_window_cache.get(replay_id)
            if metric_manager is not None:
                self._metric_window_cache.move_to_end(replay_id)
                metrics_list.append(metric_manager)

        return metrics_list

    def _remember_metric_manager(self, replay_id: int, metric_manager: dict[str, Any] | None) -> None:
        """Stores a row's metric_manager payload in the bounded window cache.

        Entries are kept most-recently-used last and the oldest are evicted beyond the
        cap. Row data is immutable for a given id, so cached entries never go stale.

        Args:
            replay_id: The replay id the payload belongs to.
            metric_manager: The metric_manager dict to cache (ignored if empty).
        """
        if not metric_manager:
            return

        cache = self._metric_window_cache
        cache[replay_id] = metric_manager
        cache.move_to_end(replay_id)
        while len(cache) > self._metric_window_cap:
            cache.popitem(last=False)

    def _cache_metric_managers(self, replay_ids: list[int]) -> None:
        """Decompresses the given replay rows and caches their metric_manager payloads.

        Args:
            replay_ids: The replay ids whose metric_manager data should be loaded.
        """
        # SQLite caps the number of bound parameters per statement, so fetch in chunks.
        chunk_size = 900
        for start in range(0, len(replay_ids), chunk_size):
            chunk = replay_ids[start : start + chunk_size]
            placeholders = ",".join("?" * len(chunk))
            rows = self._execute_select_all(
                f"SELECT id, data FROM replay_data WHERE id IN ({placeholders})",
                tuple(chunk),
            )
            for replay_id, data_blob in rows:
                try:
                    data = orjson.loads(self._decompressor.decompress(data_blob))
                except Exception:
                    continue
                self._remember_metric_manager(replay_id, data.get("metric_manager"))

    def fetch_global_variable_changes_for_current_replay_id(self):
        """Fetches global variable changes for the current replay ID."""
        rows = self._execute_select_all(
            "SELECT timestamp, variable_name, old_value, new_value FROM variable_changes WHERE replay_id = ?",
            (self.current_replay_id,),
        )

        for timestamp, variable, old_value, new_value in rows:
            # read_only notification is handled by monitor_read_only_change() in app.py
            if variable == "read_only":
                continue

            self.dolphie.app.notify(
                f"[b][$dark_yellow]{variable}[/b][/$dark_yellow]\n"
                f"Timestamp: [$light_blue]{timestamp}[/$light_blue]\n"
                f"Old Value: [$highlight]{old_value}[/$highlight]\n"
                f"New Value: [$highlight]{new_value}[/$highlight]",
                title="Global Variable Change",
                severity="warning",
                timeout=10,
            )

    def fetch_all_global_variable_changes(self) -> list[tuple[Any, ...]]:
        """Fetches all global variable changes for command 'V'."""
        rows = self._execute_select_all(
            "SELECT timestamp, variable_name, old_value, new_value FROM variable_changes ORDER BY timestamp"
        )

        return rows

    def capture_global_variable_change(
        self,
        variable_name: str,
        old_value: DatabaseScalar,
        new_value: DatabaseScalar,
    ):
        """Captures a global variable change and stores it in the SQLite database.

        Args:
            variable_name: The name of the variable that changed.
            old_value: The old value of the variable.
            new_value: The new value of the variable.
        """
        # Don't capture when not recording, or when loading a replay file (read-only mode)
        if not self.dolphie.record_for_replay or self.dolphie.replay_file:
            return

        timestamp = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S")

        last_row_id = self._execute_insert(
            "INSERT INTO variable_changes (timestamp, variable_name, old_value, new_value) VALUES (?, ?, ?, ?)",
            (timestamp, variable_name, old_value, new_value),
        )

        # Keep track of the primary key of the global variable change so we can link it to the replay data
        self.global_variable_change_ids.append(last_row_id)
