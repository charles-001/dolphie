import json
import os
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import Union

import zstandard as zstd
from dolphie.DataTypes import (
    ConnectionSource,
    ProcesslistThread,
    ProxySQLProcesslistThread,
)
from dolphie.Dolphie import Dolphie
from dolphie.Modules import MetricManager
from dolphie.Modules.Functions import format_bytes, minify_query
from loguru import logger


@dataclass
class MySQLReplayData:
    timestamp: str
    global_status: dict
    global_variables: dict
    binlog_status: dict
    innodb_metrics: dict
    replica_manager: dict
    replication_status: dict
    processlist: dict
    metric_manager: dict


@dataclass
class ProxySQLReplayData:
    timestamp: str
    global_status: dict
    global_variables: dict
    command_stats: dict
    hostgroup_summary: dict
    processlist: dict
    metric_manager: dict


class ReplayManager:
    """
    ReplayManager class for capturing and replaying Dolphie instance states.
    """

    def __init__(self, dolphie: Dolphie):
        """
        Initializes the ReplayManager with Dolphie instance and SQLite database settings.

        Args:
            dolphie: The Dolphie instance.
        """
        self.dolphie = dolphie
        self.current_index = 0  # This is used to keep track of the last primary key read from the database
        self.min_timestamp = None
        self.max_timestamp = None
        self.min_id = None
        self.max_id = None
        self.last_purge_time = datetime.now() - timedelta(hours=1)  # Initialize to an hour ago
        self.replay_file_size = 0

        # Determine filename used for replay file
        if dolphie.replay_file:
            self.replay_file = dolphie.replay_file
        elif dolphie.daemon_mode:
            self.replay_file = f"{dolphie.replay_dir}/{dolphie.host}/daemon.db"
        elif dolphie.record_for_replay:
            self.replay_file = f"{dolphie.replay_dir}/{dolphie.host}/{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}.db"
            dolphie.app.notify(
                f"File: [highlight]{self.replay_file}[/highlight]", title="Recording data for Replay", timeout=10
            )
        else:
            # No options specified for replaying, skip initialization
            return

        logger.info(f"Replay database file: {self.replay_file} ({self.dolphie.replay_retention_hours} hours retention)")

        self._initialize_sqlite()
        self._manage_metadata()

        if dolphie.replay_file:
            self._get_replay_file_metadata()

    def _initialize_sqlite(self):
        """
        Initializes the SQLite database and creates the necessary tables.
        """

        # Ensure the directory for the replay file exists
        os.makedirs(os.path.dirname(self.replay_file), mode=0o770, exist_ok=True)

        # Connect to the SQLite database
        self.conn = sqlite3.connect(self.replay_file, isolation_level=None, check_same_thread=False)
        self.cursor = self.conn.cursor()

        # Set the database file permissions to 660
        os.chmod(self.replay_file, 0o660)

        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS replay_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME,
                data BLOB
            )"""
        )
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_replay_data_timestamp ON replay_data (timestamp)")
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS metadata (
                hostname VARCHAR(255),
                host_version VARCHAR(255),
                host_distro VARCHAR(255),
                connection_source VARCHAR(255),
                dolphie_version VARCHAR(255)
            )"""
        )

        logger.info("Connected to SQLite")

        # Start the purge process
        self.purge_old_data()

    def purge_old_data(self):
        """
        Purges data older than the retention period specified by hours_of_retention and performs a vacuum.
        Only runs if at least an hour has passed since the last purge.
        """
        if not self.dolphie.record_for_replay:
            return

        current_time = datetime.now()
        if current_time - self.last_purge_time < timedelta(hours=1):
            return  # Skip purging if less than an hour has passed

        retention_date = (current_time - timedelta(hours=self.dolphie.replay_retention_hours)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        self.cursor.execute("DELETE FROM replay_data WHERE timestamp < ?", (retention_date,))
        rows_deleted = self.cursor.rowcount

        if rows_deleted:
            # This will rebuild the database file and reduce its size
            self.cursor.execute("VACUUM")

            self.replay_file_size = os.path.getsize(self.replay_file)
            logger.info(
                f"Purged {rows_deleted} rows from replay data older than {retention_date}. "
                f"Database file size is now {format_bytes(self.replay_file_size, color=False)}"
            )

        self.last_purge_time = current_time

    def seek_to_timestamp(self, timestamp: str):
        """
        Seeks to the specified timestamp in the SQLite database.

        Args:
            timestamp: The timestamp to seek to.
        """
        self.cursor.execute("SELECT id FROM replay_data WHERE timestamp = ?", (timestamp,))
        row = self.cursor.fetchone()
        if row:
            # We subtract 1 because get_next_refresh_interval naturally increments the index
            self.current_index = row[0] - 1
            self.dolphie.app.notify(
                f"Seeking to timestamp [light_blue]{timestamp}[/light_blue]", severity="success", timeout=10
            )

            return True
        else:
            # Try to find a timestamp before the specified timestamp
            self.cursor.execute(
                "SELECT id, timestamp FROM replay_data WHERE timestamp < ? ORDER BY timestamp DESC LIMIT 1",
                (timestamp,),
            )
            row = self.cursor.fetchone()
            if row:
                # We subtract 1 because get_next_refresh_interval naturally increments the index
                self.current_index = row[0] - 1
                self.dolphie.app.notify(
                    f"Timestamp not found, seeking to closest timestamp [light_blue]{row[1]}[/light_blue]",
                    timeout=10,
                )

                return True
            else:
                self.dolphie.app.notify(
                    f"No timestamps found on or before [light_blue]{timestamp}[/light_blue]",
                    severity="error",
                    timeout=10,
                )
                return False

    def _manage_metadata(self):
        """
        Manages the metadata table with information we care about.
        """
        if not self.dolphie.record_for_replay:
            return

        self.cursor.execute("SELECT * FROM metadata")
        row = self.cursor.fetchone()
        if row is None:
            self.cursor.execute(
                "INSERT INTO metadata (hostname, host_version, host_distro, connection_source, dolphie_version) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    self.dolphie.host_with_port,
                    self.dolphie.host_version,
                    self.dolphie.host_distro,
                    self.dolphie.connection_source,
                    self.dolphie.app_version,
                ),
            )
        else:
            connection_source = row[3]
            app_version = row[4]
            logger.info(
                f"Replay database metadata - Host: {row[0]}, Version: {row[1]} ({row[2]}), "
                f"Dolphie version: {app_version}"
            )

            if self.dolphie.daemon_mode:
                # Avoid mixing Dolphie versions in the same replay file. Never know what I might change in the future :)
                if app_version != self.dolphie.app_version:
                    new_replay_file = f"{self.replay_file}_v{app_version}"
                    logger.warning(
                        f"The version of Dolphie ({self.dolphie.app_version}) differs from the version of the "
                        f"daemon's replay file ({app_version}). To avoid potential compatibility issues, the current "
                        f"database file will be renamed to: {new_replay_file}"
                    )

                    os.rename(self.replay_file, new_replay_file)
                    self._initialize_sqlite()
                    self._manage_metadata()

                # Avoid mixing connection sources in the same replay file
                if connection_source != self.dolphie.connection_source:
                    logger.critical(
                        f"The connection source of the daemon's replay file ({connection_source}) "
                        f"differs from the current connection source ({self.dolphie.connection_source}). You should "
                        "never mix connection sources in the same replay file. Please rename the daemon's replay file "
                        "and restart the daemon."
                    )

    def _get_replay_file_metadata(self):
        """
        Retrieves the replay's metadata from the metadata table.
        """
        self.cursor.execute("SELECT * FROM metadata")
        row = self.cursor.fetchone()
        if row:
            self.dolphie.host_with_port = row[0]
            self.dolphie.host_version = row[1]
            self.dolphie.host_distro = row[2]
            self.dolphie.connection_source = row[3]
        else:
            raise Exception("Metadata not found in replay file.")

    def _compress_data(self, data: str):
        """
        Compresses data using zstd.

        Args:
            data (str): Data to compress.

        Returns:
            bytes: Compressed data.
        """
        compressor = zstd.ZstdCompressor(level=15)
        return compressor.compress(data.encode("utf-8"))

    def _decompress_data(self, compressed_data: bytes):
        """
        Decompresses data using zstd.

        Args:
            compressed_data (bytes): Compressed data.

        Returns:
            str: Decompressed data.
        """
        decompressor = zstd.ZstdDecompressor()
        return decompressor.decompress(compressed_data).decode("utf-8")

    def _condition_metrics(self, metric_manager: MetricManager.MetricManager):
        """
        Captures the metrics from the metric manager and returns them in a structured format.

        Args:
            metric_manager: The metric manager to capture metrics from.

        Returns:
            dict: A dictionary of captured metrics.
        """
        metrics = {"datetimes": metric_manager.datetimes}

        for metric_instance_name, metric_instance_data in metric_manager.metrics.__dict__.items():
            # Skip if the metric instance is not for the current connection source
            if self.dolphie.connection_source == ConnectionSource.mysql:
                if (
                    ConnectionSource.mysql not in metric_instance_data.connection_source
                    or not metric_instance_data.use_with_replay
                ):
                    continue
            elif (
                self.dolphie.connection_source == ConnectionSource.proxysql
                and ConnectionSource.proxysql not in metric_instance_data.connection_source
            ):
                continue

            metric_entry = metrics.setdefault(metric_instance_name, {})
            for k, v in metric_instance_data.__dict__.items():
                if not hasattr(v, "values") or not v.values:
                    continue

                metric_entry.setdefault(k, [])
                if v.values:
                    metric_entry[k] = v.values

        return metrics

    def capture_state(self):
        """
        Captures the current state of the Dolphie instance and stores it in the SQLite database.
        """
        if not self.dolphie.record_for_replay:
            return

        # Convert the dictionary of processlist threads to a list of dictionaries
        processlist = [
            {**thread_data, "query": minify_query(thread_data["query"])} if "query" in thread_data else thread_data
            for thread_data in (v.thread_data for v in self.dolphie.processlist_threads.values())
        ]

        if self.dolphie.connection_source == ConnectionSource.mysql:
            # Remove some global status variables that are not useful for replaying
            keys_to_remove = []
            for var_name in self.dolphie.global_status.keys():
                exclude_vars = [
                    "Mysqlx",
                    "Ssl",
                    "Performance_schema",
                    "Rsa_public_key",
                    "Caching_sha2_password_rsa_public_key",
                ]
                for exclude_var in exclude_vars:
                    if exclude_var in var_name:
                        keys_to_remove.append(var_name)
                        break

            for key in keys_to_remove:
                self.dolphie.global_status.pop(key)

            state = MySQLReplayData(
                timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                global_status=self.dolphie.global_status,
                global_variables=self.dolphie.global_variables,
                binlog_status=self.dolphie.binlog_status,
                innodb_metrics=self.dolphie.innodb_metrics,
                replica_manager=self.dolphie.replica_manager.available_replicas,
                replication_status=self.dolphie.replication_status,
                processlist=processlist,
                metric_manager=self._condition_metrics(self.dolphie.metric_manager),
            )
        else:
            state = ProxySQLReplayData(
                timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                global_status=self.dolphie.global_status,
                global_variables=self.dolphie.global_variables,
                command_stats=self.dolphie.proxysql_command_stats,
                hostgroup_summary=self.dolphie.proxysql_hostgroup_summary,
                processlist=processlist,
                metric_manager=self._condition_metrics(self.dolphie.metric_manager),
            )

        self.cursor.execute(
            "INSERT INTO replay_data (timestamp, data) VALUES (?, ?)",
            (
                state.timestamp,
                self._compress_data(json.dumps(asdict(state))),
            ),
        )

        self.purge_old_data()

        if not self.dolphie.daemon_mode:
            self.replay_file_size = os.path.getsize(self.replay_file)

    def get_next_refresh_interval(self) -> Union[MySQLReplayData, ProxySQLReplayData, None]:
        """
        Gets the next refresh interval's data from the SQLite database and returns it as a ReplayData object.

        Returns:
            ReplayData: The next replay data.
        """

        # Get the min and max timestamps and IDs from the database so we can update the UI
        self.cursor.execute("SELECT MIN(timestamp), MAX(timestamp), MIN(id), MAX(id) FROM replay_data")
        row = self.cursor.fetchone()
        if row:
            self.min_timestamp = row[0]
            self.max_timestamp = row[1]
            self.min_id = row[2]
            self.max_id = row[3]

        # Get the next row
        self.cursor.execute(
            "SELECT id, timestamp, data FROM replay_data WHERE id > ? ORDER BY id LIMIT 1",
            (self.current_index,),
        )
        row = self.cursor.fetchone()
        if row is None:
            return None

        self.current_index = row[0]

        # Decompress and parse the JSON data
        data = json.loads(self._decompress_data(row[2]))

        processlist = {}
        if self.dolphie.connection_source == ConnectionSource.mysql:
            # Re-create the ProcesslistThread object for each thread in the JSON's processlist
            for thread_data in data["processlist"]:
                processlist[str(thread_data["id"])] = ProcesslistThread(thread_data)

            replay_data = MySQLReplayData(
                timestamp=row[1],
                global_status=data.get("global_status", {}),
                global_variables=data.get("global_variables", {}),
                binlog_status=data.get("binlog_status", {}),
                innodb_metrics=data.get("innodb_metrics", {}),
                replica_manager=data.get("replica_manager", {}),
                replication_status=data.get("replication_status", {}),
                processlist=processlist,
                metric_manager=data.get("metric_manager", {}),
            )
        elif self.dolphie.connection_source == ConnectionSource.proxysql:
            # Re-create the ProxySQLProcesslistThread object for each thread in the JSON's processlist
            for thread_data in data["processlist"]:
                processlist[str(thread_data["id"])] = ProxySQLProcesslistThread(thread_data)

            replay_data = ProxySQLReplayData(
                timestamp=row[1],
                global_status=data.get("global_status", {}),
                global_variables=data.get("global_variables", {}),
                command_stats=data.get("command_stats", {}),
                hostgroup_summary=data.get("hostgroup_summary", {}),
                processlist=processlist,
                metric_manager=data.get("metric_manager", {}),
            )
        else:
            self.dolphie.app.notify("Invalid connection source for replay data", severity="error")

        return replay_data

    def __del__(self):
        """Close the SQLite connection when the ReplayManager is destroyed."""
        if hasattr(self, "conn"):
            self.conn.close()
