from __future__ import annotations

import re
import string
import time
from collections.abc import Mapping
from datetime import date, timedelta
from decimal import Decimal
from ssl import SSLError

import pymysql
from loguru import logger
from textual.app import App

from dolphie.DataTypes import ConnectionSource, ConnectionSourceType, DatabaseRow, DatabaseScalar
from dolphie.Modules.ArgumentParser import SSLConfig
from dolphie.Modules.Functions import coerce_int, coerce_str, escape_markup
from dolphie.Modules.ManualException import ManualException
from dolphie.Modules.Queries import MySQLQueries, ProxySQLQueries


class Database:
    def __init__(
        self,
        app: App,
        host: str,
        user: str | None,
        password: str | None,
        socket: str | None,
        port: int,
        ssl: SSLConfig,
        save_connection_id: bool = True,
        auto_connect: bool = True,
        daemon_mode: bool = False,
        read_timeout: float | None = None,
    ):
        self.app = app
        self.host = host
        self.user = user
        self.password = password
        self.socket = socket
        self.port = port
        self.ssl = ssl
        self.save_connection_id = save_connection_id
        self.daemon_mode = daemon_mode
        # None preserves pymysql's default (no read/write timeout) for callers
        # like the main connection, where some legitimate queries can be slow.
        self.read_timeout = read_timeout

        self._PRIVILEGE_ERROR_CODES = {
            1227,  # Access denied; SUPER privilege
            1370,  # execute command denied; REPLICATION CLIENT privilege
            1044,  # Access denied for user to database
            1142,  # command denied to user
            1143,  # column command denied to user
        }

        self.connection: pymysql.Connection | None = None
        self.cursor: pymysql.cursors.DictCursor | None = None
        self.connection_id: int | None = None
        self.source: ConnectionSourceType | None = None
        self.is_running_query: bool = False
        self.has_connected: bool = False
        self.last_execute_successful: bool = False
        # Track queries that have already shown privilege error notifications.
        self.privilege_errors_notified: set[str] = set()

        # Pre-compile regex pattern to filter non-printable characters
        self.non_printable_regex = re.compile(f"[^{re.escape(string.printable)}]")

        self.max_reconnect_attempts: int
        if daemon_mode:
            self.max_reconnect_attempts = 999999999
        else:
            self.max_reconnect_attempts = 3

        if auto_connect:
            self.connect()

    def connect(self, reconnect_attempt: bool = False):
        try:
            connection = pymysql.connect(
                host=self.host,
                user=self.user,
                passwd=self.password,
                unix_socket=self.socket,
                port=int(self.port),
                use_unicode=False,
                ssl=self.ssl or None,
                autocommit=True,
                connect_timeout=5,
                read_timeout=self.read_timeout,
                write_timeout=self.read_timeout,
                program_name="Dolphie",
            )
            self.connection = connection
            self.cursor = connection.cursor(pymysql.cursors.DictCursor)

            # If the query is successful, then the connection is to ProxySQL
            try:
                self.cursor.execute("SELECT @@admin-version")
                self.source = ConnectionSource.proxysql
            except Exception:
                self.source = ConnectionSource.mysql

            # Get connection ID for processlist filtering
            if self.save_connection_id:
                self.connection_id = self.connection.thread_id()

            # We don't want any SQL modes to be set to avoid unexpected behavior between MySQL & MariaDB
            if self.source == ConnectionSource.mysql:
                self.execute("SET SESSION sql_mode=''")

            logger.info(f"Connected to {self.source} with Process ID {self.connection_id}")
            self.has_connected = True
        except pymysql.Error as e:
            if reconnect_attempt:
                logger.error(f"Failed to reconnect to {self.source}: {e.args[1]}")
                error_message = coerce_str(e.args[1] if len(e.args) > 1 else next(iter(e.args), ""))
                escaped_error_message = escape_markup(error_message)
                self.app.notify(
                    (
                        f"[$b_light_blue]{self.host}:{self.port}[/$b_light_blue]: "
                        f"Failed to reconnect to MySQL: {escaped_error_message}"
                    ),
                    title="MySQL Reconnection Failed",
                    severity="error",
                    timeout=10,
                )
            else:
                if len(e.args) == 1:
                    raise ManualException(e.args[0]) from e
                else:
                    raise ManualException(e.args[1]) from e
        except FileNotFoundError as e:  # Catch SSL file path errors
            raise ManualException("SSL certificate file path isn't valid!") from e
        except SSLError as e:
            raise ManualException(f"SSL error: {e}") from e

    def close(self):
        connection = self.connection
        if connection is not None and self.is_connected():
            connection.close()

    def is_connected(self) -> bool:
        return bool(self.connection and self.connection.open)

    def _process_row(self, row: Mapping[str, object]) -> DatabaseRow:
        return {field: self._decode_value(value) for field, value in row.items()}

    def _decode_value(self, value: object) -> DatabaseScalar:
        if isinstance(value, (bytes, bytearray)):
            # First attempt: UTF-8
            try:
                decoded_value = value.decode("utf-8")
            except UnicodeDecodeError:
                # Second attempt: Latin-1
                try:
                    decoded_value = value.decode("latin-1")
                except UnicodeDecodeError:
                    # Fallback: Hex representation
                    return f"/* Failed to decode query, returning hex: {value.hex()} */"

            # Skip regex substitution for pure ASCII values (vast majority of MySQL system data)
            if decoded_value.isascii():
                return decoded_value

            return self.non_printable_regex.sub("?", decoded_value)

        if isinstance(value, (str, int, float, Decimal, date, timedelta)) or value is None:
            return value
        return coerce_str(value)

    def fetchall(self) -> list[DatabaseRow]:
        cursor = self.cursor
        if not self.is_connected() or not self.last_execute_successful or cursor is None:
            return []

        rows = cursor.fetchall()
        return [self._process_row(row) for row in rows] if rows else []

    def fetchone(self) -> DatabaseRow:
        cursor = self.cursor
        if not self.is_connected() or not self.last_execute_successful or cursor is None:
            return {}

        row = cursor.fetchone()
        return self._process_row(row) if row else {}

    def fetch_value_from_field(
        self,
        query: str,
        field: str | None = None,
        values: object = None,
        ignore_error: bool = False,
    ) -> DatabaseScalar:
        if not self.is_connected():
            return None

        self.execute(query, values, ignore_error=ignore_error)
        cursor = self.cursor
        if not self.last_execute_successful or cursor is None:
            return None

        data = cursor.fetchone()

        if not data:
            return None

        field = field or next(iter(data))  # Use field if provided, otherwise get first field
        value = data.get(field)
        return self._decode_value(value)

    def fetch_status_and_variables(self, command: str) -> dict[str, int | str]:
        self.execute(
            getattr(ProxySQLQueries, command)
            if self.source == ConnectionSource.proxysql
            else getattr(MySQLQueries, command)
        )
        data = self.fetchall()

        if command in {"status", "variables", "mysql_stats"}:
            values: dict[str, int | str] = {}
            for row in data:
                variable_name = coerce_str(row.get("Variable_name"))
                value = coerce_str(row.get("Value"))
                if variable_name:
                    values[variable_name] = int(value) if value.isnumeric() else value
            return values
        elif command == "innodb_metrics":
            return {name: coerce_int(row.get("COUNT")) for row in data if (name := coerce_str(row.get("NAME")))}

        return {}

    def execute(self, query: str, values: object = None, ignore_error: bool = False) -> int | None:
        if not self.is_connected():
            self.last_execute_successful = False
            return None

        if self.is_running_query:
            self.app.notify(
                "Another query is already running, please repeat action",
                title="Unable to run multiple queries at the same time",
                severity="error",
                timeout=10,
            )
            self.last_execute_successful = False
            return None

        # Prefix all queries with Dolphie so they can be easily identified in the processlist
        if self.source != ConnectionSource.proxysql:
            query = "/* Dolphie */ " + query

        # Check if this query has already failed with a privilege error - skip execution to save database call
        raw_query = query.replace("/* Dolphie */ ", "")
        if raw_query in self.privilege_errors_notified:
            self.last_execute_successful = False
            return None

        error_code: int | None = None
        for attempt_number in range(self.max_reconnect_attempts):
            self.is_running_query = True
            error_message = None

            try:
                cursor = self.cursor
                if cursor is None:
                    raise AttributeError
                rows = cursor.execute(query, values)
                self.is_running_query = False
                self.last_execute_successful = True

                return rows
            except AttributeError:
                # If the cursor is not defined, reconnect and try again
                self.is_running_query = False
                self.last_execute_successful = False

                self.close()
                self.connect()

                time.sleep(1)
            except pymysql.Error as e:
                self.is_running_query = False
                self.last_execute_successful = False

                if len(e.args) == 1:
                    error_code = e.args[0]
                else:
                    error_code = e.args[0]
                    if e.args[1]:
                        error_message = coerce_str(e.args[1])

                # Check if this is a privilege error - silently return None without raising exception
                if error_code in self._PRIVILEGE_ERROR_CODES:
                    # Show notification only the first time this query fails with privilege error
                    if raw_query not in self.privilege_errors_notified:
                        self.privilege_errors_notified.add(raw_query)

                        logger.warning(
                            f"Privilege error (code {error_code}): {error_message}. "
                            f"Query: {raw_query}. "
                            f"This query will be skipped and stats for this feature won't be available."
                        )

                        # Escape [ and ] characters in the error message and query
                        escaped_error_message = escape_markup(error_message) if error_message else "Access denied"
                        escaped_query = escape_markup(raw_query)

                        self.app.notify(
                            f"[$b_highlight]{self.host}:{self.port}[/$b_highlight]: [dim]{error_code}: "
                            f"{escaped_error_message}[/dim]\nQuery: [$b_light_blue]{escaped_query}[/$b_light_blue]\n"
                            "Stats for this feature won't be available.",
                            title="Insufficient Privileges",
                            severity="warning",
                            timeout=9,
                        )

                    return None

                # If ignore_error is set, return None for any error
                if ignore_error:
                    return None

                # Determine if this is a connection-loss error:
                # 1. is_connected() catches client-side errors where pymysql closes the socket
                #    (e.g. 2006, 2013, 2055 - server gone, lost connection, etc.)
                # 2. Server-side shutdown errors (1053, 1079, 1080) where the error packet is
                #    received successfully but the server is going away
                if not self.is_connected() or error_code in (1053, 1079, 1080):
                    if error_message:
                        logger.error(
                            f"{self.source} has lost its connection: {error_message}, attempting to reconnect..."
                        )
                        # Escape [ and ] characters in the error message
                        escaped_error_message = escape_markup(error_message)
                        self.app.notify(
                            f"[$b_light_blue]{self.host}:{self.port}[/$b_light_blue]: {escaped_error_message}",
                            title="MySQL Connection Lost",
                            severity="error",
                            timeout=10,
                        )

                    self.close()
                    self.connect(reconnect_attempt=True)

                    connection = self.connection
                    if connection is None or not connection.open:
                        # Exponential backoff
                        time.sleep(min(1 * (2**attempt_number), 20))  # Cap the wait time at 20 seconds

                        # Skip the rest of the loop
                        continue

                    self.app.notify(
                        f"[$b_light_blue]{self.host}:{self.port}[/$b_light_blue]: Successfully reconnected",
                        title="MySQL Connection Created",
                        severity="information",
                        timeout=10,
                    )

                    # Retry the query
                    return self.execute(query, values)
                else:
                    raise ManualException(coerce_str(error_message), query=query, code=error_code or 0) from e

        connection = self.connection
        if connection is None or not connection.open:
            raise ManualException(
                f"Failed to reconnect to {self.source} after {self.max_reconnect_attempts} attempts",
                query=query,
                code=error_code or 0,
            )
