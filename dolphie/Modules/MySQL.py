import time
from ssl import SSLError

import pymysql
from dolphie.DataTypes import ConnectionSource
from dolphie.Modules.ManualException import ManualException
from dolphie.Modules.Queries import MySQLQueries, ProxySQLQueries
from loguru import logger
from textual.app import App


class Database:
    def __init__(
        self,
        app: App,
        host: str,
        user: str,
        password: str,
        socket: str,
        port: int,
        ssl: str,
        save_connection_id: bool = True,
        auto_connect: bool = True,
        daemon_mode: bool = False,
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

        self.connection: pymysql.Connection = None
        self.connection_id: int = None
        self.running_query: bool = False
        self.source: ConnectionSource = None
        if daemon_mode:
            self.max_reconnect_attempts: int = 999999999
        else:
            self.max_reconnect_attempts: int = 3

        if auto_connect:
            self.connect()

    def connect(self, reconnect_attempt: bool = False):
        try:
            self.connection = pymysql.connect(
                host=self.host,
                user=self.user,
                passwd=self.password,
                unix_socket=self.socket,
                port=int(self.port),
                use_unicode=False,
                ssl=self.ssl,
                autocommit=True,
                connect_timeout=5,
                program_name="Dolphie",
            )
            self.cursor = self.connection.cursor(pymysql.cursors.DictCursor)

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
        except pymysql.Error as e:
            if reconnect_attempt:
                logger.error(f"Failed to reconnect to {self.source}: {e.args[1]}")
                self.app.notify(
                    f"[b light_blue]{self.host}:{self.port}[/b light_blue]: Failed to reconnect to MySQL: {e.args[1]}",
                    title="MySQL Reconnection Failed",
                    severity="error",
                    timeout=10,
                )
            else:
                if len(e.args) == 1:
                    raise ManualException(e.args[0])
                else:
                    raise ManualException(e.args[1])
        except FileNotFoundError:  # Catch SSL file path errors
            raise ManualException("SSL certificate file path isn't valid!")
        except SSLError as e:
            raise ManualException(f"SSL error: {e}")

    def close(self):
        if self.is_connected():
            self.connection.close()

    def is_connected(self) -> bool:
        return self.connection and self.connection.open

    def _process_row(self, row):
        return {field: self._decode_value(value) for field, value in row.items()}

    def _decode_value(self, value):
        if isinstance(value, (bytes, bytearray)):
            try:
                return value.decode()
            except UnicodeDecodeError:
                return "/* Dolphie can't decode query with utf-8 */"
        return value

    def fetchall(self):
        if not self.is_connected():
            return []

        rows = self.cursor.fetchall()
        return [self._process_row(row) for row in rows] if rows else []

    def fetchone(self):
        if not self.is_connected():
            return {}

        row = self.cursor.fetchone()
        return self._process_row(row) if row else {}

    def fetch_value_from_field(self, query, field=None, values=None):
        if not self.is_connected():
            return None

        self.execute(query, values)
        data = self.cursor.fetchone()

        if not data:
            return None

        field = field or next(iter(data))  # Use field if provided, otherwise get first field
        value = data.get(field)
        return self._decode_value(value)

    def fetch_status_and_variables(self, command):
        if not self.is_connected():
            return None

        self.execute(
            getattr(ProxySQLQueries, command)
            if self.source == ConnectionSource.proxysql
            else getattr(MySQLQueries, command)
        )
        data = self.fetchall()

        if command in {"status", "variables", "mysql_stats"}:
            return {
                row["Variable_name"]: int(row["Value"]) if row["Value"].isnumeric() else row["Value"] for row in data
            }
        elif command == "innodb_metrics":
            return {row["NAME"]: int(row["COUNT"]) for row in data}

    def execute(self, query, values=None, ignore_error=False):
        if not self.is_connected():
            return None

        if self.running_query:
            self.app.notify(
                "Another query is already running, please repeat action",
                title="Unable to run multiple queries at the same time",
                severity="error",
                timeout=10,
            )
            return None

        # Prefix all queries with dolphie so they can be identified in the processlist from other people
        if self.source != ConnectionSource.proxysql:
            query = "/* dolphie */ " + query

        for attempt_number in range(self.max_reconnect_attempts):
            self.running_query = True
            error_message = None

            try:
                rows = self.cursor.execute(query, values)
                self.running_query = False

                return rows
            except AttributeError:
                # If the cursor is not defined, reconnect and try again
                self.running_query = False

                self.close()
                self.connect()

                time.sleep(1)
            except pymysql.Error as e:
                self.running_query = False

                if ignore_error:
                    return None
                else:
                    if len(e.args) == 1:
                        error_code = e.args[0]
                    else:
                        error_code = e.args[0]
                        if e.args[1]:
                            error_message = e.args[1]

                    # Check if the error is due to a connection issue
                    if error_code in (0, 2006, 2013, 2055):
                        # 0: Not connected to MySQL
                        # 2006: MySQL server has gone away
                        # 2013: Lost connection to MySQL server during query
                        # 2055: Lost connection to MySQL server at hostname

                        if error_message:
                            logger.error(
                                f"{self.source} has lost its connection: {error_message}, attempting to reconnect..."
                            )
                            self.app.notify(
                                f"[b light_blue]{self.host}:{self.port}[/b light_blue]: {error_message}",
                                title="MySQL Connection Lost",
                                severity="error",
                                timeout=10,
                            )

                        self.close()
                        self.connect(reconnect_attempt=True)

                        # Exponential backoff
                        time.sleep(min(1 * (2**attempt_number), 20))  # Cap the wait time at 20 seconds

                        # If the connection is still not open, skip the rest of the attempt
                        if not self.connection.open:
                            continue

                        self.app.notify(
                            f"[b light_blue]{self.host}:{self.port}[/b light_blue]: Successfully reconnected",
                            title="MySQL Connection Created",
                            severity="success",
                            timeout=10,
                        )

                        # Retry the query
                        return self.execute(query, values)
                    else:
                        raise ManualException(error_message, query=query)

        if not self.connection.open:
            raise ManualException(
                f"Failed to execute query after {self.max_reconnect_attempts} reconnection attempts",
                query=query,
            )
