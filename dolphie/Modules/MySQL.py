import time
from ssl import SSLError

import pymysql
from dolphie.Modules.ManualException import ManualException
from dolphie.Modules.Queries import MySQLQueries
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
    ):
        self.connection: pymysql.Connection = None
        self.connection_id: int = None

        self.app = app
        self.host = host
        self.user = user
        self.password = password
        self.socket = socket
        self.port = port
        self.ssl = ssl
        self.save_connection_id = save_connection_id

        self.max_reconnect_attempts: int = 3
        self.running_query: bool = False
        self.using_ssl: str = None

        if auto_connect:
            self.connect()

    def connect(self):
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

            # Get connection ID for processlist filtering
            if self.save_connection_id:
                self.connection_id = self.fetch_value_from_field("SELECT CONNECTION_ID()")

            # Determine if SSL is being used
            self.using_ssl = "ON" if self.fetch_value_from_field("SHOW STATUS LIKE 'Ssl_cipher'", "Value") else "OFF"
        except pymysql.Error as e:
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
        if self.connection:
            return self.connection.open

        return False

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

        error_message = None

        # Prefix all queries with dolphie so they can be identified in the processlist from other people
        query = "/* dolphie */ " + query

        for _ in range(self.max_reconnect_attempts):
            self.running_query = True

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
                        error_message = e.args[1]

                    # Check if the error is due to a connection issue
                    if error_code in (0, 2006, 2013, 2055):
                        # 0: Not connected to MySQL
                        # 2006: MySQL server has gone away
                        # 2013: Lost connection to MySQL server during query
                        # 2055: Lost connection to MySQL server at hostname

                        self.app.notify(
                            f"[b light_blue]{self.host}:{self.port}[/b light_blue]: {error_message}",
                            title="MySQL Connection Lost",
                            severity="error",
                            timeout=10,
                        )

                        self.close()
                        self.connect()

                        self.app.notify(
                            f"[b light_blue]{self.host}:{self.port}[/b light_blue]: Successfully reconnected",
                            title="MySQL Connection Created",
                            severity="success",
                            timeout=10,
                        )

                        time.sleep(1)
                    else:
                        raise ManualException(error_message, query=query)

        if error_message is not None:
            raise ManualException(
                f"{self.host}:{self.port}: Failed to execute query after"
                f" {self.max_reconnect_attempts} reconnection attempts - error: {error_message}",
                query=query,
            )

    def _process_row(self, row):
        processed_row = {}

        for field, value in row.items():
            if isinstance(value, (bytes, bytearray)):
                processed_row[field] = value.decode()
            else:
                processed_row[field] = value

        return processed_row

    def fetchall(self):
        if not self.is_connected():
            return None

        rows = [self._process_row(row) for row in self.cursor.fetchall()]

        if not rows:
            return []

        return rows

    def fetchone(self):
        if not self.is_connected():
            return

        row = self.cursor.fetchone()

        if not row:
            return {}

        return self._process_row(row)

    def fetch_value_from_field(self, query, field=None, values=None):
        if not self.is_connected():
            return None

        self.execute(query, values)
        data = self.cursor.fetchone()

        if not data:
            return None

        field = field or next(iter(data))  # Use field if provided, otherwise get first field
        value = data.get(field)

        if isinstance(value, (bytes, bytearray)):
            return value.decode()

        return value

    def fetch_status_and_variables(self, command):
        if not self.is_connected():
            return None

        command_data = {}

        self.execute(getattr(MySQLQueries, command))
        data = self.fetchall()

        if command in {"status", "variables"}:
            for row in data:
                variable = row["Variable_name"]
                value = row["Value"]

                converted_value = int(value) if value.isnumeric() else value

                command_data[variable] = converted_value
        elif command == "innodb_metrics":
            for row in data:
                metric = row["NAME"]
                value = int(row["COUNT"])

                command_data[metric] = value

        return command_data
