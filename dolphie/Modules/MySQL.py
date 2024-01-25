import time

import pymysql
from dolphie.Modules.ManualException import ManualException
from dolphie.Modules.Queries import MySQLQueries
from textual.app import App


class Database:
    def __init__(self, app: App, tab_name: str, host: str, user: str, password: str, socket: str, port: int, ssl: str):
        self.connection: pymysql.Connection = None
        self.connection_id: int = None

        self.app = app
        self.tab_name = tab_name
        self.host = host
        self.user = user
        self.password = password
        self.socket = socket
        self.port = port
        self.ssl = ssl

        self.max_reconnect_attempts = 3

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
                read_timeout=5,
                write_timeout=5,
            )
            self.cursor = self.connection.cursor(pymysql.cursors.DictCursor)

            # Get connection ID for processlist filtering
            self.connection_id = self.fetch_value_from_field("SELECT CONNECTION_ID()")

            # Reduce any issues with the queries Dolphie runs (mostly targetting only_full_group_by)
            self.execute("SET SESSION sql_mode = ''")
        except pymysql.Error as e:
            raise ManualException(e.args[1])
        except FileNotFoundError:  # Catch SSL file path errors
            raise ManualException("SSL certificate file path isn't valid!")

    def close(self):
        if self.connection and self.connection.open:
            self.connection.close()

    def execute(self, query, values=None, ignore_error=False):
        if not self.connection.open:
            return

        # Prefix all queries with dolphie so they can be identified in the processlist from other people
        query = "/* dolphie */ " + query

        error_message = None
        for _ in range(self.max_reconnect_attempts):
            try:
                return self.cursor.execute(query, values)
            except pymysql.Error as e:
                if ignore_error:
                    return None
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
                            f"Tab [highlight]{self.tab_name}[/highlight]: {error_message}",
                            title="MySQL Connection Error",
                            severity="error",
                            timeout=10,
                        )

                        self.close()
                        self.connect()

                        self.app.notify(
                            f"Tab [highlight]{self.tab_name}[/highlight]: Successfully reconnected!",
                            title="MySQL Connection",
                            severity="success",
                            timeout=10,
                        )

                        time.sleep(1)
                    else:
                        raise ManualException(error_message, query=query)

        if error_message is not None:
            raise ManualException(
                f"{self.host}: Failed to execute query after"
                f" {self.max_reconnect_attempts} reconnection attempts - error: {error_message}",
                query=query,
            )

    def process_row(self, row):
        processed_row = {}

        for field, value in row.items():
            if isinstance(value, (bytes, bytearray)):
                processed_row[field] = value.decode()
            else:
                processed_row[field] = value

        return processed_row

    def fetchall(self):
        rows = [self.process_row(row) for row in self.cursor.fetchall()]

        if not rows:
            return []

        return rows

    def fetchone(self):
        row = self.cursor.fetchone()

        if not row:
            return {}

        return self.process_row(row)

    def fetch_value_from_field(self, query, field=None, values=None):
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
