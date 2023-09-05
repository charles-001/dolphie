import pymysql
from dolphie.Modules.Functions import detect_encoding
from dolphie.Modules.ManualException import ManualException
from dolphie.Modules.Queries import MySQLQueries


class Database:
    def __init__(self, host, user, password, socket, port, ssl):
        self.host = host
        self.user = user
        self.password = password
        self.socket = socket
        self.port = port
        self.ssl = ssl

        try:
            self.connection = pymysql.connect(
                host=host,
                user=user,
                passwd=password,
                unix_socket=socket,
                port=port,
                use_unicode=False,
                ssl=ssl,
                autocommit=True,
            )
            self.cursor = self.connection.cursor(pymysql.cursors.DictCursor)
        except pymysql.Error as e:
            raise ManualException(f"Failed to connect to database host {self.host}:{self.port}", reason=e.args[1])
        except FileNotFoundError:  # Catch SSL file path errors
            raise ManualException("SSL certificate file path isn't valid!")

    def execute(self, query, values=None, ignore_error=False):
        if not self.connection.open:
            return

        # Prefix all queries with dolphie so they can be identified in the processlist from other people
        query = "/* dolphie */ " + query

        try:
            return self.cursor.execute(query, values)
        except Exception as e:
            if ignore_error:
                return None
            else:
                raise ManualException("Failed to execute query\n", query=query, reason=e.args[1])

    def process_row(self, row):
        processed_row = {}

        for field, value in row.items():
            if isinstance(value, (bytes, bytearray)):
                if "query" in field:
                    processed_row[field] = value.decode(detect_encoding(value))
                else:
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
            if field == "Status":
                return value.decode(detect_encoding(value))
            return value.decode()
        return value

    def fetch_status_and_variables(self, command):
        command_data = {}

        if command in {"status", "variables"}:
            self.execute(getattr(MySQLQueries, command))
            data = self.fetchall()

            for row in data:
                variable = row["Variable_name"]
                value = row["Value"]

                converted_value = int(value) if value.isnumeric() else value

                command_data[variable] = converted_value
        elif command == "innodb_metrics":
            self.execute(MySQLQueries.innodb_metrics)
            data = self.fetchall()

            for row in data:
                metric = row["NAME"]
                value = int(row["COUNT"])

                command_data[metric] = value

        return command_data
