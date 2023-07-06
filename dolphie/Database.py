import pymysql
from dolphie.Functions import detect_encoding
from dolphie.ManualException import ManualException


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
            raise ManualException("Failed to connect to database host %s" % self.host, reason=e.args[1])
        except FileNotFoundError:  # Catch SSL file path errors
            raise ManualException("SSL certificate file path isn't valid!")

    def execute(self, query, values=None, ignore_error=False):
        # Prefix all queries with dolphie so they can be identified in the processlist
        query = "/* dolphie */ " + query

        try:
            return self.cursor.execute(query, values)
        except Exception as e:
            if ignore_error:
                return None
            else:
                raise ManualException("Failed to execute query\n", query=query, reason=e.args[1])

    def fetchall(self):
        rows = []

        # Iterate over each row obtained from self.cursor.fetchall()
        for row in self.cursor.fetchall():
            processed_row = {}

            # Iterate over each field-value pair in the row dictionary
            for field, value in row.items():
                if isinstance(value, (bytes, bytearray)):
                    # If the value is an instance of bytes or bytearray, decode it
                    if "query" in field:
                        # If the field name contains the word 'query', detect the encoding
                        processed_row[field] = value.decode(detect_encoding(value))
                    else:
                        # Otherwise, decode the value as utf-8 by default
                        processed_row[field] = value.decode()
                else:
                    # Otherwise, use the original value
                    processed_row[field] = value

            # Append the processed row to the rows list
            rows.append(processed_row)

        # Return the list of processed rows
        return rows

    def fetchone(self, query, field, values=None):
        self.execute(query, values)
        data = self.cursor.fetchone()

        if not data:
            return None

        value = data[field]
        if isinstance(value, (bytes, bytearray)):
            # If the value is an instance of bytes or bytearray, decode it
            if field == "Status":
                # If the field name is Status, detect the encoding
                return value.decode(detect_encoding(value))
            else:
                # Otherwise, decode the value as utf-8 by default
                return value.decode()
        else:
            return value

    def close(self):
        if self.connection:
            self.connection.close()
