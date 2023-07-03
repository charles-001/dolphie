import pymysql


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
            raise Exception("Failed to connect to database host %s - Reason: %s" % (self.host, e.args[1]))
        except FileNotFoundError:  # Catch SSL file path errors
            raise Exception("SSL certificate file path isn't valid!")

    def fetchone(self, query, field, values=None):
        if values is None:
            values = []
        self.cursor.execute(query, tuple(values))
        data = self.cursor.fetchone()
        if isinstance(data[field], (bytes, bytearray)):
            return data[field].decode()
        else:
            return data[field]

    def close(self):
        if self.connection:
            self.connection.close()
