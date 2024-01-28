class EnumBase:
    @classmethod
    def ALL(cls):
        return [value for name, value in cls.__dict__.items() if not name.startswith("__") and isinstance(value, str)]


class Panels(EnumBase):
    DASHBOARD = "dashboard"
    PROCESSLIST = "processlist"
    GRAPHS = "graphs"
    REPLICATION = "replication"
    LOCKS = "locks"
