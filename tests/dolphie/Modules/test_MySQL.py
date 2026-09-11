from __future__ import annotations

import re
from datetime import datetime, timezone

from dolphie.DataTypes import ConnectionSource, DatabaseRow
from dolphie.Modules.MySQL import Database


class StatusDatabase(Database):
    def __init__(self, rows: list[DatabaseRow]) -> None:
        self.source = ConnectionSource.mysql
        self.rows = rows

    def execute(self, query: str, values: object = None, ignore_error: bool = False) -> int | None:
        return 0

    def fetchall(self) -> list[DatabaseRow]:
        return self.rows


def test_database_decodes_binary_scalars() -> None:
    database = Database.__new__(Database)
    database.non_printable_regex = re.compile(r"[^\x20-\x7e]")

    assert database._decode_value(b"plain text") == "plain text"
    assert database._decode_value(bytearray("café", "utf-8")) == "caf?"
    assert database._decode_value(42) == 42
    timestamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert database._decode_value(timestamp) is timestamp


def test_status_values_are_normalized_at_database_boundary() -> None:
    database = StatusDatabase(
        [
            {"Variable_name": "Threads_connected", "Value": "12"},
            {"Variable_name": "version", "Value": "8.4.0"},
            {"Variable_name": None, "Value": "ignored"},
        ]
    )

    assert database.fetch_status_and_variables("status") == {
        "Threads_connected": 12,
        "version": "8.4.0",
    }
