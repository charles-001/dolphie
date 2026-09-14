"""Read replay files the way an external tool does, without the code under test."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Any

import orjson
import zstandard as zstd


def open_read_only(replay_file: Path) -> sqlite3.Connection:
    """Close the returned connection. ``with connection`` alone leaves it open and pins the daemon's WAL."""
    return sqlite3.connect(f"{replay_file.resolve().as_uri()}?mode=ro", uri=True)


def journal_mode(replay_file: Path) -> str:
    with closing(open_read_only(replay_file)) as connection:
        return connection.execute("PRAGMA journal_mode").fetchone()[0]


def row_count(replay_file: Path) -> int:
    with closing(open_read_only(replay_file)) as connection:
        return connection.execute("SELECT count(*) FROM replay_data").fetchone()[0]


def read_compression_dict(replay_file: Path) -> bytes | None:
    with closing(open_read_only(replay_file)) as connection:
        return connection.execute("SELECT compression_dict FROM metadata").fetchone()[0]


def sidecars(replay_file: Path) -> set[str]:
    """Names next to the replay file: its -wal and -shm while open in WAL mode, nothing after a clean close."""
    return {path.name for path in replay_file.parent.iterdir()} - {replay_file.name}


def read_replay_rows(replay_file: Path, columns: str) -> list[tuple[Any, ...]]:
    """Every replay_data row in id order, with each blob column decoded using the stored dictionary."""
    dictionary = read_compression_dict(replay_file)
    decompressor = zstd.ZstdDecompressor(dict_data=zstd.ZstdCompressionDict(dictionary) if dictionary else None)
    with closing(open_read_only(replay_file)) as connection:
        rows = connection.execute(f"SELECT {columns} FROM replay_data ORDER BY id").fetchall()
    return [
        tuple(orjson.loads(decompressor.decompress(value)) if isinstance(value, bytes) else value for value in row)
        for row in rows
    ]
