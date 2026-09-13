"""Read replay files the way an external tool does, without the code under test."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import orjson
import zstandard as zstd


def read_replay_rows(replay_file: Path, columns: str) -> list[tuple[Any, ...]]:
    """Every replay_data row in id order, with each blob column decoded using the stored dictionary."""
    connection = sqlite3.connect(f"{replay_file.resolve().as_uri()}?mode=ro", uri=True)
    try:
        (dictionary,) = connection.execute("SELECT compression_dict FROM metadata").fetchone()
        decompressor = zstd.ZstdDecompressor(dict_data=zstd.ZstdCompressionDict(dictionary) if dictionary else None)
        rows = connection.execute(f"SELECT {columns} FROM replay_data ORDER BY id").fetchall()
    finally:
        connection.close()
    return [
        tuple(orjson.loads(decompressor.decompress(value)) if isinstance(value, bytes) else value for value in row)
        for row in rows
    ]
