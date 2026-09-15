import os
import sqlite3
from collections import namedtuple
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from dolphie.Dolphie import Dolphie, mount_holding

DiskUsage = namedtuple("DiskUsage", "total used free percent")
Partition = namedtuple("Partition", "device mountpoint fstype opts")


def make_dolphie(global_variables: dict[str, str]) -> Dolphie:
    mount_holding.cache_clear()
    dolphie = SimpleNamespace(
        enable_system_utilization=True,
        global_variables=global_variables,
        system_utilization={},
    )
    return cast(Dolphie, dolphie)


def test_lists_the_sqlite_databases_and_not_the_files_sqlite_keeps_beside_them(tmp_path: Path) -> None:
    host_dir = tmp_path / "localhost_3306"
    host_dir.mkdir()
    # A daemon recording in WAL mode, an old-schema file it renamed, and a stray operator copy
    with closing(sqlite3.connect(host_dir / "daemon.db")) as connection:
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("CREATE TABLE replay_data (id INTEGER PRIMARY KEY)")
        connection.execute("INSERT INTO replay_data DEFAULT VALUES")
        assert {p.name for p in host_dir.iterdir()} >= {"daemon.db", "daemon.db-wal", "daemon.db-shm"}
        with closing(sqlite3.connect(host_dir / "daemon.db_old_schema_v1")) as old:
            old.execute("CREATE TABLE replay_data (id INTEGER PRIMARY KEY)")
        (host_dir / "notes.txt").write_text("not a replay")
        dolphie = cast(Dolphie, SimpleNamespace(replay_dir=str(tmp_path)))

        listed = [path for path, _ in Dolphie.get_replay_files(dolphie)]

    assert listed == [str(host_dir / "daemon.db"), str(host_dir / "daemon.db_old_schema_v1")]


def test_records_the_filesystem_holding_the_data_directory(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    asked: list[str] = []

    def disk_usage(path: str) -> DiskUsage:
        asked.append(path)
        return DiskUsage(total=1000, used=250, free=750, percent=25.0)

    monkeypatch.setattr("dolphie.Dolphie.psutil.disk_usage", disk_usage)
    # The root filesystem also contains the path. The deepest mount is the one that holds it.
    volume = tmp_path / "var" / "lib"
    (volume / "mysql").mkdir(parents=True)
    datadir = f"{volume / 'mysql'}/"
    mount_table_reads = 0

    def disk_partitions(**_: object) -> list[Partition]:
        nonlocal mount_table_reads
        mount_table_reads += 1
        return [Partition("/dev/a", "/", "ext4", ""), Partition("/dev/b", str(volume), "xfs", "")]

    monkeypatch.setattr("dolphie.Dolphie.psutil.disk_partitions", disk_partitions)
    dolphie = make_dolphie({"datadir": datadir})

    Dolphie.collect_system_utilization(dolphie)
    Dolphie.collect_system_utilization(dolphie)

    # Usage is read at the mount, which stays readable when the data directory is mysql-only
    assert asked == [str(volume), str(volume)]
    assert dolphie.system_utilization["Datadir_Total"] == 1000
    assert dolphie.system_utilization["Datadir_Used"] == 250
    assert dolphie.system_utilization["Datadir_Mount"] == str(volume)
    # Usage is polled. The mount table is read once for the datadir.
    assert mount_table_reads == 1


def test_reads_usage_at_the_mount_when_the_data_directory_denies_access(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    volume = tmp_path / "data"
    datadir = volume / "mysql" / "data"
    datadir.mkdir(parents=True)
    real_stat = os.stat

    def mysql_only(path: str, *args: object, **kwargs: object) -> os.stat_result:
        if path == str(datadir):
            raise PermissionError(path)
        return real_stat(path, *args, **kwargs)

    monkeypatch.setattr("dolphie.Dolphie.os.stat", mysql_only)
    monkeypatch.setattr("dolphie.Dolphie.psutil.disk_usage", lambda _: DiskUsage(1000, 250, 750, 25.0))
    monkeypatch.setattr(
        "dolphie.Dolphie.psutil.disk_partitions",
        lambda **_: [Partition("/dev/a", "/", "ext4", ""), Partition("/dev/b", str(volume), "xfs", "")],
    )
    dolphie = make_dolphie({"datadir": f"{datadir}/"})

    Dolphie.collect_system_utilization(dolphie)

    assert dolphie.system_utilization["Datadir_Mount"] == str(volume)
    assert dolphie.system_utilization["Datadir_Total"] == 1000


def test_leaves_disk_usage_out_before_variables_arrive_or_when_the_path_is_not_on_this_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The root mount holds every path, so a remote server's data directory must not be read as
    # this host's root filesystem
    monkeypatch.setattr("dolphie.Dolphie.psutil.disk_usage", lambda _: DiskUsage(1000, 250, 750, 25.0))
    monkeypatch.setattr("dolphie.Dolphie.psutil.disk_partitions", lambda **_: [Partition("/dev/a", "/", "ext4", "")])

    first_poll = make_dolphie({})
    Dolphie.collect_system_utilization(first_poll)
    assert "Datadir_Total" not in first_poll.system_utilization
    assert "CPU_Percent" in first_poll.system_utilization

    remote = make_dolphie({"datadir": "/container/only/"})
    Dolphie.collect_system_utilization(remote)
    assert "Datadir_Total" not in remote.system_utilization
    assert "Datadir_Mount" not in remote.system_utilization
