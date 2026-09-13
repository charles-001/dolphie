from collections import namedtuple
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from dolphie.Dolphie import Dolphie

DiskUsage = namedtuple("DiskUsage", "total used free percent")
Partition = namedtuple("Partition", "device mountpoint fstype opts")


def make_dolphie(global_variables: dict[str, str]) -> Dolphie:
    dolphie = SimpleNamespace(
        enable_system_utilization=True,
        global_variables=global_variables,
        system_utilization={},
        _datadir_mount=("", None),
    )
    return cast(Dolphie, dolphie)


def test_records_the_filesystem_holding_the_data_directory(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    asked: list[str] = []

    def disk_usage(path: str) -> DiskUsage:
        asked.append(path)
        return DiskUsage(total=1000, used=250, free=750, percent=25.0)

    monkeypatch.setattr("dolphie.Dolphie.psutil.disk_usage", disk_usage)
    # The root filesystem also contains the path. The deepest mount is the one that holds it.
    volume = tmp_path / "var" / "lib"
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


def test_leaves_disk_usage_out_before_variables_arrive_or_when_the_path_is_unreachable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def missing(path: str) -> DiskUsage:
        raise FileNotFoundError(path)

    monkeypatch.setattr("dolphie.Dolphie.psutil.disk_usage", missing)
    monkeypatch.setattr("dolphie.Dolphie.psutil.disk_partitions", lambda **_: [])

    first_poll = make_dolphie({})
    Dolphie.collect_system_utilization(first_poll)
    assert "Datadir_Total" not in first_poll.system_utilization
    assert "CPU_Percent" in first_poll.system_utilization

    unreachable = make_dolphie({"datadir": "/container/only/"})
    Dolphie.collect_system_utilization(unreachable)
    assert "Datadir_Total" not in unreachable.system_utilization
