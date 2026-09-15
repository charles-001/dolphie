import os
import sqlite3
from collections import namedtuple
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from dolphie.Dolphie import Dolphie, disk_io_counts, mount_holding, mounted_devices, network_io_bytes

DiskUsage = namedtuple("DiskUsage", "total used free percent")
Partition = namedtuple("Partition", "device mountpoint fstype opts")
DiskIO = namedtuple("DiskIO", "read_count write_count")
NetIO = namedtuple("NetIO", "bytes_sent bytes_recv")


def make_dolphie(global_variables: dict[str, str]) -> Dolphie:
    mount_holding.cache_clear()
    mounted_devices.cache_clear()
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
    # Usage is polled. The mount table is read once for the datadir and once for the IO devices.
    assert mount_table_reads == 2


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


def test_counts_each_io_once_across_the_devices_that_carry_a_filesystem(monkeypatch: pytest.MonkeyPatch) -> None:
    # /data on a RAID10 of four members, root on LVM over RAID1, plus the loop devices snap leaves
    # behind. psutil's own total adds every one of these.
    per_disk = {
        "md2": DiskIO(170, 720),
        "nvme0n1": DiskIO(66, 626),
        "nvme1n1": DiskIO(4, 626),
        "nvme2n1": DiskIO(94, 638),
        "nvme3n1": DiskIO(6, 638),
        "dm-0": DiskIO(0, 180),
        "md1": DiskIO(0, 180),
        "nvme5n1": DiskIO(0, 176),
        "nvme6n1": DiskIO(0, 176),
        "nvme6n1p2": DiskIO(1, 2),
        "loop0": DiskIO(3, 0),
    }
    mounted_devices.cache_clear()
    monkeypatch.setattr("dolphie.Dolphie.psutil.disk_io_counters", lambda perdisk=False: per_disk if perdisk else None)
    monkeypatch.setattr(
        "dolphie.Dolphie.psutil.disk_partitions",
        lambda **_: [
            Partition("/dev/md2", "/data", "xfs", ""),
            Partition("/dev/mapper/vg0-root", "/", "ext4", ""),
            Partition("/dev/nvme6n1p2", "/boot/efi", "vfat", ""),
        ],
    )
    # The mapper name is a link to the device the counters know
    monkeypatch.setattr("dolphie.Dolphie.os.path.realpath", lambda p: "/dev/dm-0" if p.endswith("vg0-root") else p)

    assert disk_io_counts() == (170 + 0 + 1, 720 + 180 + 2)


def test_falls_back_to_the_total_where_counter_names_do_not_match_the_mount_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # macOS mounts /dev/disk3s1s1 and counts under disk3
    mounted_devices.cache_clear()
    monkeypatch.setattr(
        "dolphie.Dolphie.psutil.disk_io_counters",
        lambda perdisk=False: {"disk3": DiskIO(10, 20)} if perdisk else DiskIO(10, 20),
    )
    monkeypatch.setattr(
        "dolphie.Dolphie.psutil.disk_partitions", lambda **_: [Partition("/dev/disk3s1s1", "/", "apfs", "")]
    )

    assert disk_io_counts() == (10, 20)


def test_counts_a_bond_once_and_leaves_loopback_out(monkeypatch: pytest.MonkeyPatch) -> None:
    per_nic = {
        "lo": NetIO(1670, 1670),
        "bond0": NetIO(15_966_098, 5_375_957),
        "eth0": NetIO(7_671_973, 2_701_103),
        "eth1": NetIO(8_294_125, 2_674_853),
        "eth2": NetIO(0, 0),
    }
    monkeypatch.setattr("dolphie.Dolphie.psutil.net_io_counters", lambda pernic=False: per_nic)
    # A bond member names its master in sysfs. The bond itself, and a NIC on its own, do not.
    monkeypatch.setattr(
        "dolphie.Dolphie.os.path.exists", lambda p: p in {"/sys/class/net/eth0/master", "/sys/class/net/eth1/master"}
    )

    assert network_io_bytes() == (15_966_098, 5_375_957)
