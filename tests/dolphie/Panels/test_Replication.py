from types import SimpleNamespace
from typing import cast
from unittest.mock import MagicMock

from dolphie.DataTypes import ConnectionSource
from dolphie.Modules.TabManager import Tab
from dolphie.Panels.Replication import _filter_gtid_sets, fetch_replicas


def test_filter_gtid_sets_ignores_excluded_sources():
    gtid_sets = "source-a:1-10,\nsource-b:1-5"

    assert _filter_gtid_sets(gtid_sets, {"source-a"}) == "source-b:1-5"


def test_fetch_replicas_skips_rows_without_hosts():
    replica_manager = MagicMock()
    replica_manager.available_replicas = [{}]
    replica_manager.replicas = {}
    replica_manager.ports = {}
    tab = SimpleNamespace(
        dolphie=SimpleNamespace(
            connection_source_alt=ConnectionSource.mysql,
            replica_manager=replica_manager,
        )
    )

    fetch_replicas(cast(Tab, tab))

    replica_manager.create_replica_row_key.assert_not_called()
    replica_manager.add_replica.assert_not_called()


def test_fetch_replicas_skips_mariadb_rows_without_usable_ports():
    replica_manager = MagicMock()
    replica_manager.available_replicas = [{"id": 1, "host": "127.0.0.1:3306", "user": "replica"}]
    replica_manager.replicas = {}
    replica_manager.ports = {"missing-port": {}}
    replica_manager.get_replica.return_value = None
    tab = SimpleNamespace(
        dolphie=SimpleNamespace(
            connection_source_alt=ConnectionSource.mariadb,
            get_hostname=lambda host: host,
            replica_manager=replica_manager,
        )
    )

    fetch_replicas(cast(Tab, tab))

    replica_manager.add_replica.assert_not_called()
