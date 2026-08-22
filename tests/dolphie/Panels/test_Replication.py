import threading
import time
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from textual.worker import Worker, WorkerState

from dolphie.DataTypes import (
    ConnectionSource,
    ConnectionStatus,
    DatabaseRow,
    Replica,
    ReplicaManager,
    ReplicaRow,
)
from dolphie.Modules.Functions import coerce_str, host_without_port
from dolphie.Modules.Queries import MySQLQueries
from dolphie.Modules.TabManager import Tab
from dolphie.Modules.WorkerDataProcessor import (
    WorkerDataProcessor,
    build_replica_discovery,
    is_group_replication_primary,
)
from dolphie.Modules.WorkerManager import WorkerManager
from dolphie.Panels import Replication
from dolphie.Panels.Replication import (
    _detect_mariadb_errant_trx,
    _filter_gtid_sets,
    _format_clusterset_replication,
    _record_replica_error,
    _refresh_errant_transactions,
    fetch_replicas,
    fetch_replication_data,
    replication_channel_priority,
    user_replication_channels,
)


def test_filter_gtid_sets_ignores_excluded_sources():
    gtid_sets = "source-a:1-10,\nsource-b:1-5"

    assert _filter_gtid_sets(gtid_sets, {"source-a"}) == "source-b:1-5"


def test_clusterset_secondary_reports_channel_as_managed_by_primary():
    status, error = _format_clusterset_replication(
        {
            "Replica_IO_Running": "No",
            "Replica_SQL_Running": "No",
        },
        is_cluster_primary=False,
    )

    assert "Managed by cluster PRIMARY" in status
    assert "$light_blue" in status
    assert error is None


def test_clusterset_primary_reports_async_channel_health():
    status, error = _format_clusterset_replication(
        {
            "Replica_IO_Running": "Yes",
            "Replica_SQL_Running": "Yes",
            "Seconds_Behind_Source": 3,
        },
        is_cluster_primary=True,
    )

    assert "IO ON" in status
    assert "SQL ON" in status
    assert "00:00:03" in status
    assert error is None


def test_replica_panel_replaces_shared_title_while_replicas_load(monkeypatch: pytest.MonkeyPatch):
    sync_grid = MagicMock()
    monkeypatch.setattr(Replication, "_sync_grid", sync_grid)
    title = MagicMock()
    tab = cast(
        Tab,
        SimpleNamespace(
            id="host-2",
            dolphie=SimpleNamespace(
                app=MagicMock(),
                panels=SimpleNamespace(replication=SimpleNamespace()),
                replay_file=None,
                replica_manager=SimpleNamespace(active_count=0, discovery_count=2),
            ),
            replica_widgets={},
            replicas_container=SimpleNamespace(display=False),
            replicas_grid=MagicMock(),
            replicas_loading_indicator=SimpleNamespace(display=False),
            replicas_title=title,
        ),
    )

    Replication.create_replica_panel(tab)

    assert tab.replicas_container.display is True
    assert tab.replicas_loading_indicator.display is True
    assert "Loading [$highlight]2[/$highlight] replicas" in title.update.call_args.args[0]
    sync_grid.assert_called_once_with(
        tab.replicas_grid,
        {},
        "replica",
        tab.id,
        tab.dolphie.app,
        tab.replica_widgets,
    )


def test_managed_channels_are_excluded_from_generic_replication():
    statuses: list[DatabaseRow] = [
        {"Channel_Name": "group_replication_recovery"},
        {"Channel_Name": "clusterset_replication"},
        {"Channel_Name": "customer_channel"},
    ]

    assert user_replication_channels(statuses) == [{"Channel_Name": "customer_channel"}]


def test_replication_channel_priority_surfaces_failures_before_lag():
    healthy = {
        "Replica_IO_Running": "Yes",
        "Replica_SQL_Running": "Yes",
        "Seconds_Behind": 30,
    }
    unknown = {
        "Replica_IO_Running": "Yes",
        "Replica_SQL_Running": "Yes",
        "Seconds_Behind": None,
    }
    stopped = {
        "Replica_IO_Running": "Yes",
        "Replica_SQL_Running": "No",
        "Seconds_Behind": None,
    }

    assert max([healthy, stopped, unknown], key=replication_channel_priority) is stopped


def test_mariadb_errant_detection_ignores_malformed_gtids():
    assert _detect_mariadb_errant_trx("0-7-invalid,0-7-12", 7, "0-7-10") == "0-7-12"


def test_mariadb_errant_detection_compares_sequence_by_domain():
    assert _detect_mariadb_errant_trx("0-7-12", 7, "0-1-20") is None
    assert _detect_mariadb_errant_trx("0-7-21", 7, "0-1-20") == "0-7-21"


@pytest.mark.parametrize(
    ("address", "host"),
    [
        ("db.example.com:3306", "db.example.com"),
        ("10.0.0.1:49152", "10.0.0.1"),
        ("[2001:db8::1]:3306", "2001:db8::1"),
        ("2001:db8::1", "2001:db8::1"),
        ("db.example.com", "db.example.com"),
    ],
)
def test_host_without_port(address: str, host: str):
    assert host_without_port(address) == host


def test_mysql_discovery_uses_uuid_identity_and_refreshes_same_size_port_changes():
    processlist_rows = [
        {"id": 10, "user": "repl", "host": "10.0.0.2:49152", "replica_uuid": "uuid-2"},
    ]

    first = build_replica_discovery(
        processlist_rows,
        [{"Replica_UUID": "uuid-2", "Host": "replica-2", "Port": 3306}],
        [],
        mariadb=False,
        use_show_replicas=True,
    )
    second = build_replica_discovery(
        processlist_rows,
        [{"Replica_UUID": "uuid-2", "Host": "replica-2", "Port": 4406}],
        first,
        mariadb=False,
        use_show_replicas=True,
    )

    assert first[0].get("identity") == second[0].get("identity") == "mysql-uuid:uuid-2"
    assert first[0].get("port") == 3306
    assert second[0].get("port") == 4406


def test_mysql_discovery_pairs_reported_host_with_reported_port():
    # report_host/report_port exist so a replica behind NAT or on a container-internal
    # network can advertise the address a monitor should actually connect through —
    # the processlist-visible peer address (here, a Docker bridge IP) may be
    # unreachable from outside. Verified against dolphie's own GR docker fixture,
    # where the primary's processlist shows the replica at its internal bridge IP
    # while SHOW REPLICAS reports the host-mapped 127.0.0.1:<published-port>.
    discovered = build_replica_discovery(
        [{"id": 10, "user": "repl", "host": "172.28.1.5:49152", "replica_uuid": "replica-uuid"}],
        [{"Replica_UUID": "replica-uuid", "Host": "127.0.0.1", "Port": 3324}],
        [],
        mariadb=False,
        use_show_replicas=True,
    )

    assert discovered[0].get("host") == "127.0.0.1"
    assert discovered[0].get("port") == 3324


def test_uuid_less_discovery_keeps_same_host_replicas_distinct():
    discovered = build_replica_discovery(
        [
            {"id": 10, "user": "repl", "host": "10.0.0.2:49152", "replica_uuid": ""},
            {"id": 20, "user": "repl", "host": "10.0.0.2:49153", "replica_uuid": ""},
        ],
        [],
        [],
        mariadb=False,
        use_show_replicas=False,
    )

    assert len({row.get("identity") for row in discovered}) == 2


def test_mariadb_discovery_is_stable_when_report_order_changes():
    processlist_rows = [
        {"id": 10, "user": "repl", "host": "replica-a:49152"},
        {"id": 20, "user": "repl", "host": "replica-b:49153"},
    ]
    reported_rows = [
        {"Server_id": 101, "Host": "replica-a", "Port": 3307},
        {"Server_id": 202, "Host": "replica-b", "Port": 3308},
    ]

    first = build_replica_discovery(
        processlist_rows,
        reported_rows,
        [],
        mariadb=True,
        use_show_replicas=False,
    )
    second = build_replica_discovery(
        list(reversed(processlist_rows)),
        list(reversed(reported_rows)),
        first,
        mariadb=True,
        use_show_replicas=False,
    )

    first_by_thread = {row.get("id"): row.get("identity") for row in first}
    second_by_thread = {row.get("id"): row.get("identity") for row in second}
    assert (
        first_by_thread
        == second_by_thread
        == {
            10: "mariadb-id:101",
            20: "mariadb-id:202",
        }
    )


def test_mariadb_discovery_does_not_pair_unrelated_reported_hosts():
    discovered = build_replica_discovery(
        [
            {"id": 10, "user": "repl", "host": "replica-a:49152"},
            {"id": 20, "user": "repl", "host": "replica-b:49153"},
        ],
        [
            {"Server_id": 101, "Host": "other-a", "Port": 3307},
            {"Server_id": 202, "Host": "other-b", "Port": 3308},
        ],
        [],
        mariadb=True,
        use_show_replicas=False,
    )

    assert {(row.get("host"), row.get("port")) for row in discovered} == {
        ("replica-a", 3306),
        ("replica-b", 3306),
    }
    assert all(coerce_str(row.get("identity")).startswith("endpoint:") for row in discovered)


def test_mariadb_discovery_pairs_a_single_unmatched_report_by_rotation():
    # A lone replica whose report_host doesn't match the processlist IP must still
    # get its reported port instead of silently defaulting to 3306
    discovered = build_replica_discovery(
        [{"id": 10, "user": "repl", "host": "10.0.0.5:49152"}],
        [{"Server_id": 101, "Host": "replica-a", "Port": 3307}],
        [],
        mariadb=True,
        use_show_replicas=False,
    )

    assert discovered[0].get("host") == "replica-a"
    assert discovered[0].get("port") == 3307
    assert discovered[0].get("identity") == "mariadb-id:101"


def test_duplicate_reported_identities_are_disambiguated_by_endpoint():
    discovered = build_replica_discovery(
        [
            {"id": 10, "user": "repl", "host": "replica-a:49152"},
            {"id": 20, "user": "repl", "host": "replica-b:49153"},
        ],
        [
            {"Server_id": 101, "Host": "replica-a", "Port": 3307},
            {"Server_id": 101, "Host": "replica-b", "Port": 3308},
        ],
        [],
        mariadb=True,
        use_show_replicas=False,
    )

    assert len({row.get("identity") for row in discovered}) == 2
    assert all(":endpoint:" in coerce_str(row.get("identity")) for row in discovered)


def test_group_replication_primary_is_computed_from_polled_members():
    members: list[DatabaseRow] = [
        {"MEMBER_ID": "secondary", "MEMBER_ROLE": "SECONDARY"},
        {"MEMBER_ID": "primary", "MEMBER_ROLE": "PRIMARY"},
    ]

    assert is_group_replication_primary(members, "primary")
    assert not is_group_replication_primary(members, "secondary")
    assert not is_group_replication_primary([], "primary")


@pytest.mark.parametrize(
    ("is_primary", "clusterset_role", "expects_warning"),
    [
        (False, None, False),
        (True, "REPLICA", False),
        (True, "PRIMARY", True),
    ],
)
def test_read_only_warning_respects_innodb_cluster_role(
    is_primary: bool,
    clusterset_role: str | None,
    expects_warning: bool,
):
    app = SimpleNamespace(
        notify=MagicMock(),
        tab_manager=SimpleNamespace(update_connection_status=MagicMock()),
    )
    processor = WorkerDataProcessor(cast(Any, app))
    tab = SimpleNamespace(
        dolphie=SimpleNamespace(
            connection_source=ConnectionSource.mysql,
            connection_status=ConnectionStatus.read_write,
            global_variables={"read_only": "ON"},
            group_replication=False,
            group_replication_data={"clusterset_role": clusterset_role},
            host_with_port="db:3306",
            innodb_cluster=True,
            is_group_replication_primary=is_primary,
            replication_status=[],
        )
    )

    processor.monitor_read_only_change(cast(Tab, tab))

    message = app.notify.call_args.kwargs["message"]
    assert ("SHOULD BE READ/WRITE?" in message) is expects_warning


def test_failed_port_correlation_still_publishes_processlist_discovery():
    replica_manager = ReplicaManager()
    original: list[ReplicaRow] = [{"id": 1, "host": "replica", "identity": "mysql-uuid:old", "port": 3306}]
    replica_manager.replace_discovery(original)
    connection = MagicMock()

    def execute(query: str):
        connection.last_execute_successful = query != "SHOW REPLICAS"

    connection.execute.side_effect = execute
    connection.fetchall.return_value = [{"id": 2, "user": "repl", "host": "new-replica:49152", "replica_uuid": "new"}]
    tab = SimpleNamespace(
        id="tab1",
        dolphie=SimpleNamespace(
            connection_source_alt=ConnectionSource.mysql,
            daemon_mode=False,
            is_mysql_version_at_least=lambda _version: True,
            main_db_connection=connection,
            performance_schema_enabled=True,
            replica_manager=replica_manager,
            replicaset=False,
        ),
    )

    WorkerDataProcessor(MagicMock())._refresh_replica_discovery(cast(Tab, tab))

    # A failing SHOW REPLICAS (e.g. missing REPLICATION SLAVE privilege) must not
    # block processlist-based discovery — replicas are published without port
    # correlation and the reported query is retried next cycle (nothing cached)
    assert replica_manager.available_replicas == [
        {
            "id": 2,
            "user": "repl",
            "host": "new-replica",
            "replica_uuid": "new",
            "identity": "mysql-uuid:new",
            "port": 3306,
        }
    ]
    assert replica_manager.reported_replica_signature is None


def test_replication_source_uuids_are_cleared_when_replication_stops():
    connection = MagicMock()
    connection.fetchall.return_value = []
    tab = SimpleNamespace(
        dolphie=SimpleNamespace(
            connection_source_alt=ConnectionSource.mysql,
            heartbeat_table=None,
            host_version="8.0.36",
            is_mysql_version_at_least=lambda *_args, **_kwargs: True,
            main_db_connection=connection,
            polling_latency=1,
            replication_source_uuids={"stale-source"},
            replication_status=[],
        )
    )

    assert fetch_replication_data(cast(Tab, tab)) == []
    assert tab.dolphie.replication_source_uuids == set()


def test_multi_source_replica_sources_are_not_reported_as_errant():
    source_a = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    source_b = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    local = "cccccccc-cccc-cccc-cccc-cccccccccccc"
    connection = MagicMock()
    connection.fetchall.return_value = [
        {
            "Channel_Name": "channel_a",
            "Source_UUID": source_a,
            "Seconds_Behind_Source": 4,
            "Executed_Gtid_Set": f"{source_a}:1-11,\n{source_b}:1-20,\n{local}:1",
            "Retrieved_Gtid_Set": f"{source_a}:1-11",
        },
        {
            "Channel_Name": "channel_b",
            "Source_UUID": source_b,
            "Seconds_Behind_Source": 0,
        },
    ]
    replica = Replica(
        identity="mysql-uuid:replica",
        row_key="replica",
        host="replica",
        port=3306,
        connection=connection,
        connection_source_alt=ConnectionSource.mysql,
        mysql_version="8.4.8",
    )
    tab = SimpleNamespace(
        dolphie=SimpleNamespace(
            connection_source_alt=ConnectionSource.mysql,
            global_variables={"gtid_executed": f"{source_a}:1-10"},
            group_replication=False,
            innodb_cluster=False,
            heartbeat_table="heartbeat.replication",
            host_version="8.4.8",
            is_mysql_version_at_least=lambda *_args, **_kwargs: True,
            polling_latency=1,
            replication_source_uuids=set(),
            replication_status=[],
            server_uuid=source_a,
        )
    )

    # Heartbeat lag is preferred over the channel's Seconds_Behind_Source when configured
    connection.fetchone.return_value = {"Seconds_Behind_Source": 4}

    status = fetch_replication_data(cast(Tab, tab), replica)
    assert isinstance(status, dict)
    replica.replication_status = status
    assert replica.replication_source_uuids == {source_a, source_b}
    assert status["Seconds_Behind"] == 4

    connection.reset_mock()
    connection.last_execute_successful = True
    connection.fetchone.return_value = {"errant_trxs": f"{local}:1"}
    _refresh_errant_transactions(cast(Tab, tab), replica, current_time=100)

    _, query_values = connection.execute.call_args.args
    assert query_values == (
        f"{source_a}:1-11,\n{source_b}:1-20,\n{local}:1",
        f"{source_a}:1-10",
        f"{source_a}:1-11",
    )
    assert replica.errant_transactions == f"{local}:1"


@pytest.mark.parametrize(
    ("mysql_version", "supports_replica_term", "expected_query"),
    [
        ("11.4.7-MariaDB", True, MySQLQueries.show_all_replicas_status),
        ("10.4.34-MariaDB", False, MySQLQueries.show_all_slaves_status),
    ],
)
def test_mariadb_multi_source_replica_selects_primary_connection(
    mysql_version: str,
    supports_replica_term: bool,
    expected_query: str,
):
    connection = MagicMock()
    connection.fetchall.return_value = [
        {
            "Connection_name": "analytics",
            "Master_Server_Id": 12,
            "Seconds_Behind_Master": 3,
        },
        {
            "Connection_name": "primary",
            "Master_Server_Id": 11,
            "Seconds_Behind_Master": 1,
        },
    ]
    replica = Replica(
        identity="mariadb:replica",
        row_key="replica",
        host="replica",
        port=3306,
        connection=connection,
        connection_source_alt=ConnectionSource.mariadb,
        mysql_version=mysql_version,
    )
    tab = SimpleNamespace(
        dolphie=SimpleNamespace(
            heartbeat_table=None,
            is_mysql_version_at_least=lambda minimum, **_kwargs: supports_replica_term and minimum == "10.5.1",
            polling_latency=1,
            replication_source_uuids=set(),
            server_uuid=11,
        )
    )

    status = fetch_replication_data(cast(Tab, tab), replica)

    assert isinstance(status, dict)
    assert connection.execute.call_args.args == (expected_query,)
    assert status["Channel_Name"] == "primary"
    assert status["Seconds_Behind"] == 1


def test_unknown_replica_lag_is_not_coerced_to_zero():
    connection = MagicMock()
    connection.fetchall.return_value = [
        {
            "Source_UUID": "primary",
            "Seconds_Behind_Source": None,
        }
    ]
    replica = Replica(
        identity="mysql-uuid:replica",
        row_key="replica",
        host="replica",
        port=3306,
        connection=connection,
        connection_source_alt=ConnectionSource.mysql,
        mysql_version="8.4.8",
    )
    tab = SimpleNamespace(
        dolphie=SimpleNamespace(
            heartbeat_table=None,
            is_mysql_version_at_least=lambda *_args, **_kwargs: True,
            polling_latency=1,
            replication_source_uuids=set(),
            server_uuid="primary",
        )
    )

    status = fetch_replication_data(cast(Tab, tab), replica)

    assert isinstance(status, dict)
    assert status["Seconds_Behind"] is None
    assert status["Replica_Speed"] == 0


def test_multi_source_without_primary_match_falls_back_to_first_channel():
    # No channel matching the monitored source (IO thread reconnecting, or the
    # replica reaches the primary via a VIP/proxy) must not fail the poll
    connection = MagicMock()
    connection.fetchall.return_value = [
        {"Source_UUID": "other-a", "Seconds_Behind_Source": 7},
        {"Source_UUID": "other-b", "Seconds_Behind_Source": 0},
    ]
    replica = Replica(
        identity="mysql-uuid:replica",
        row_key="replica",
        host="replica",
        port=3306,
        connection=connection,
        connection_source_alt=ConnectionSource.mysql,
        mysql_version="8.4.8",
    )
    tab = SimpleNamespace(
        dolphie=SimpleNamespace(
            heartbeat_table=None,
            is_mysql_version_at_least=lambda *_args, **_kwargs: True,
            replication_source_uuids=set(),
            server_uuid="primary",
        )
    )

    status = fetch_replication_data(cast(Tab, tab), replica)
    assert isinstance(status, dict)
    assert status.get("Source_UUID") == "other-a"
    assert status.get("Seconds_Behind") == 7


def test_innodb_cluster_group_gtids_are_not_reported_as_errant():
    source_uuid = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    group_uuid = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    view_change_uuid = "cccccccc-cccc-cccc-cccc-cccccccccccc"
    connection = MagicMock()
    connection.last_execute_successful = True
    connection.fetchone.return_value = {"errant_trxs": ""}
    replica = Replica(
        identity="mysql-uuid:replica",
        row_key="replica",
        host="replica",
        port=3306,
        connection=connection,
        group_replication_view_change_uuid=view_change_uuid,
        replication_source_uuids={source_uuid},
        replication_status={
            "Executed_Gtid_Set": f"{source_uuid}:1-9,\n{group_uuid}:1-101,\n{view_change_uuid}:1-2",
            "Retrieved_Gtid_Set": f"{group_uuid}:1-101",
        },
    )
    tab = SimpleNamespace(
        dolphie=SimpleNamespace(
            global_variables={
                "group_replication_group_name": group_uuid,
                "gtid_executed": f"{source_uuid}:1-9,\n{group_uuid}:1-100,\n{view_change_uuid}:1-2",
            },
            group_replication=False,
            innodb_cluster=True,
            replication_source_uuids=set(),
            server_uuid=source_uuid,
        )
    )

    _refresh_errant_transactions(cast(Tab, tab), replica, current_time=100)

    _, query_values = connection.execute.call_args.args
    replica_gtid_set = f"{source_uuid}:1-9,\n{group_uuid}:1-101,\n{view_change_uuid}:1-2"
    primary_gtid_set = f"{source_uuid}:1-9,\n{group_uuid}:1-100,\n{view_change_uuid}:1-2"
    assert query_values == (replica_gtid_set, primary_gtid_set, f"{group_uuid}:1-101")
    assert replica.errant_transactions is None


def test_same_source_gtid_divergence_is_reported_as_errant():
    source_uuid = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    unrelated_uuid = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    connection = MagicMock()
    connection.last_execute_successful = True
    connection.fetchone.return_value = {
        "errant_trxs": f"{source_uuid}:11-20,\n{unrelated_uuid}:1-5",
    }
    replica = Replica(
        identity="mysql-uuid:replica",
        row_key="replica",
        host="replica",
        port=3306,
        connection=connection,
        replication_source_uuids={source_uuid, unrelated_uuid},
        replication_status={
            "Executed_Gtid_Set": f"{source_uuid}:1-20,\n{unrelated_uuid}:1-5",
            "Retrieved_Gtid_Set": f"{source_uuid}:1-10",
        },
    )
    tab = SimpleNamespace(
        dolphie=SimpleNamespace(
            global_variables={"gtid_executed": f"{source_uuid}:1-10"},
            replication_source_uuids=set(),
            server_uuid=source_uuid,
        )
    )

    _refresh_errant_transactions(cast(Tab, tab), replica, current_time=100)

    _, query_values = connection.execute.call_args.args
    assert query_values == (
        f"{source_uuid}:1-20,\n{unrelated_uuid}:1-5",
        f"{source_uuid}:1-10",
        f"{source_uuid}:1-10",
    )
    assert replica.errant_transactions == f"{source_uuid}:11-20"


def test_cleared_retrieved_gtid_set_does_not_report_source_gtids_as_errant():
    # A replica restart clears Retrieved_Gtid_Set, so a stale primary gtid_executed
    # snapshot must not surface the primary's own recent GTIDs as errant
    source_uuid = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    connection = MagicMock()
    connection.last_execute_successful = True
    connection.fetchone.return_value = {"errant_trxs": f"{source_uuid}:11-12"}
    replica = Replica(
        identity="mysql-uuid:replica",
        row_key="replica",
        host="replica",
        port=3306,
        connection=connection,
        replication_source_uuids={source_uuid},
        replication_status={
            "Executed_Gtid_Set": f"{source_uuid}:1-12",
            "Retrieved_Gtid_Set": "",
        },
    )
    tab = SimpleNamespace(
        dolphie=SimpleNamespace(
            global_variables={"gtid_executed": f"{source_uuid}:1-10"},
            replication_source_uuids=set(),
            server_uuid=source_uuid,
        )
    )

    _refresh_errant_transactions(cast(Tab, tab), replica, current_time=100)

    assert replica.errant_transactions is None


def test_replica_manager_publishes_detached_snapshots_and_preserves_identity():
    manager = ReplicaManager()
    manager.replace_discovery([{"id": 1, "host": "replica", "identity": "mysql-uuid:abc", "port": 3306}])

    snapshot = manager.available_replicas
    snapshot[0]["host"] = "mutated"

    assert manager.available_replicas[0].get("host") == "replica"

    replica = manager.upsert_replica("mysql-uuid:abc", 1, "replica", 3306)
    connection = MagicMock()
    replica.connection = connection
    changed = manager.upsert_replica("mysql-uuid:abc", 2, "replica", 4406)

    assert changed is replica
    assert changed.row_key == manager.create_replica_row_key("mysql-uuid:abc")
    assert changed.thread_id == 2
    assert changed.host_with_port == "replica:4406"
    assert changed.connection is None
    connection.close.assert_called_once()


def test_fetch_replicas_reconciles_an_empty_discovery_snapshot():
    replica_manager = ReplicaManager()
    replica = replica_manager.upsert_replica("mysql-uuid:abc", 1, "replica", 3306)
    replica.connection = MagicMock()
    tab = SimpleNamespace(
        dolphie=SimpleNamespace(
            replica_manager=replica_manager,
        )
    )

    fetch_replicas(cast(Tab, tab))

    assert replica_manager.active_count == 0
    replica.connection.close.assert_called_once()


def test_fetch_replicas_bounds_concurrent_polling(monkeypatch: pytest.MonkeyPatch):
    replica_manager = ReplicaManager()
    replica_manager.replace_discovery(
        [
            {
                "id": index,
                "host": f"replica-{index}",
                "identity": f"mysql-uuid:{index}",
                "port": 3306,
            }
            for index in range(1, 9)
        ]
    )
    tab = SimpleNamespace(
        dolphie=SimpleNamespace(
            get_hostname=lambda host: host,
            replica_manager=replica_manager,
        )
    )
    state_lock = threading.Lock()
    active = 0
    maximum_active = 0

    def fake_poll(*_args):
        nonlocal active, maximum_active
        with state_lock:
            active += 1
            maximum_active = max(maximum_active, active)
        time.sleep(0.02)
        with state_lock:
            active -= 1

    monkeypatch.setattr(Replication, "_poll_replica", fake_poll)

    fetch_replicas(cast(Tab, tab))

    assert 1 < maximum_active <= 4
    assert replica_manager.active_count == 8


def test_fetch_replicas_polls_duplicate_identity_once(monkeypatch: pytest.MonkeyPatch):
    replica_manager = ReplicaManager()
    replica_manager.replace_discovery(
        [
            {"id": 1, "host": "replica-a", "identity": "mysql-uuid:duplicate", "port": 3306},
            {"id": 2, "host": "replica-b", "identity": "mysql-uuid:duplicate", "port": 3306},
        ]
    )
    tab = SimpleNamespace(
        dolphie=SimpleNamespace(
            get_hostname=lambda host: host,
            replica_manager=replica_manager,
        )
    )
    poll = MagicMock()
    monkeypatch.setattr(Replication, "_poll_replica", poll)

    fetch_replicas(cast(Tab, tab))

    poll.assert_called_once()
    assert replica_manager.active_count == 1


def test_errant_gtid_checks_are_throttled():
    connection = MagicMock()
    connection.last_execute_successful = True
    connection.fetchone.return_value = {"errant_trxs": "errant:1"}
    replica = Replica(
        identity="mysql-uuid:abc",
        row_key="replica-abc",
        host="replica",
        port=3306,
        connection=connection,
        replication_status={"Executed_Gtid_Set": "replica:1"},
    )
    tab = SimpleNamespace(
        dolphie=SimpleNamespace(
            server_uuid="primary",
            replication_source_uuids=set(),
            group_replication=False,
            innodb_cluster=False,
            global_variables={"gtid_executed": "primary:1"},
        )
    )

    _refresh_errant_transactions(cast(Tab, tab), replica, current_time=100)
    _refresh_errant_transactions(cast(Tab, tab), replica, current_time=101)

    connection.execute.assert_called_once()
    assert replica.errant_transactions == "errant:1"
    assert replica.next_errant_check_at == 130


def test_replica_errors_use_capped_exponential_backoff():
    replica = Replica(identity="mysql-uuid:abc", row_key="replica-abc", host="replica", port=3306)

    for attempt in range(1, 8):
        _record_replica_error(replica, "offline", current_time=100, refresh_interval=2)
        assert replica.consecutive_errors == attempt

    assert replica.next_poll_at == 160
    assert replica.last_error == "offline"


def test_replica_worker_errors_schedule_a_retry():
    app = MagicMock()
    tab = SimpleNamespace(
        id="tab-1",
        dolphie=SimpleNamespace(
            refresh_interval=3,
            host_with_port="db:3306",
        ),
    )
    app.tab_manager.get_tab.return_value = tab
    app.tab_manager.active_tab = tab
    event = SimpleNamespace(
        state=WorkerState.ERROR,
        worker=SimpleNamespace(
            name="tab-1",
            group="replicas",
            error=RuntimeError("poll failed"),
        ),
    )

    WorkerManager(app).on_worker_state_changed(cast(Worker.StateChanged, event))

    retry_delay, callback = app.set_timer.call_args.args
    assert retry_delay == 6
    assert callback.func == app.run_worker_replicas
    assert callback.args == ("tab-1",)
