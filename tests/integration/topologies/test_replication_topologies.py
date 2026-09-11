"""Replication panel against real multi-node topologies: async replicas, Group Replication, Galera,
multi-source channels, and an InnoDB ClusterSet."""

from __future__ import annotations

from pathlib import Path

import pytest

from dolphie.DataTypes import ConnectionSource
from tests.integration.harness import DolphieHarness, make_config, run_dolphie
from tests.integration.servers import Topology

mariadb = pytest.mark.parametrize("topology", ["mariadb"], indirect=True)
gr = pytest.mark.parametrize("topology", ["gr"], indirect=True)
galera = pytest.mark.parametrize("topology", ["galera"], indirect=True)
multi_source = pytest.mark.parametrize("topology", ["multi-source"], indirect=True)
clusterset = pytest.mark.parametrize("topology", ["clusterset"], indirect=True)


async def open_replication_panel(harness: DolphieHarness) -> None:
    await harness.wait_for_polls(2)
    await harness.press("4")
    assert harness.dolphie.panels.replication.visible, harness.notifications
    assert harness.tab.panel_replication.display


async def assert_replica_channels(harness: DolphieHarness, count: int) -> None:
    await harness.wait_for(lambda: len(harness.dolphie.replication_status) == count, message=f"{count} channels")
    for status in harness.dolphie.replication_status:
        assert status.get("Replica_IO_Running", status.get("Slave_IO_Running")) == "Yes", status
    await open_replication_panel(harness)
    assert harness.tab.replication_container.display


@mariadb
async def test_mariadb_primary_discovers_and_polls_its_replicas(topology: Topology, tmp_path: Path) -> None:
    primary = topology.endpoints["primary"]

    async with run_dolphie(make_config(primary, tmp_path)) as harness:
        await harness.wait_for_polls(2)
        assert harness.dolphie.connection_source_alt == ConnectionSource.mariadb
        await harness.wait_for(lambda: harness.dolphie.replica_manager.discovery_count == 2, message="2 replicas")

        await open_replication_panel(harness)
        # The replicas worker connects to each replica through its report-host and report-port.
        await harness.wait_for(lambda: harness.dolphie.replica_manager.active_count == 2, message="2 replica polls")
        await harness.wait_for(
            lambda: not harness.tab.replicas_loading_indicator.display, message="replica tables to render"
        )
        assert harness.tab.replicas_container.display


@mariadb
async def test_mariadb_replica_shows_its_channel(topology: Topology, tmp_path: Path) -> None:
    replica = topology.endpoints["replica_1"]

    async with run_dolphie(make_config(replica, tmp_path)) as harness:
        await assert_replica_channels(harness, 1)
        assert harness.dolphie.global_variables.get("read_only") == "ON"


@gr
async def test_group_replication_members(topology: Topology, tmp_path: Path) -> None:
    node = topology.endpoints["node1"]

    async with run_dolphie(make_config(node, tmp_path)) as harness:
        await harness.wait_for_polls(2)
        assert harness.dolphie.group_replication
        assert not harness.dolphie.innodb_cluster
        await harness.wait_for(lambda: len(harness.dolphie.group_replication_members) == 3, message="3 members")
        assert {m.get("MEMBER_STATE") for m in harness.dolphie.group_replication_members} == {"ONLINE"}

        await open_replication_panel(harness)
        assert harness.tab.group_replication_container.display


@gr
async def test_async_replica_of_a_group(topology: Topology, tmp_path: Path) -> None:
    replica = topology.endpoints["async_replica"]

    async with run_dolphie(make_config(replica, tmp_path)) as harness:
        await assert_replica_channels(harness, 1)
        assert not harness.dolphie.group_replication


@galera
async def test_galera_cluster_nodes(topology: Topology, tmp_path: Path) -> None:
    node = topology.endpoints["node1"]

    async with run_dolphie(make_config(node, tmp_path)) as harness:
        await harness.wait_for_polls(2)
        assert harness.dolphie.galera_cluster
        await harness.wait_for(
            lambda: harness.dolphie.global_status.get("wsrep_cluster_size") == 3, message="3 galera nodes"
        )
        assert harness.dolphie.global_status.get("wsrep_ready") == "ON"
        await open_replication_panel(harness)


@galera
async def test_async_replica_of_a_galera_cluster(topology: Topology, tmp_path: Path) -> None:
    replica = topology.endpoints["async_replica"]

    async with run_dolphie(make_config(replica, tmp_path)) as harness:
        await assert_replica_channels(harness, 1)
        assert not harness.dolphie.galera_cluster


@multi_source
async def test_multi_source_replica_shows_every_channel(topology: Topology, tmp_path: Path) -> None:
    replica = topology.endpoints["replica"]

    async with run_dolphie(make_config(replica, tmp_path)) as harness:
        await assert_replica_channels(harness, 2)
        channels = {str(status.get("Channel_Name")) for status in harness.dolphie.replication_status}
        assert len(channels) == 2
        assert "" not in channels


@clusterset
async def test_clusterset_primary_and_replica_cluster(topology: Topology, tmp_path: Path) -> None:
    primary = topology.endpoints["primary_node1"]
    replica_primary = topology.endpoints["replica_node4"]

    async with run_dolphie(make_config(primary, tmp_path)) as harness:
        await harness.wait_for_polls(2)
        assert harness.dolphie.innodb_cluster
        assert not harness.dolphie.group_replication
        await harness.wait_for(lambda: len(harness.dolphie.group_replication_members) == 3, message="3 members")
        await open_replication_panel(harness)
        assert harness.tab.group_replication_container.display

    async with run_dolphie(make_config(replica_primary, tmp_path)) as harness:
        await harness.wait_for_polls(2)
        assert harness.dolphie.innodb_cluster
        # The replica cluster's primary pulls from the primary cluster over the clusterset channel.
        await harness.wait_for(
            lambda: any(
                status.get("Channel_Name") == "clusterset_replication" for status in harness.dolphie.replication_status
            ),
            message="clusterset_replication channel",
        )
        await open_replication_panel(harness)
