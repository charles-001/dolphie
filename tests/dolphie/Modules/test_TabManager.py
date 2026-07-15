from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock

from dolphie.App import DolphieApp
from dolphie.DataTypes import ConnectionSource, ConnectionStatus
from dolphie.Dolphie import Dolphie
from dolphie.Modules.MetricManager import MetricManager
from dolphie.Modules.TabManager import Tab, TabManager
from dolphie.Panels import Replication as ReplicationPanel


def test_metric_reset_preserves_per_host_visibility() -> None:
    first_manager = MetricManager(None)
    second_manager = MetricManager(None)
    first_manager.metrics.dml.Com_select.visible = False

    first_manager.reset()

    assert first_manager.metrics.dml.Com_select.visible is False
    assert second_manager.metrics.dml.Com_select.visible is True


def test_switch_tab_binds_dashboard_before_first_worker_poll() -> None:
    dashboard = SimpleNamespace(bind_host=Mock())
    dolphie = SimpleNamespace(main_db_connection=SimpleNamespace(is_connected=lambda: False))
    sync_replication_ui = Mock()
    tab = cast(
        Tab,
        SimpleNamespace(
            id="host-1",
            dolphie=dolphie,
            main_container=SimpleNamespace(display=True),
            panel_graphs=SimpleNamespace(display=False),
            graph_dashboard=dashboard,
        ),
    )
    manager = cast(TabManager, object.__new__(TabManager))
    manager.app = cast(DolphieApp, SimpleNamespace(sync_replication_ui=sync_replication_ui))
    manager.tabs = {"host-1": tab}
    manager.active_tab = None
    manager.update_topbar = Mock()

    manager.switch_tab("host-1", set_active=False)

    assert manager.active_tab is tab
    assert tab.main_container.display is False
    dashboard.bind_host.assert_called_once_with(dolphie, render=False)
    sync_replication_ui.assert_called_once_with(tab)


def test_switch_tab_removes_inactive_replication_widgets() -> None:
    inactive_tab = SimpleNamespace(remove_replication_panel_components=Mock())
    dashboard = SimpleNamespace(bind_host=Mock())
    dolphie = SimpleNamespace(main_db_connection=SimpleNamespace(is_connected=lambda: False))
    active_tab = cast(
        Tab,
        SimpleNamespace(
            id="host-2",
            dolphie=dolphie,
            main_container=SimpleNamespace(display=True),
            panel_graphs=SimpleNamespace(display=False),
            graph_dashboard=dashboard,
        ),
    )
    manager = cast(TabManager, object.__new__(TabManager))
    manager.app = cast(DolphieApp, SimpleNamespace(sync_replication_ui=Mock()))
    manager.tabs = {"host-1": cast(Tab, inactive_tab), "host-2": active_tab}
    manager.active_tab = cast(Tab, inactive_tab)
    manager.update_topbar = Mock()

    manager.switch_tab("host-2", set_active=False)

    inactive_tab.remove_replication_panel_components.assert_called_once_with()
    assert manager.active_tab is active_tab


def test_sync_replication_ui_renders_selected_hosts_cached_snapshot(monkeypatch) -> None:
    create_panel = Mock()
    create_replica_panel = Mock()
    monkeypatch.setattr(ReplicationPanel, "create_panel", create_panel)
    monkeypatch.setattr(ReplicationPanel, "create_replica_panel", create_replica_panel)

    toggle_replication_panel_components = Mock()
    tab = cast(
        Tab,
        SimpleNamespace(
            dolphie=SimpleNamespace(
                connection_source=ConnectionSource.mysql,
                panels=SimpleNamespace(replication=SimpleNamespace(visible=True)),
            ),
            toggle_replication_panel_components=toggle_replication_panel_components,
        ),
    )

    app = cast(DolphieApp, object.__new__(DolphieApp))
    app.sync_replication_ui(tab)

    create_panel.assert_called_once_with(tab)
    create_replica_panel.assert_called_once_with(tab)
    toggle_replication_panel_components.assert_called_once_with()


def test_daemon_disconnect_skips_uninitialized_ui_references() -> None:
    class Connection:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    class ReplicaManager:
        def __init__(self) -> None:
            self.removed = False

        def remove_all_replicas(self) -> None:
            self.removed = True

    async def run_test() -> None:
        main_connection = Connection()
        secondary_connection = Connection()
        replica_manager = ReplicaManager()
        dolphie = cast(
            Dolphie,
            SimpleNamespace(
                daemon_mode=True,
                main_db_connection=main_connection,
                secondary_db_connection=secondary_connection,
                replica_manager=replica_manager,
                connection_status=None,
            ),
        )
        tab = Tab(id="daemon", name="daemon", dolphie=dolphie)
        manager = cast(TabManager, object.__new__(TabManager))
        manager.active_tab = tab

        await manager.disconnect_tab(tab, update_topbar=False, wait_for_workers=False)

        assert main_connection.closed
        assert secondary_connection.closed
        assert replica_manager.removed
        assert dolphie.connection_status == ConnectionStatus.disconnected

    asyncio.run(run_test())
