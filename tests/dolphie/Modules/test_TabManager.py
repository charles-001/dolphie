from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock

from dolphie.DataTypes import ConnectionStatus
from dolphie.Dolphie import Dolphie
from dolphie.Modules.MetricManager import MetricManager
from dolphie.Modules.TabManager import Tab, TabManager


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
    manager.tabs = {"host-1": tab}
    manager.active_tab = None
    manager.update_topbar = Mock()

    manager.switch_tab("host-1", set_active=False)

    assert manager.active_tab is tab
    assert tab.main_container.display is False
    dashboard.bind_host.assert_called_once_with(dolphie, render=False)


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
