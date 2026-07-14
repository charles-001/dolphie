from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from typing import cast
from unittest.mock import patch

from textual.app import App, ComposeResult
from textual.widgets import Switch

from dolphie.DataTypes import ConnectionSource, Panels
from dolphie.Dolphie import Dolphie
from dolphie.Modules.MetricGraphDefinitions import GRAPH_TABS, GRAPHS_BY_ID, MetricKey
from dolphie.Modules.MetricManager import MetricManager
from dolphie.Modules.Theme import DOLPHIE_THEME
from dolphie.Widgets.MetricGraphDashboard import MetricControlsScroll, MetricGraphDashboard

CSS_PATH = Path(__file__).parents[3] / "dolphie" / "Dolphie.tcss"


class DashboardTestApp(App[None]):
    CSS_PATH = CSS_PATH

    def __init__(self) -> None:
        super().__init__()
        self.register_theme(DOLPHIE_THEME)
        self.theme = DOLPHIE_THEME.name
        self.dashboard = MetricGraphDashboard(marker="braille", id="dashboard")

    def compose(self) -> ComposeResult:
        yield self.dashboard
        yield Switch(value=True, id="unrelated-switch")


def make_host(
    *,
    connection_source: str = ConnectionSource.mysql,
    system_utilization: bool = True,
    active_redo: bool = False,
    replay: bool = False,
) -> Dolphie:
    panels = Panels()
    panels.metadata_locks.visible = True
    return cast(
        Dolphie,
        SimpleNamespace(
            metric_manager=MetricManager("replay.db" if replay else None),
            connection_source=connection_source,
            system_utilization={"CPU_Percent": 1} if system_utilization else {},
            global_variables={"innodb_adaptive_hash_index": "ON"},
            global_status={"Active_redo_log_count": 1} if active_redo else {},
            replication_status=[],
            metadata_locks_enabled=True,
            panels=panels,
            replay_file="replay.db" if replay else None,
        ),
    )


def test_dashboard_composes_registry_rows_controls_and_graphs() -> None:
    async def run_test() -> None:
        async with DashboardTestApp().run_test(size=(120, 50)) as pilot:
            dashboard = pilot.app.query_one(MetricGraphDashboard)
            await pilot.pause()

            assert set(dashboard.graphs) == set(GRAPHS_BY_ID)
            assert len(dashboard.controls) == len(set(dashboard.controls))
            assert len(dashboard.query(".metric-graph-row")) == sum(len(tab.rows) for tab in GRAPH_TABS)
            assert dashboard.query(".metric-control-group-label")
            assert dashboard.query(".metric-controls-overflow")

    asyncio.run(run_test())


def test_dashboard_binds_two_hosts_before_first_poll_without_visibility_leakage() -> None:
    async def run_test() -> None:
        first = make_host(system_utilization=False)
        second = make_host(system_utilization=False)
        first.metric_manager.metrics.dml.Com_select.visible = False

        async with DashboardTestApp().run_test(size=(120, 40)) as pilot:
            dashboard = pilot.app.query_one(MetricGraphDashboard)
            control = dashboard.controls[MetricKey("dml", "Com_select")]

            dashboard.bind_host(first)
            await pilot.pause()
            assert control.series_visible is False
            assert control.toggle is not None
            assert control.toggle.value is False

            dashboard.bind_host(second)
            await pilot.pause()
            assert control.series_visible is True
            assert control.toggle is not None
            assert control.toggle.value is True

    asyncio.run(run_test())


def test_dashboard_routes_typed_visibility_and_ignores_unrelated_switches() -> None:
    async def run_test() -> None:
        host = make_host(system_utilization=False)
        async with DashboardTestApp().run_test(size=(120, 40)) as pilot:
            dashboard = pilot.app.query_one(MetricGraphDashboard)
            dashboard.bind_host(host)
            dashboard.tabs.active = "graph-tab-dml"
            await pilot.pause()

            unrelated = pilot.app.query_one("#unrelated-switch", Switch)
            unrelated.value = False
            await pilot.pause()
            assert host.metric_manager.metrics.dml.Com_select.visible is True

            control = dashboard.controls[MetricKey("dml", "Com_select")]
            assert control.toggle is not None
            control.toggle.value = False
            await pilot.pause()
            assert host.metric_manager.metrics.dml.Com_select.visible is False

    asyncio.run(run_test())


def test_redo_availability_updates_widths_and_clears_stale_control() -> None:
    async def run_test() -> None:
        host = make_host(active_redo=True)
        async with DashboardTestApp().run_test(size=(120, 40)) as pilot:
            dashboard = pilot.app.query_one(MetricGraphDashboard)
            dashboard.bind_host(host)
            await pilot.pause()

            active_graph = dashboard.graphs["graph_redo_log_active_count"]
            data_graph = dashboard.graphs["graph_redo_log_data_written"]
            active_control = dashboard.controls[MetricKey("redo_log_active_count", "Active_redo_log_count")]
            assert active_graph.display
            assert str(data_graph.styles.width) == "55fr"
            assert active_control.series_visible

            host.global_status.clear()
            dashboard.refresh_active()
            await pilot.pause()
            assert not active_graph.display
            assert str(data_graph.styles.width) == "88fr"
            assert not active_control.display
            assert not dashboard.control_groups["graph_redo_log_active_count"].display
            assert host.metric_manager.metrics.redo_log_active_count.Active_redo_log_count.visible is False

    asyncio.run(run_test())


def test_replay_never_exposes_active_redo_graph() -> None:
    async def run_test() -> None:
        host = make_host(active_redo=True, replay=True)
        async with DashboardTestApp().run_test(size=(120, 40)) as pilot:
            dashboard = pilot.app.query_one(MetricGraphDashboard)
            dashboard.bind_host(host)
            await pilot.pause()

            assert not dashboard.graphs["graph_redo_log_active_count"].display
            assert str(dashboard.graphs["graph_redo_log_data_written"].styles.width) == "88fr"

    asyncio.run(run_test())


def test_control_strip_has_keyboard_horizontal_scrolling() -> None:
    controls = MetricControlsScroll()

    with patch.object(controls, "scroll_relative") as scroll_relative:
        controls.action_scroll_controls_left()
        controls.action_scroll_controls_right()

    assert controls.can_focus
    assert scroll_relative.call_args_list[0].kwargs == {"x": -12, "animate": False}
    assert scroll_relative.call_args_list[1].kwargs == {"x": 12, "animate": False}
