from __future__ import annotations

from typing import TYPE_CHECKING

from textual import on
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, HorizontalScroll
from textual.widget import Widget
from textual.widgets import Label, Static, TabbedContent, TabPane

from dolphie.Modules.MetricDefinitions import MetricData, MetricInstance, MetricInstances, create_metric_instances
from dolphie.Modules.MetricGraph import Graph, get_number_format_function
from dolphie.Modules.MetricGraphDefinitions import (
    GRAPH_TABS,
    GRAPH_TABS_BY_ID,
    GraphAvailability,
    GraphSpec,
    GraphTabSpec,
    MetricKey,
    TabAvailability,
    resolve_metric_data,
    swatch_for_metric,
)
from dolphie.Widgets.MetricSeriesControl import MetricSeriesControl

if TYPE_CHECKING:
    from dolphie.Dolphie import Dolphie


class MetricControlsScroll(HorizontalScroll):
    """Keyboard-scrollable control strip for dense metric tabs."""

    can_focus = True
    BINDINGS = [
        Binding("left", "scroll_controls_left", "Scroll metrics left", show=False),
        Binding("right", "scroll_controls_right", "Scroll metrics right", show=False),
    ]

    def action_scroll_controls_left(self) -> None:
        self.scroll_relative(x=-12, animate=False)

    def action_scroll_controls_right(self) -> None:
        self.scroll_relative(x=12, animate=False)


class MetricGraphDashboard(Widget):
    """Own the shared graph DOM and bind it to the active host's metric state."""

    def __init__(self, *, marker: str, id: str | None = None) -> None:
        super().__init__(id=id)
        self.marker = marker
        self.graphs: dict[str, Graph] = {}
        self.controls: dict[MetricKey, MetricSeriesControl] = {}
        self.control_groups: dict[str, Horizontal] = {}
        self._control_tabs: dict[MetricKey, str] = {}
        self._bound_dolphie: Dolphie | None = None
        # Change guards so per-tick availability syncs only touch the DOM on
        # actual transitions.
        self._shown_tabs: dict[str, bool] = {}
        self._applied_graph_states: dict[str, tuple[bool, int]] = {}
        # When availability hides the active tab we auto-switch away; remember
        # the displaced tab so it is restored once it's available again.
        self._displaced_tab_id: str | None = None
        self._auto_selected_tab_id: str | None = None
        self.tabs = TabbedContent(id="metric_graph_tabs")
        self._build_widget_registries()

    def _build_widget_registries(self) -> None:
        # A default catalog supplies each control's static presentation defaults.
        catalog = create_metric_instances()
        for tab_spec in GRAPH_TABS:
            for graph_spec, metric_keys in tab_spec.unique_series_by_graph:
                graph = Graph(
                    spec=graph_spec,
                    id=graph_spec.id,
                    classes="panel_data",
                )
                graph.marker = self.marker
                self.graphs[graph_spec.id] = graph

                for metric_key in metric_keys:
                    metric_data = self._resolve_metric_data(catalog, metric_key)
                    control = MetricSeriesControl(
                        metric_key=metric_key,
                        label=metric_data.label,
                        color=metric_data.color,
                        switchable=metric_data.create_switch,
                        visible=metric_data.visible,
                        swatch=swatch_for_metric(tab_spec, metric_key),
                    )
                    self.controls[metric_key] = control
                    self._control_tabs[metric_key] = tab_spec.id

    def compose(self) -> ComposeResult:
        with self.tabs:
            for tab_spec in GRAPH_TABS:
                with TabPane(
                    tab_spec.title,
                    id=self._pane_id(tab_spec.id),
                    name=tab_spec.id,
                ):
                    groups = self._control_groups(tab_spec)
                    yield Horizontal(
                        MetricControlsScroll(
                            *groups,
                            id=f"metric-controls-{tab_spec.id}",
                            classes="metric-series-controls",
                        ),
                        Static("⇆", classes="metric-controls-overflow"),
                        classes="metric-controls-row",
                    )
                    for row_index, row_spec in enumerate(tab_spec.rows):
                        yield Horizontal(
                            *(self.graphs[graph.id] for graph in row_spec.graphs),
                            id=f"metric-graph-row-{tab_spec.id}-{row_index}",
                            classes="metric-graph-row",
                        )

    def _control_groups(self, tab_spec: GraphTabSpec) -> list[Horizontal]:
        groups: list[Horizontal] = []
        show_group_labels = len(tab_spec.graphs) > 1
        for graph_spec, metric_keys in tab_spec.unique_series_by_graph:
            if not metric_keys:
                continue

            children: list[Widget] = []
            if show_group_labels and graph_spec.control_label:
                children.append(Label(graph_spec.control_label, classes="metric-control-group-label"))
            children.extend(self.controls[metric] for metric in metric_keys)
            group = Horizontal(*children, classes="metric-control-group")
            self.control_groups[graph_spec.id] = group
            groups.append(group)
        return groups

    @property
    def active_tab_id(self) -> str | None:
        active = self.tabs.active
        if active is None:
            return None
        prefix = "graph-tab-"
        return active[len(prefix) :] if active.startswith(prefix) else None

    def bind_host(self, dolphie: Dolphie, *, render: bool = True) -> None:
        """Atomically bind graph controls, availability, and content to one host."""
        host_changed = self._bound_dolphie is not dolphie
        self._bound_dolphie = dolphie
        if host_changed:
            # The new host's metric state must be re-synced even where the
            # DOM availability state is unchanged, and a pending tab restore
            # from the previous host no longer applies.
            self._applied_graph_states.clear()
            self._displaced_tab_id = None
            self._auto_selected_tab_id = None
        self._sync_availability()
        if host_changed:
            self.sync_controls()
        elif self.active_tab_id is not None:
            self.sync_controls(self.active_tab_id)
        if render:
            self.render_active()

    def refresh_active(self) -> None:
        """Refresh availability, controls, and graphs for the bound host."""
        if self._bound_dolphie is None:
            return
        self._sync_availability()
        active_tab_id = self.active_tab_id
        if active_tab_id is None:
            return
        self.sync_controls(active_tab_id)
        self.render_tab(active_tab_id)

    def render_active(self) -> None:
        """Render the selected graph tab for the bound host."""
        active_tab_id = self.active_tab_id
        if active_tab_id is not None:
            self.render_tab(active_tab_id)

    def render_tab(self, tab_id: str) -> None:
        """Render a registry graph tab without string-based widget routing."""
        dolphie = self._bound_dolphie
        tab_spec = GRAPH_TABS_BY_ID.get(tab_id)
        if dolphie is None or tab_spec is None:
            return

        for graph_spec in tab_spec.graphs:
            graph = self.graphs[graph_spec.id]
            if not graph.display:
                graph.render_graph(None)
                continue
            metric_instance = getattr(dolphie.metric_manager.metrics, graph_spec.metric_group)
            assert isinstance(metric_instance, MetricInstance)
            graph.render_graph(metric_instance)

    def sync_controls(self, tab_id: str | None = None) -> None:
        """Synchronize shared controls from the bound host without emitting events."""
        dolphie = self._bound_dolphie
        if dolphie is None:
            return

        for metric_key, control in self.controls.items():
            if tab_id is not None and self._control_tabs[metric_key] != tab_id:
                continue
            metric_instance = getattr(dolphie.metric_manager.metrics, metric_key.group)
            assert isinstance(metric_instance, MetricInstance)
            metric_data = self._resolve_metric_data(dolphie.metric_manager.metrics, metric_key)
            number_format = get_number_format_function(metric_instance)
            value = metric_data.latest_value()
            formatted_value = "—" if value is None else number_format(value)
            if value is not None and metric_data.per_second_calculation:
                formatted_value = f"{formatted_value}/s"
            control.update_metric(formatted_value, metric_data.visible)

    def _sync_availability(self) -> None:
        dolphie = self._bound_dolphie
        if dolphie is None:
            return

        available_tab_ids: list[str] = []
        for tab_spec in GRAPH_TABS:
            available = dolphie.connection_source in tab_spec.connection_sources and self._tab_available(
                tab_spec, dolphie
            )
            if available:
                available_tab_ids.append(tab_spec.id)
            if self._shown_tabs.get(tab_spec.id) != available:
                self._shown_tabs[tab_spec.id] = available
                if available:
                    self.tabs.show_tab(self._pane_id(tab_spec.id))
                else:
                    self.tabs.hide_tab(self._pane_id(tab_spec.id))

            for row_spec in tab_spec.rows:
                graph_availability = {graph.id: self._graph_available(graph, dolphie) for graph in row_spec.graphs}
                optional_graph_hidden = any(
                    graph.availability is not GraphAvailability.ALWAYS and not graph_availability[graph.id]
                    for graph in row_spec.graphs
                )
                for graph_spec in row_spec.graphs:
                    graph_available = available and graph_availability[graph_spec.id]
                    weight = (
                        graph_spec.expanded_weight
                        if optional_graph_hidden and graph_spec.expanded_weight is not None
                        else graph_spec.weight
                    )
                    if self._applied_graph_states.get(graph_spec.id) == (graph_available, weight):
                        continue
                    self._applied_graph_states[graph_spec.id] = (graph_available, weight)

                    graph = self.graphs[graph_spec.id]
                    graph.display = graph_available
                    control_group = self.control_groups.get(graph_spec.id)
                    if control_group is not None:
                        control_group.display = graph_available
                    graph.styles.width = f"{weight}fr"
                    if graph_spec.availability is not GraphAvailability.ALWAYS:
                        # Availability-gated series are not user-switchable, so their
                        # host visibility state follows graph availability.
                        metric_data = self._resolve_metric_data(
                            dolphie.metric_manager.metrics,
                            graph_spec.series[0],
                        )
                        metric_data.visible = graph_available
                        control = self.controls[graph_spec.series[0]]
                        control.update_metric(control.formatted_value, graph_available)

        if not available_tab_ids:
            return
        active_tab_id = self.active_tab_id
        if self._displaced_tab_id in available_tab_ids and active_tab_id == self._auto_selected_tab_id:
            displaced_tab_id = self._displaced_tab_id
            self._displaced_tab_id = None
            self._auto_selected_tab_id = None
            self.tabs.active = self._pane_id(displaced_tab_id)
        elif active_tab_id not in available_tab_ids:
            if self._displaced_tab_id is None:
                self._displaced_tab_id = active_tab_id
            self._auto_selected_tab_id = available_tab_ids[0]
            self.tabs.active = self._pane_id(available_tab_ids[0])

    @staticmethod
    def _tab_available(tab_spec: GraphTabSpec, dolphie: Dolphie) -> bool:
        availability = tab_spec.availability
        if availability is TabAvailability.ALWAYS:
            return True
        if availability is TabAvailability.SYSTEM_UTILIZATION:
            return bool(dolphie.system_utilization)
        if availability is TabAvailability.ADAPTIVE_HASH_INDEX:
            return dolphie.global_variables.get("innodb_adaptive_hash_index") != "OFF"
        if availability is TabAvailability.REPLICATION:
            return bool(dolphie.replication_status)
        if availability is TabAvailability.LOCKS:
            return bool(
                (dolphie.metadata_locks_enabled and dolphie.panels.metadata_locks.visible) or dolphie.replay_file
            )
        return False

    @staticmethod
    def _graph_available(graph_spec: GraphSpec, dolphie: Dolphie) -> bool:
        if graph_spec.availability is GraphAvailability.ALWAYS:
            return True
        if graph_spec.availability is GraphAvailability.ACTIVE_REDO_LOG:
            return "Active_redo_log_count" in dolphie.global_status and not dolphie.replay_file
        return False

    @on(MetricSeriesControl.VisibilityChanged)
    def _visibility_changed(self, event: MetricSeriesControl.VisibilityChanged) -> None:
        """Apply a typed visibility event only to the currently bound host."""
        dolphie = self._bound_dolphie
        if dolphie is None:
            return
        metric_data = self._resolve_metric_data(dolphie.metric_manager.metrics, event.metric_key)
        metric_data.visible = event.visible
        self.sync_controls(self._control_tabs[event.metric_key])
        self.render_tab(self._control_tabs[event.metric_key])

    @on(TabbedContent.TabActivated, "#metric_graph_tabs")
    def _tab_activated(self, event: TabbedContent.TabActivated) -> None:
        """Render newly selected graph content from the current host."""
        tab_id = event.pane.name
        if tab_id is not None:
            if tab_id != self._auto_selected_tab_id:
                # A manual selection supersedes any pending auto-switch restore.
                self._displaced_tab_id = None
                self._auto_selected_tab_id = None
            self.sync_controls(tab_id)
            self.render_tab(tab_id)

    @staticmethod
    def _resolve_metric_data(metrics: MetricInstances, metric_key: MetricKey) -> MetricData:
        return resolve_metric_data(getattr(metrics, metric_key.group), metric_key)

    @staticmethod
    def _pane_id(tab_id: str) -> str:
        return f"graph-tab-{tab_id}"
