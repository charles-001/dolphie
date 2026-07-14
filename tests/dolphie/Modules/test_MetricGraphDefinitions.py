from dataclasses import FrozenInstanceError

import pytest

from dolphie.Modules.MetricDefinitions import create_metric_instances
from dolphie.Modules.MetricGraphDefinitions import (
    GRAPH_TABS,
    GRAPH_TABS_BY_ID,
    GRAPHS_BY_ID,
    GraphAvailability,
    GraphRenderer,
    MetricKey,
    validate_graph_definitions,
)


def test_graph_registry_is_unique_resolvable_and_immutable() -> None:
    validate_graph_definitions()

    graph_ids = [graph.id for tab in GRAPH_TABS for graph in tab.graphs]
    assert len(graph_ids) == len(set(graph_ids))
    assert set(graph_ids) == set(GRAPHS_BY_ID)

    metric_key = MetricKey("dml", "Com_select")
    with pytest.raises(FrozenInstanceError):
        metric_key.__setattr__("metric", "Com_insert")


def test_registry_owns_merged_layouts_and_metric_groups_do_not() -> None:
    metrics = create_metric_instances()
    system = GRAPH_TABS_BY_ID["system"]
    ahi = GRAPH_TABS_BY_ID["adaptive_hash_index"]
    redo = GRAPH_TABS_BY_ID["redo_log"]

    assert [len(row.graphs) for row in system.rows] == [2, 2]
    assert [graph.metric_group for graph in ahi.graphs] == [
        "adaptive_hash_index",
        "adaptive_hash_index_hit_ratio",
    ]
    assert [graph.weight for graph in redo.graphs] == [33, 55, 12]
    assert redo.graphs[0].availability is GraphAvailability.ACTIVE_REDO_LOG
    assert redo.graphs[1].expanded_weight == 88
    assert redo.graphs[2].renderer is GraphRenderer.REDO_LOG_BAR

    assert not hasattr(metrics.dml, "graphs")
    assert not hasattr(metrics.dml, "tab_name")
    assert not hasattr(metrics.dml, "graph_tab_name")
