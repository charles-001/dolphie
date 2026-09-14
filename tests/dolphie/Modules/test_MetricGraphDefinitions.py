from dataclasses import replace

import pytest

from dolphie.Modules import MetricGraphDefinitions
from dolphie.Modules.MetricGraphDefinitions import GRAPH_TABS, GRAPHS_BY_ID, validate_graph_definitions


def test_graph_registry_is_unique_and_resolvable() -> None:
    validate_graph_definitions()

    graph_ids = [graph.id for tab in GRAPH_TABS for graph in tab.graphs]
    assert len(graph_ids) == len(set(graph_ids))
    assert set(graph_ids) == set(GRAPHS_BY_ID)


def test_a_graph_sharing_a_tab_must_carry_a_title(monkeypatch: pytest.MonkeyPatch) -> None:
    workload = next(tab for tab in GRAPH_TABS if tab.id == "workload")
    first_row = workload.rows[0]
    untitled = replace(first_row, graphs=(replace(first_row.graphs[0], title=None), *first_row.graphs[1:]))
    monkeypatch.setattr(MetricGraphDefinitions, "GRAPH_TABS", (replace(workload, rows=(untitled, *workload.rows[1:])),))

    with pytest.raises(ValueError, match="graph_connections shares tab workload"):
        validate_graph_definitions()
