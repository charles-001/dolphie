from dolphie.Modules.MetricGraphDefinitions import GRAPH_TABS, GRAPHS_BY_ID, validate_graph_definitions


def test_graph_registry_is_unique_and_resolvable() -> None:
    validate_graph_definitions()

    graph_ids = [graph.id for tab in GRAPH_TABS for graph in tab.graphs]
    assert len(graph_ids) == len(set(graph_ids))
    assert set(graph_ids) == set(GRAPHS_BY_ID)
