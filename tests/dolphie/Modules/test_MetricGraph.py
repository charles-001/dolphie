import asyncio
from unittest.mock import patch

from textual.app import App, ComposeResult
from textual_plotext import PlotextPlot

from dolphie.Modules.MetricDefinitions import create_metric_instances
from dolphie.Modules.MetricGraph import Graph, calculate_hourly_rate, downsample_series
from dolphie.Modules.MetricGraphDefinitions import GRAPHS_BY_ID, GraphSpec


class GraphTestApp(App[None]):
    def __init__(self, spec: GraphSpec) -> None:
        super().__init__()
        self.spec = spec

    def compose(self) -> ComposeResult:
        yield Graph(id="graph", spec=self.spec)


def test_hourly_rate_handles_empty_history() -> None:
    assert calculate_hourly_rate([], []) == 0


def test_downsample_series_bounds_work_and_preserves_extrema() -> None:
    x_values = [f"01/01/26 00:{index // 60:02}:{index % 60:02}" for index in range(600)]
    y_values = [0] * 600
    y_values[123] = 500
    y_values[456] = -200

    sampled_x, sampled_y = downsample_series(x_values, y_values, max_points=100)

    assert len(sampled_x) == len(sampled_y)
    assert len(sampled_x) <= 100
    assert sampled_x[0] == x_values[0]
    assert sampled_x[-1] == x_values[-1]
    assert max(sampled_y) == 500
    assert min(sampled_y) == -200


def test_graph_uses_isolated_plotext_figures() -> None:
    spec = GRAPHS_BY_ID["graph_dml"]
    first_graph = Graph(spec=spec)
    second_graph = Graph(spec=spec)

    assert isinstance(first_graph, PlotextPlot)
    assert first_graph.plt is not second_graph.plt
    assert first_graph.marker == "braille"


def test_graph_renders_metric_history_when_mounted() -> None:
    async def run_test() -> None:
        metrics = create_metric_instances()
        metrics.dml.Com_select.append_sample(10, "01/01/26 00:00:00", 1)
        metrics.dml.Com_select.append_sample(20, "01/01/26 00:00:01", 1)

        async with GraphTestApp(GRAPHS_BY_ID["graph_dml"]).run_test(
            size=(100, 30),
        ) as pilot:
            graph = pilot.app.query_one("#graph", Graph)
            graph.render_graph(metrics.dml)
            await pilot.pause()

            assert "SELECT" not in graph.render().plain

    asyncio.run(run_test())


def test_checkpoint_graph_renders_tick_margin_and_threshold_labels() -> None:
    async def run_test() -> None:
        metrics = create_metric_instances()
        metrics.checkpoint.checkpoint_age_sync_flush = 80
        metrics.checkpoint.checkpoint_age_max = 100
        metrics.checkpoint.Innodb_checkpoint_age.append_sample(50, "01/01/26 00:00:00", 1)
        metrics.checkpoint.Innodb_checkpoint_age.append_sample(60, "01/01/26 00:00:01", 1)

        async with GraphTestApp(GRAPHS_BY_ID["graph_checkpoint"]).run_test(
            size=(100, 30),
        ) as pilot:
            graph = pilot.app.query_one("#graph", Graph)
            graph.render_graph(metrics.checkpoint)
            await pilot.pause()

            rendered_text = graph.render().plain
            assert "Uncheckpointed" not in rendered_text
            assert "Warning" in rendered_text
            assert "Critical" in rendered_text

    asyncio.run(run_test())


def test_redo_log_bar_renders_threshold_when_mounted() -> None:
    async def run_test() -> None:
        metrics = create_metric_instances()
        metrics.redo_log.redo_log_size = 100
        metrics.redo_log.Innodb_lsn_current.append_sample(10, "01/01/26 00:00:00", 1)
        metrics.redo_log.Innodb_lsn_current.append_sample(20, "01/01/26 00:00:01", 1)

        async with GraphTestApp(GRAPHS_BY_ID["graph_redo_log_bar"]).run_test(
            size=(100, 30),
        ) as pilot:
            graph = pilot.app.query_one("#graph", Graph)
            graph.render_graph(metrics.redo_log)
            await pilot.pause()

            rendered_text = graph.render().plain
            assert "/hr" in rendered_text
            assert "Log Size" in rendered_text
            assert any(character in rendered_text for character in "█▀▄▚▟")

    asyncio.run(run_test())


def test_graph_reports_renderer_failures() -> None:
    async def run_test() -> None:
        metrics = create_metric_instances()
        async with GraphTestApp(GRAPHS_BY_ID["graph_dml"]).run_test(
            size=(100, 30),
        ) as pilot:
            graph = pilot.app.query_one("#graph", Graph)
            with (
                patch.object(graph, "_render_by_spec", side_effect=ValueError("bad replay data")),
                patch("dolphie.Modules.MetricGraph.logger.exception") as log_exception,
            ):
                graph.render_graph(metrics.dml)
                await pilot.pause()

            assert graph.render().plain == "Graph unavailable"
            log_exception.assert_called_once()

    asyncio.run(run_test())
