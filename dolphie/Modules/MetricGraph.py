from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import cast

from loguru import logger
from rich.text import Text
from textual.color import Color as TextualColor
from textual_plotext import PlotextPlot

from dolphie.Modules.Functions import format_bytes, format_number, format_time
from dolphie.Modules.MetricDefinitions import (
    CheckpointMetrics,
    MetricColor,
    MetricInstance,
    RedoLogActiveCountMetrics,
    RedoLogMetrics,
    SystemMemoryMetrics,
    ValueFormat,
)
from dolphie.Modules.MetricGraphDefinitions import GraphRenderer, GraphSpec, resolve_metric_data
from dolphie.Modules.Theme import BACKGROUND, FOREGROUND

_REDO_LOG_ACTIVE_MAX = 34
_SECONDS_PER_HOUR = 3600
_MIN_RENDER_POINTS = 100
_BACKGROUND_COLOR = TextualColor.parse(BACKGROUND).rgb
_TICK_COLOR = (133, 159, 213)
_TEXT_COLOR = TextualColor.parse(FOREGROUND).rgb
_WARNING_COLOR = (241, 251, 130)
_CRITICAL_COLOR = MetricColor.orange
_BAR_COLOR = (46, 124, 175)


def _plotext_x(value: str) -> float:
    """Bridge plotext's numeric-only stub for its supported date-string X values."""
    return cast(float, value)


def calculate_hourly_rate(values: list[int | float], polling_intervals: list[float]) -> int:
    """Extrapolate an hourly total from interval-weighted rate samples."""
    if not values:
        return 0

    weighted_samples = [
        (value, polling_intervals[index])
        for index, value in enumerate(values)
        if index < len(polling_intervals) and polling_intervals[index] > 0
    ]
    if weighted_samples:
        observed_seconds = sum(interval for _, interval in weighted_samples)
        average_rate = sum(value * interval for value, interval in weighted_samples) / observed_seconds
    else:
        average_rate = sum(values) / len(values)
    return round(average_rate * _SECONDS_PER_HOUR)


def downsample_series(
    x_values: Sequence[str],
    y_values: Sequence[int | float],
    max_points: int,
) -> tuple[list[str], list[int | float]]:
    """Reduce a series while retaining endpoint and bucket extrema."""
    point_count = min(len(x_values), len(y_values))
    if point_count == 0:
        return [], []
    if point_count <= max_points or max_points < 4:
        return list(x_values[-point_count:]), list(y_values[-point_count:])

    bucket_count = max((max_points - 2) // 2, 1)
    interior_count = point_count - 2
    selected_indices = [0]
    for bucket in range(bucket_count):
        start = 1 + (bucket * interior_count // bucket_count)
        end = 1 + ((bucket + 1) * interior_count // bucket_count)
        if start >= end:
            continue

        bucket_indices = range(start, end)
        minimum_index = min(bucket_indices, key=y_values.__getitem__)
        maximum_index = max(bucket_indices, key=y_values.__getitem__)
        selected_indices.extend(sorted({minimum_index, maximum_index}))

    selected_indices.append(point_count - 1)
    return (
        [x_values[index] for index in selected_indices],
        [y_values[index] for index in selected_indices],
    )


class Graph(PlotextPlot):
    """Render Dolphie's original plotext graphs with isolated widget state."""

    def __init__(
        self,
        *,
        name: str | None = None,
        id: str | None = None,
        classes: str | None = None,
        disabled: bool = False,
        spec: GraphSpec,
    ) -> None:
        """Initialize the Graph widget."""
        super().__init__(name=name, id=id, classes=classes, disabled=disabled)
        self.spec = spec
        self.marker = "braille"
        self.metric_instance: MetricInstance | None = None
        self._rendered_plot = Text()
        self._render_error: str | None = None

    def render(self) -> Text:
        """Build the isolated plotext figure at the current widget size."""
        if self._render_error is not None:
            return Text(self._render_error, style="bold red", justify="center")

        self.plt.plotsize(self.size.width, self.size.height)
        self.plt._set_size(self.size.width, self.size.height)
        try:
            self._rendered_plot = Text.from_ansi(self.plt.build())
        except OSError:
            logger.exception("Failed to build graph {}", self.spec.id)
            self._render_error = "Graph unavailable"
            return Text(self._render_error, style="bold red", justify="center")
        return self._rendered_plot

    def _setup_plot(self) -> None:
        """Clear and configure a figure with Dolphie's original styling."""
        self.plt.clf()
        self.plt.date_form("d/m/y H:M:S")
        self.plt.canvas_color(_BACKGROUND_COLOR)
        self.plt.axes_color(_BACKGROUND_COLOR)
        self.plt.ticks_color(_TICK_COLOR)

    def _finalize_plot(self, metric_instance: MetricInstance, max_y_value: int | float) -> None:
        """Apply the original five-interval Y axis and schedule a repaint."""
        max_y_ticks = 5
        y_tick_interval = max_y_value / max_y_ticks
        if y_tick_interval >= 1:
            y_ticks = [index * y_tick_interval for index in range(max_y_ticks + 1)]
        else:
            y_ticks = [float(index) for index in range(int(max_y_value) + 2)]

        format_function = get_number_format_function(metric_instance)
        self.plt.yticks(y_ticks, [format_function(value) for value in y_ticks])
        self.refresh()

    def _prepare_series(
        self,
        x_values: list[str],
        y_values: list[int | float],
    ) -> tuple[list[str], list[int | float]]:
        """Bound plotting work while retaining endpoints and local extrema."""
        max_points = max(self.size.width * 2, _MIN_RENDER_POINTS)
        return downsample_series(x_values, y_values, max_points)

    def _render_checkpoint_metrics(
        self,
        metric_instance: CheckpointMetrics,
        x: list[str],
        y: list[int | float],
    ) -> int | float:
        """Render checkpoint age using the original threshold treatment."""
        self.plt.hline(0, _BACKGROUND_COLOR)
        self.plt.hline(metric_instance.checkpoint_age_sync_flush, _WARNING_COLOR)
        self.plt.hline(metric_instance.checkpoint_age_max, _CRITICAL_COLOR)

        # x is chronological; max() would compare day-first date strings lexicographically
        max_x = _plotext_x(x[-1])
        self.plt.text(
            "Critical",
            y=metric_instance.checkpoint_age_max,
            x=max_x,
            alignment="right",
            color=_TEXT_COLOR,
            style="bold",
        )
        self.plt.text(
            "Warning",
            y=metric_instance.checkpoint_age_sync_flush,
            x=max_x,
            alignment="right",
            color=_TEXT_COLOR,
            style="bold",
        )

        metric = metric_instance.Innodb_checkpoint_age
        self.plt.plot(x, y, marker=self.marker, color=metric.color)
        return metric_instance.checkpoint_age_max

    def _render_redo_log_bar_metrics(
        self,
        metric_instance: RedoLogMetrics,
        y_values: list[int | float],
        polling_intervals: list[float],
    ) -> int | float:
        """Render redo generation as the original filled plotext bar."""
        hourly_rate = calculate_hourly_rate(y_values, polling_intervals)
        log_size = metric_instance.redo_log_size
        x = [0]
        y = [hourly_rate]

        self.plt.hline(log_size, _CRITICAL_COLOR)
        self.plt.text(
            "Log Size",
            y=log_size,
            x=0,
            alignment="center",
            color=_TEXT_COLOR,
            style="bold",
        )

        bar_color = _CRITICAL_COLOR if log_size > 0 and hourly_rate >= log_size else _BAR_COLOR
        self.plt.text(
            f"{format_bytes(hourly_rate, color=False)}/hr",
            y=hourly_rate,
            x=0,
            alignment="center",
            color=_TEXT_COLOR,
            style="bold",
            background=bar_color,
        )
        self.plt.bar(x, y, marker="hd", color=bar_color)
        return max(log_size, hourly_rate)

    def _render_redo_log_line_metrics(
        self,
        metric_instance: RedoLogMetrics,
        x: list[str],
        y: list[int | float],
    ) -> int | float:
        """Render the line graph for RedoLogMetrics."""
        metric = metric_instance.Innodb_lsn_current
        self.plt.plot(x, y, marker=self.marker, color=metric.color)
        return max(y, default=0)

    def _render_active_redo_log_metrics(
        self,
        metric_instance: RedoLogActiveCountMetrics,
        x: list[str],
        y: list[int | float],
    ) -> int:
        """Render active redo logs with the original fixed maximum."""
        self.plt.hline(1, _BACKGROUND_COLOR)
        self.plt.hline(_REDO_LOG_ACTIVE_MAX, _CRITICAL_COLOR)
        self.plt.text(
            "Max Count",
            y=_REDO_LOG_ACTIVE_MAX,
            x=_plotext_x(x[-1]),
            alignment="right",
            color=_TEXT_COLOR,
            style="bold",
        )

        metric = metric_instance.Active_redo_log_count
        self.plt.plot(x, y, marker=self.marker, color=metric.color)
        return _REDO_LOG_ACTIVE_MAX

    def _render_system_memory_metrics(
        self,
        metric_instance: SystemMemoryMetrics,
        x: list[str],
        y: list[int | float],
    ) -> int | float:
        """Render the graph for SystemMemoryMetrics."""
        total_mem = metric_instance.Memory_Total.last_value or 0
        self.plt.hline(0, _BACKGROUND_COLOR)
        self.plt.hline(total_mem, _CRITICAL_COLOR)
        self.plt.text(
            "Total",
            y=total_mem,
            x=_plotext_x(x[-1]),
            alignment="right",
            color=_TEXT_COLOR,
            style="bold",
        )

        metric = metric_instance.Memory_Used
        self.plt.plot(x, y, marker=self.marker, color=metric.color)
        return total_mem

    def _render_default_metrics(self, metric_instance: MetricInstance) -> int | float:
        """Render a graph for any standard metric instance."""
        max_y: int | float = 0
        for metric_key in self.spec.series:
            metric_data = resolve_metric_data(metric_instance, metric_key)
            if metric_data.visible:
                x, y, _ = metric_data.snapshot()
                if y and x:
                    x, y = self._prepare_series(x, y)
                    if x and y:
                        self.plt.plot(x, y, marker=self.marker, color=metric_data.color)
                        max_y = max(max_y, max(y))
        return max_y

    def _render_by_spec(self, metric_instance: MetricInstance) -> int | float:
        """Dispatch to the renderer explicitly declared by the graph specification."""
        renderer = self.spec.renderer
        if renderer is GraphRenderer.LINE:
            return self._render_default_metrics(metric_instance)

        metric_data = resolve_metric_data(metric_instance, self.spec.series[0])
        x, y, intervals = metric_data.snapshot()
        if renderer is GraphRenderer.REDO_LOG_BAR:
            if not y:
                return 0
            assert isinstance(metric_instance, RedoLogMetrics)
            return self._render_redo_log_bar_metrics(metric_instance, y, intervals)

        x, y = self._prepare_series(x, y)
        if not x or not y:
            return 0
        if renderer is GraphRenderer.CHECKPOINT:
            assert isinstance(metric_instance, CheckpointMetrics)
            return self._render_checkpoint_metrics(metric_instance, x, y)
        if renderer is GraphRenderer.REDO_LOG_LINE:
            assert isinstance(metric_instance, RedoLogMetrics)
            return self._render_redo_log_line_metrics(metric_instance, x, y)
        if renderer is GraphRenderer.ACTIVE_REDO_LOG:
            assert isinstance(metric_instance, RedoLogActiveCountMetrics)
            return self._render_active_redo_log_metrics(metric_instance, x, y)
        if renderer is GraphRenderer.SYSTEM_MEMORY:
            assert isinstance(metric_instance, SystemMemoryMetrics)
            return self._render_system_memory_metrics(metric_instance, x, y)
        raise ValueError(f"Unsupported graph renderer: {renderer.value}")

    def render_graph(self, metric_instance: MetricInstance | None) -> None:
        """Render a graph for the given metric instance.

        Args:
            metric_instance: The metric dataclass instance to plot.
        """
        self.metric_instance = metric_instance

        if metric_instance is None:
            self.plt.clf()
            self._rendered_plot = Text()
            self._render_error = None
            self.refresh()
            return

        self._setup_plot()
        self._render_error = None

        try:
            max_y_value = self._render_by_spec(metric_instance)
            self._finalize_plot(metric_instance, max_y_value)
        except (AttributeError, IndexError, TypeError, ValueError):
            logger.exception(
                "Failed to render graph {} for metric group {}",
                self.spec.id,
                self.spec.metric_group,
            )
            self.plt.clf()
            self._render_error = "Graph unavailable"
            self.refresh()


def get_number_format_function(data: MetricInstance, color: bool = False) -> Callable[[int | float], str]:
    """Return the formatting function declared by the metric group."""
    value_format = type(data).value_format
    if value_format is ValueFormat.TIME:
        return lambda val: format_time(val)
    if value_format is ValueFormat.BYTES:
        return lambda val: format_bytes(val, color=color)
    if value_format is ValueFormat.PERCENT:
        return lambda val: f"{round(val)}%"
    return lambda val: format_number(val, color=color)
