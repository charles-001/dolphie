from __future__ import annotations

from rich.text import Text
from textual import events, on
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal
from textual.message import Message
from textual.widgets import Label, Switch

from dolphie.Modules.MetricDefinitions import Color
from dolphie.Modules.MetricGraphDefinitions import MetricKey, SwatchKind
from dolphie.Modules.Theme import DARK_GRAY, FOREGROUND, LIGHT_BLUE


class MetricSeriesControl(Horizontal):
    """Combine a graph-series identity, live value, and visibility switch."""

    can_focus = True
    BINDINGS = [
        Binding("enter", "toggle_series", "Toggle series", show=False),
        Binding("space", "toggle_series", "Toggle series", show=False),
    ]

    class VisibilityChanged(Message):
        """Report a typed metric visibility change."""

        def __init__(self, metric_key: MetricKey, visible: bool) -> None:
            super().__init__()
            self.metric_key = metric_key
            self.visible = visible

    def __init__(
        self,
        *,
        metric_key: MetricKey,
        label: str,
        color: Color,
        switchable: bool,
        visible: bool,
        swatch: SwatchKind,
    ) -> None:
        super().__init__(id=f"metric-control-{metric_key.dom_id}", classes="metric-series-control")
        self.metric_key = metric_key
        self.metric_label = label
        self.metric_color = color
        self.switchable = switchable
        self.series_visible = visible
        self.swatch = swatch
        self.formatted_value = "—"
        self.value_label = Label(self._render_label(), classes="metric-series-label")
        self.toggle = (
            Switch(
                animate=False,
                value=visible,
                id=f"metric-switch-{metric_key.dom_id}",
                classes="metric-series-switch",
            )
            if switchable
            else None
        )
        self.display = switchable or visible

    def compose(self) -> ComposeResult:
        """Compose the value label and optional series switch."""
        yield self.value_label
        if self.toggle is not None:
            yield self.toggle

    def update_metric(self, formatted_value: str, visible: bool) -> None:
        """Synchronize value, styling, switch, and visibility."""
        self.formatted_value = formatted_value
        self.series_visible = visible
        self.display = self.switchable or visible
        if self.toggle is not None:
            if self.toggle.value != visible:
                with self.prevent(Switch.Changed):
                    self.toggle.value = visible
        self.value_label.update(self._render_label())

    @on(Switch.Changed, ".metric-series-switch")
    def _switch_changed(self, event: Switch.Changed) -> None:
        """Translate the implementation switch into a typed control message."""
        event.stop()
        self.series_visible = event.value
        self.value_label.update(self._render_label())
        self.post_message(self.VisibilityChanged(self.metric_key, event.value))

    def on_click(self, event: events.Click) -> None:
        """Toggle the series when the control body is clicked."""
        if not self.switchable or self.toggle is None or event.widget is self.toggle:
            return
        self.focus()
        self.action_toggle_series()

    def action_toggle_series(self) -> None:
        """Toggle a switchable series from the keyboard or control body."""
        if self.switchable and self.toggle is not None:
            self.toggle.value = not self.toggle.value

    def _render_label(self) -> Text:
        """Build a compact colored series label."""
        red, green, blue = self.metric_color
        color = f"rgb({red},{green},{blue})"
        swatch_style = color if self.series_visible else f"dim {color}"
        text_style = LIGHT_BLUE if self.series_visible else DARK_GRAY
        value_style = f"bold {FOREGROUND}" if self.series_visible else DARK_GRAY

        label = Text()
        label.append("██" if self.swatch is SwatchKind.BAR else "━━", style=swatch_style)
        label.append(f" {self.metric_label} ", style=text_style)
        label.append(self.formatted_value, style=value_style)
        if not self.series_visible:
            label.append(" OFF", style="bold red")
        return label
