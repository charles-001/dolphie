from __future__ import annotations

import asyncio
from pathlib import Path
from typing import cast

from textual import on
from textual.app import App, ComposeResult
from textual.widgets import Switch

from dolphie.Modules.MetricDefinitions import MetricColor
from dolphie.Modules.MetricGraphDefinitions import MetricKey, SwatchKind
from dolphie.Modules.Theme import DOLPHIE_THEME
from dolphie.Widgets.MetricSeriesControl import MetricSeriesControl

CSS_PATH = Path(__file__).parents[3] / "dolphie" / "Dolphie.tcss"


class MetricControlTestApp(App[None]):
    CSS_PATH = CSS_PATH

    def __init__(self) -> None:
        super().__init__()
        self.visibility_messages: list[MetricSeriesControl.VisibilityChanged] = []
        self.register_theme(DOLPHIE_THEME)
        self.theme = DOLPHIE_THEME.name

    def compose(self) -> ComposeResult:
        yield MetricSeriesControl(
            metric_key=MetricKey("dml", "Com_select"),
            label="SELECT",
            color=MetricColor.blue,
            switchable=True,
            visible=True,
            swatch=SwatchKind.LINE,
        )

    @on(MetricSeriesControl.VisibilityChanged)
    def visibility_changed(self, event: MetricSeriesControl.VisibilityChanged) -> None:
        self.visibility_messages.append(event)


def test_metric_series_control_combines_identity_value_and_switch() -> None:
    async def run_test() -> None:
        async with MetricControlTestApp().run_test() as pilot:
            app = cast(MetricControlTestApp, pilot.app)
            control = pilot.app.query_one(MetricSeriesControl)
            control.update_metric("1.2K/s", visible=True)
            await pilot.pause()

            assert str(control.value_label.render()) == "━━ SELECT 1.2K/s"
            assert control.query_one(Switch).value is True
            assert control.tooltip is None
            assert control.query_one(Switch).tooltip is None

            control.update_metric("900/s", visible=False)
            await pilot.pause()
            assert str(control.value_label.render()) == "━━ SELECT 900/s OFF"
            assert control.query_one(Switch).value is False
            assert control.display
            assert app.visibility_messages == []

    asyncio.run(run_test())


def test_fixed_hidden_metric_control_is_not_displayed() -> None:
    control = MetricSeriesControl(
        metric_key=MetricKey("system_memory", "Memory_Total"),
        label="Total",
        color=MetricColor.blue,
        switchable=False,
        visible=False,
        swatch=SwatchKind.BAR,
    )

    assert not control.display
    assert str(control._render_label()) == "██ Total — OFF"


def test_control_body_emits_typed_visibility_message() -> None:
    async def run_test() -> None:
        async with MetricControlTestApp().run_test() as pilot:
            app = cast(MetricControlTestApp, pilot.app)
            await pilot.click("#metric-control-dml-Com_select")
            await pilot.pause()

            assert len(app.visibility_messages) == 1
            message = app.visibility_messages[0]
            assert message.metric_key == MetricKey("dml", "Com_select")
            assert message.visible is False

    asyncio.run(run_test())
