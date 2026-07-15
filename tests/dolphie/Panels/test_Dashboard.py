from dolphie.Panels.Dashboard import _format_utilization_percent


def test_utilization_percent_uses_textual_theme_colors():
    assert _format_utilization_percent(50) == "[$green]50%[/$green]"
    assert _format_utilization_percent(85) == "[$yellow]85%[/$yellow]"
    assert _format_utilization_percent(95) == "[$red]95%[/$red]"
