from rich.text import Text
from textual.app import App, ComposeResult
from textual.coordinate import Coordinate

from dolphie.Modules.Theme import (
    DOLPHIE_THEME,
    HIGHLIGHT,
    LABEL,
    ThemedDataTable,
    ThemedTable,
    resolve_theme_markup,
    themed_content,
)


def test_resolve_theme_markup_for_rich_renderables():
    assert resolve_theme_markup("[$b_highlight]value[/$b_highlight]") == f"[bold {HIGHLIGHT}]value[/]"
    assert resolve_theme_markup("[b $white on #B30000]value[/]") == "[b #e9e9e9 on #B30000]value[/]"


def test_textual_content_preserves_theme_references():
    content = themed_content("db.example:[$dark_gray]3306")

    assert content.spans[0].style == "$dark_gray"


def test_themed_table_resolves_textual_variables_before_rich_renders():
    table = ThemedTable("[$label]Column")
    table.add_row("[$b_highlight]Value[/$b_highlight]")

    header = table.columns[0].header
    cell = next(iter(table.columns[0].cells))
    assert isinstance(header, Text)
    assert str(header.spans[0].style) == LABEL
    assert isinstance(cell, Text)
    assert str(cell.spans[0].style) == f"bold {HIGHLIGHT}"


async def test_themed_data_table_resolves_textual_variables_for_rich_cells():
    class TableApp(App):
        def __init__(self):
            super().__init__()
            self.register_theme(DOLPHIE_THEME)
            self.theme = DOLPHIE_THEME.name

        def compose(self) -> ComposeResult:
            yield ThemedDataTable()

    app = TableApp()
    async with app.run_test() as pilot:
        table = app.query_one(ThemedDataTable)
        table.add_column("[$label]Column")
        table.add_row("[$b_highlight]Value[/$b_highlight]")
        await pilot.pause()

        cell = table.get_cell_at(Coordinate(0, 0))
        assert isinstance(cell, Text)
        assert str(cell.spans[0].style) == f"bold {HIGHLIGHT}"
        assert table.normalize_cells(["[$b_highlight]Value[/$b_highlight]"])[0] == cell
