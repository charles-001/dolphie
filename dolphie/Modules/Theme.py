from __future__ import annotations

# Rich and Textual deliberately expose flexible renderable APIs through Any.
# ruff: noqa: ANN401
import re
from collections.abc import Iterable
from functools import lru_cache
from typing import Any

from rich.table import Column
from rich.table import Table as RichTable
from rich.text import Text
from textual.content import Content
from textual.theme import Theme
from textual.widgets import DataTable as TextualDataTable

BACKGROUND = "#0a0e1b"
DARK_GRAY = "#969aad"
DARK_YELLOW = "#e6d733"
FOREGROUND = "#e9e9e9"
GREEN = "#54efae"
HIGHLIGHT = "#91abec"
LABEL = "#c5c7d2"
LIGHT_BLUE = "#bbc8e8"
PANEL = "#192036"
PANEL_BORDER = "#6171a6"
PURPLE = "#b565f3"
RECORDING = "#ff5e5e"
RED = "#fd8383"
RED_BOLD = "#fb9a9a"
SURFACE = "#0f1525"
TABLE_BORDER = "#333f62"
YELLOW = "#f6ff8f"

THEME_VARIABLES = {
    "white": FOREGROUND,
    "green": GREEN,
    "yellow": YELLOW,
    "dark_yellow": DARK_YELLOW,
    "red": RED,
    "red2": f"bold {RED_BOLD}",
    "purple": PURPLE,
    "dark_gray": DARK_GRAY,
    "b_dark_gray": f"bold {DARK_GRAY}",
    "highlight": HIGHLIGHT,
    "label": LABEL,
    "b_label": f"bold {LABEL}",
    "light_blue": LIGHT_BLUE,
    "b_white": f"bold {FOREGROUND}",
    "b_highlight": f"bold {HIGHLIGHT}",
    "b_light_blue": f"bold {LIGHT_BLUE}",
    "recording": RECORDING,
    "b_recording": f"bold {RECORDING}",
    "panel_border": PANEL_BORDER,
    "table_border": TABLE_BORDER,
}

DOLPHIE_THEME = Theme(
    name="dolphie",
    primary=FOREGROUND,
    secondary=LIGHT_BLUE,
    accent=PURPLE,
    foreground=FOREGROUND,
    background=BACKGROUND,
    surface=SURFACE,
    panel=PANEL,
    success=GREEN,
    warning=YELLOW,
    error=RED,
    dark=True,
    variables=THEME_VARIABLES,
)

_MARKUP_TAG = re.compile(r"\[([^\]]+)\]")
_THEME_REFERENCE = re.compile(r"\$([a-zA-Z_][\w-]*)")
_THEME_TAG = re.compile(r"\[[^\]]*\$[a-zA-Z_][\w-]*[^\]]*\]")


@lru_cache(maxsize=2048)
def resolve_theme_markup(markup: str) -> str:
    """Resolve Textual theme references for renderables parsed directly by Rich."""

    def replace_tag(match: re.Match[str]) -> str:
        tag = match.group(1)
        if tag.startswith("/$"):
            return "[/]"

        def replace_reference(reference: re.Match[str]) -> str:
            name = reference.group(1)
            return THEME_VARIABLES.get(name, reference.group(0))

        return f"[{_THEME_REFERENCE.sub(replace_reference, tag)}]"

    return _MARKUP_TAG.sub(replace_tag, markup)


def themed_content(markup: str) -> Content:
    """Create Textual content while preserving dynamic theme references."""
    return Content.from_markup(markup)


def themed_text(markup: str) -> Text:
    """Create Rich text with Textual theme references resolved to concrete styles."""
    return Text.from_markup(resolve_theme_markup(markup))


def _themed_renderable(renderable: Any) -> Any:
    if isinstance(renderable, str) and _THEME_TAG.search(renderable):
        return themed_text(renderable)
    return renderable


def _themed_style(style: Any) -> Any:
    if not isinstance(style, str):
        return style
    return THEME_VARIABLES.get(style.removeprefix("$"), style)


class ThemedTable(RichTable):
    """Rich table that resolves colors from Dolphie's Textual theme."""

    def __init__(self, *headers: Any, **kwargs: Any) -> None:
        for key in ("title", "caption"):
            if key in kwargs:
                kwargs[key] = _themed_renderable(kwargs[key])
        for key in (
            "style",
            "header_style",
            "footer_style",
            "border_style",
            "title_style",
            "caption_style",
        ):
            if key in kwargs:
                kwargs[key] = _themed_style(kwargs[key])

        resolved_headers = []
        for header in headers:
            themed_header = _themed_renderable(header)
            resolved_headers.append(Column(header=themed_header) if isinstance(themed_header, Text) else themed_header)
        super().__init__(*resolved_headers, **kwargs)

    def add_column(self, header: Any = "", footer: Any = "", **kwargs: Any) -> None:
        for key in ("header_style", "footer_style", "style"):
            if key in kwargs:
                kwargs[key] = _themed_style(kwargs[key])
        super().add_column(
            _themed_renderable(header),
            _themed_renderable(footer),
            **kwargs,
        )

    def add_row(self, *renderables: Any, style: Any = None, end_section: bool = False) -> None:
        super().add_row(
            *(_themed_renderable(renderable) for renderable in renderables),
            style=_themed_style(style),
            end_section=end_section,
        )


class ThemedDataTable(TextualDataTable):
    """Textual DataTable that resolves custom theme variables in cell markup."""

    @staticmethod
    def normalize_cell(cell: Any) -> Any:
        return _themed_renderable(cell)

    @staticmethod
    def normalize_cells(cells: Iterable[Any]) -> list[Any]:
        """Normalize markup before storing or comparing table cells."""
        return [_themed_renderable(cell) for cell in cells]

    def add_column(
        self,
        label: Any,
        *,
        width: int | None = None,
        key: str | None = None,
        default: Any = None,
    ):
        return super().add_column(
            self.normalize_cell(label),
            width=width,
            key=key,
            default=self.normalize_cell(default),
        )

    def add_row(
        self,
        *cells: Any,
        height: int | None = 1,
        key: str | None = None,
        label: Any = None,
    ):
        return super().add_row(
            *self.normalize_cells(cells),
            height=height,
            key=key,
            label=self.normalize_cell(label),
        )

    def update_cell(
        self,
        row_key: Any,
        column_key: Any,
        value: Any,
        *,
        update_width: bool = False,
    ) -> None:
        super().update_cell(
            row_key,
            column_key,
            self.normalize_cell(value),
            update_width=update_width,
        )

    def update_cell_at(
        self,
        coordinate: Any,
        value: Any,
        *,
        update_width: bool = False,
    ) -> None:
        super().update_cell_at(
            coordinate,
            self.normalize_cell(value),
            update_width=update_width,
        )
