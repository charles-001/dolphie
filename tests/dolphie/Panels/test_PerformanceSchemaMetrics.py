import re

from rich.text import Text

from dolphie.Modules.Theme import HIGHLIGHT, LIGHT_BLUE
from dolphie.Panels.PerformanceSchemaMetrics import _format_file_or_table_name


def test_combined_file_io_event_uses_styled_literal_brackets() -> None:
    label = _format_file_or_table_name("InnoDB redo logs", re.compile(r"^$"))

    assert isinstance(label, Text)
    assert label.plain == "[InnoDB redo logs]"
    assert str(label.spans[0].style) == f"bold {LIGHT_BLUE}"
    assert str(label.spans[1].style) == HIGHLIGHT
    assert str(label.spans[2].style) == f"bold {LIGHT_BLUE}"
