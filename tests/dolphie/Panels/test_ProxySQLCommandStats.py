from contextlib import nullcontext
from types import SimpleNamespace
from typing import cast

from dolphie.Modules.TabManager import Tab
from dolphie.Panels.ProxySQLCommandStats import create_panel


class FakeDataTable:
    def __init__(self) -> None:
        self.columns: dict[str, object] = {}
        self.rows: dict[str, list[object]] = {}

    @property
    def row_count(self) -> int:
        return len(self.rows)

    def add_column(self, label: object, *, key: str, width: object) -> None:
        self.columns[key] = label

    def add_row(self, *values: object, key: str) -> None:
        self.rows[key] = list(values)

    @staticmethod
    def normalize_cells(cells: list[object]) -> list[object]:
        return cells

    def get_row(self, key: str) -> list[object]:
        return self.rows[key]

    def update_cell(self, row_key: str, column_key: str, value: object) -> None:
        column_index = list(self.columns).index(column_key)
        self.rows[row_key][column_index] = value

    def clear(self) -> None:
        self.rows.clear()

    def remove_row(self, key: str) -> None:
        self.rows.pop(key)


def test_command_stats_keep_nested_per_second_history() -> None:
    datatable = FakeDataTable()
    title = SimpleNamespace(update=lambda value: None)
    dolphie = SimpleNamespace(
        app=SimpleNamespace(batch_update=nullcontext),
        panels=SimpleNamespace(proxysql_command_stats=SimpleNamespace(title="Command Stats")),
        polling_latency=1.0,
        proxysql_command_stats=[{"Command": "SELECT", "cnt_100us": "5"}],
        proxysql_per_second_data={},
    )
    tab = cast(
        Tab,
        SimpleNamespace(
            dolphie=dolphie,
            proxysql_command_stats_datatable=datatable,
            proxysql_command_stats_title=title,
        ),
    )

    create_panel(tab)
    assert dolphie.proxysql_per_second_data["SELECT"]["cnt_100us"] == 5

    dolphie.proxysql_command_stats = [{"Command": "SELECT", "cnt_100us": "8"}]
    create_panel(tab)
    assert dolphie.proxysql_per_second_data["SELECT"]["cnt_100us"] == 8
