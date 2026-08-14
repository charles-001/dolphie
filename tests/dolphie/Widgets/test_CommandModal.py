import asyncio

import pytest
from textual.app import App
from textual.widgets import Button, Input

from dolphie.DataTypes import ConnectionSource, HotkeyCommands, ProcesslistThread
from dolphie.Widgets.CommandModal import CommandModal

# The order the filter modal dismisses its values in
FILTER_FIELDS = ("username", "host", "db", "hostgroup", "query_time", "query_text")

PROCESSLIST_DATA = {
    "1": ProcesslistThread({"id": "1", "user": "app_user", "host": "app-server-1", "db": "mydb", "time": 1}),
    "2": ProcesslistThread({"id": "2", "user": "reporting", "host": "app-server-2", "db": "reports", "time": 2}),
}

# What's in effect when the modal is opened
CURRENT_FILTERS = {
    "username": "!azure_superuser",
    "host": None,
    "db": "mydb",
    "hostgroup": None,
    "query_time": 5,
    "query_text": None,
}


async def _open_filter_modal(current_filters, field_values):
    """Open the filter modal prefilled with current_filters, apply field_values, then submit it."""
    app = App()
    dismissed = []

    async with app.run_test() as pilot:
        modal = CommandModal(
            command=HotkeyCommands.thread_filter,
            message="Filter threads by field(s)",
            processlist_data=PROCESSLIST_DATA,
            host_cache_data={},
            connection_source=ConnectionSource.mysql,
            current_filters=current_filters,
        )
        app.push_screen(modal, dismissed.append)
        await pilot.pause()

        prefilled = {field: modal.query_one(f"#filter_by_{field}_input", Input).value for field in FILTER_FIELDS}

        for field, value in field_values.items():
            modal.query_one(f"#filter_by_{field}_input", Input).value = value

        modal.query_one("#submit", Button).press()
        await pilot.pause()

        # The modal is gone once it's dismissed, so there's only an error response when it isn't
        error_response = modal.query("#error_response")
        error = str(error_response.first().render()) if error_response and error_response.first().display else None

    return prefilled, dismissed[0] if dismissed else None, error


def submit_filter_modal(current_filters=None, field_values=None):
    return asyncio.run(_open_filter_modal(current_filters or {}, field_values or {}))


def test_filters_in_effect_are_prefilled():
    prefilled, _, _ = submit_filter_modal(CURRENT_FILTERS)

    assert prefilled == {
        "username": "!azure_superuser",
        "host": "",
        "db": "mydb",
        "hostgroup": "",
        "query_time": "5",  # Stored as an int, so it has to be rendered as text
        "query_text": "",
    }


def test_prefilled_filters_submit_unchanged():
    _, dismissed, error = submit_filter_modal(CURRENT_FILTERS)

    assert dismissed == ["!azure_superuser", "", "mydb", "", "5", ""]
    assert error is None


@pytest.mark.parametrize(
    ("cleared_field", "expected"),
    [
        ("username", ["", "", "mydb", "", "5", ""]),
        ("db", ["!azure_superuser", "", "", "", "5", ""]),
        ("query_time", ["!azure_superuser", "", "mydb", "", "", ""]),
    ],
)
def test_clearing_a_prefilled_field_returns_it_empty(cleared_field, expected):
    _, dismissed, error = submit_filter_modal(CURRENT_FILTERS, {cleared_field: ""})

    assert dismissed == expected
    assert error is None


def test_clearing_every_prefilled_field_is_allowed():
    # Submitting nothing is how every filter gets removed at once
    _, dismissed, error = submit_filter_modal(CURRENT_FILTERS, dict.fromkeys(FILTER_FIELDS, ""))

    assert dismissed == [""] * len(FILTER_FIELDS)
    assert error is None


def test_a_field_is_required_when_no_filters_are_in_effect():
    # With nothing prefilled there's nothing to remove, so an empty submit is a mistake
    _, dismissed, error = submit_filter_modal()

    assert dismissed is None
    assert error == "At least one field must be provided"
