import asyncio
from types import SimpleNamespace
from typing import cast

import pytest

from dolphie.App import DolphieApp
from dolphie.DataTypes import ConnectionSource
from dolphie.Modules.KeyEventManager import KeyEventManager

FILTER_ATTRIBUTES = (
    "user_filter",
    "host_filter",
    "db_filter",
    "hostgroup_filter",
    "query_time_filter",
    "query_filter",
)

# The order the filter modal dismisses its values in
NO_FILTERS_SUBMITTED = ["", "", "", "", "", ""]


def create_dolphie(**filters):
    """A stand-in for Dolphie with only what the filter command touches."""
    dolphie = SimpleNamespace(
        processlist_threads_snapshot={},
        filter_dropdown_values={field: set() for field in ("user", "db", "host", "hostgroup")},
        host_cache={},
        connection_source=ConnectionSource.mysql,
        replay_file=None,
        secondary_db_connection=None,
        main_db_connection=SimpleNamespace(is_connected=lambda: True),
    )

    for attribute in FILTER_ATTRIBUTES:
        setattr(dolphie, attribute, filters.get(attribute))

    return dolphie


def press_filter_key(dolphie, submitted):
    """Run the filter command, answer its modal with submitted, and report what it did."""
    notifications = []
    prefilled = []
    refreshes = []

    def push_screen(modal, callback):
        # The modal is what turns the filters in effect into prefilled fields
        prefilled.append(modal.current_filters)
        callback(submitted)

    app = SimpleNamespace(
        tab_manager=SimpleNamespace(active_tab=SimpleNamespace(dolphie=dolphie), loading_hostgroups=False),
        command_manager=SimpleNamespace(
            exclude_keys=set(),
            get_commands=lambda replay_file, connection_source: {"f": {"human_key": "f"}},
        ),
        notify=lambda message, title=None, severity=None, timeout=None: notifications.append((title, message)),
        force_refresh_for_replay=lambda need_current_data=False: refreshes.append(need_current_data),
        app=SimpleNamespace(push_screen=push_screen),
    )

    asyncio.run(KeyEventManager(cast(DolphieApp, app)).process_key_event("f"))

    return SimpleNamespace(
        filters={attribute: getattr(dolphie, attribute) for attribute in FILTER_ATTRIBUTES},
        prefilled=prefilled[0],
        notifications=notifications,
        refreshes=refreshes,
    )


def test_submitted_filters_are_applied():
    dolphie = create_dolphie()

    result = press_filter_key(dolphie, ["!azure_superuser", "", "mydb", "", "5", ""])

    assert result.filters == {
        "user_filter": "!azure_superuser",
        "host_filter": None,
        "db_filter": "mydb",
        "hostgroup_filter": None,
        "query_time_filter": 5,  # Query time is the one filter stored as an int
        "query_filter": None,
    }
    assert result.refreshes == [True]
    assert [title for title, _ in result.notifications] == ["Filter applied"] * 3
    assert result.notifications[0][1] == "[b]User[/b]: not [$b_highlight]azure_superuser[/$b_highlight]"


def test_filters_in_effect_are_given_to_the_modal():
    dolphie = create_dolphie(user_filter="!azure_superuser", db_filter="mydb", query_time_filter=5)

    result = press_filter_key(dolphie, NO_FILTERS_SUBMITTED)

    assert result.prefilled == {
        "username": "!azure_superuser",
        "host": None,
        "db": "mydb",
        "hostgroup": None,
        "query_time": 5,
        "query_text": None,
    }


def test_resubmitting_filters_unchanged_notifies_nothing():
    dolphie = create_dolphie(user_filter="!azure_superuser", db_filter="mydb", query_time_filter=5)

    result = press_filter_key(dolphie, ["!azure_superuser", "", "mydb", "", "5", ""])

    assert result.filters["user_filter"] == "!azure_superuser"
    assert result.filters["db_filter"] == "mydb"
    assert result.filters["query_time_filter"] == 5
    assert result.notifications == []


@pytest.mark.parametrize(
    ("submitted", "expected"),
    [
        (["!azure_superuser", "", "", "", "5", ""], ("!azure_superuser", None, 5)),
        (["", "", "mydb", "", "5", ""], (None, "mydb", 5)),
        (["!azure_superuser", "", "mydb", "", "", ""], ("!azure_superuser", "mydb", None)),
        (["!azure_superuser", "", "", "", "", ""], ("!azure_superuser", None, None)),
    ],
)
def test_clearing_a_field_removes_that_filter(submitted, expected):
    # The modal is prefilled with what's in effect, so an empty field means "remove this one"
    applied_filters = ("user_filter", "db_filter", "query_time_filter")
    dolphie = create_dolphie(user_filter="!azure_superuser", db_filter="mydb", query_time_filter=5)

    result = press_filter_key(dolphie, submitted)

    assert tuple(result.filters[attribute] for attribute in applied_filters) == expected
    # However many filters were removed, they're reported in one notification
    assert [title for title, _ in result.notifications] == ["Filter removed"]


def test_clearing_every_field_removes_all_filters():
    dolphie = create_dolphie(user_filter="!azure_superuser", db_filter="mydb", query_time_filter=5)

    result = press_filter_key(dolphie, NO_FILTERS_SUBMITTED)

    assert result.filters == dict.fromkeys(FILTER_ATTRIBUTES, None)
    assert result.refreshes == [True]

    title, message = result.notifications[0]
    assert (title, len(result.notifications)) == ("Filter removed", 1)
    for filter_name in ("User", "Database", "Minimum Query Time"):
        assert filter_name in message


def test_changing_a_filter_only_notifies_the_one_that_changed():
    dolphie = create_dolphie(user_filter="!azure_superuser", db_filter="mydb")

    result = press_filter_key(dolphie, ["!azure_superuser", "", "otherdb", "", "", ""])

    assert result.filters["db_filter"] == "otherdb"
    assert result.notifications == [("Filter applied", "[b]Database[/b]: [$b_highlight]otherdb[/$b_highlight]")]
