import sqlite3

import pytest

from dolphie.Modules.Functions import filter_excludes, filter_sql_condition, merge_filters, parse_filter


@pytest.mark.parametrize(
    ("filter_value", "expected"),
    [
        ("azure_superuser", ("azure_superuser", False)),
        ("!azure_superuser", ("azure_superuser", True)),
        ("", ("", False)),
        ("!", ("", True)),
        ("a!b", ("a!b", False)),  # Only a leading ! negates
        (1, ("1", False)),  # Numeric filters (i.e. hostgroup) come in as ints
    ],
)
def test_parse_filter(filter_value, expected):
    assert parse_filter(filter_value) == expected


@pytest.mark.parametrize(
    ("filter_sets", "expected"),
    [
        ([], {}),
        ([{"time": 5}], {"time": 5}),
        # A more specific set only overrides the filters it has
        ([{"time": 5}, {"user": "bob"}], {"time": 5, "user": "bob"}),
        ([{"time": 5, "user": "bob"}, {"user": "alice"}], {"time": 5, "user": "alice"}),
        # None unsets a filter an earlier set applied
        ([{"time": 5, "user": "bob"}, {"time": None}], {"user": "bob"}),
        ([{"time": 5}, {"user": None}], {"time": 5}),  # Unsetting one that isn't set does nothing
        ([{"time": None}, {"time": 5}], {"time": 5}),  # A later set can put one back
    ],
)
def test_merge_filters(filter_sets, expected):
    assert merge_filters(*filter_sets) == expected


@pytest.mark.parametrize(
    ("filter_value", "thread_value", "partial", "expected"),
    [
        # Exact matching
        ("azure_superuser", "azure_superuser", False, False),
        ("azure_superuser", "app_user", False, True),
        ("!azure_superuser", "azure_superuser", False, True),
        ("!azure_superuser", "app_user", False, False),
        # Partial matching
        ("10.0.", "10.0.0.5:3306", True, False),
        ("10.0.", "192.168.1.9:3306", True, True),
        ("!10.0.", "10.0.0.5:3306", True, True),
        ("!10.0.", "192.168.1.9:3306", True, False),
        # Threads with no value for the field (i.e. no database) are kept when excluding
        ("mydb", None, False, True),
        ("!mydb", None, False, False),
        ("select", None, True, True),
        ("!select", None, True, False),
    ],
)
def test_filter_excludes(filter_value, thread_value, partial, expected):
    assert filter_excludes(filter_value, thread_value, partial=partial) == expected


@pytest.mark.parametrize(
    ("columns", "filter_value", "pattern", "expected"),
    [
        ("User", "azure_superuser", None, "User = 'azure_superuser'"),
        ("User", "!azure_superuser", None, "NOT (IFNULL(User, '') = 'azure_superuser')"),
        ("Host", "10.0.", "{}%", "Host LIKE '10.0.%'"),
        ("Host", "!10.0.", "{}%", "NOT (IFNULL(Host, '') LIKE '10.0.%')"),
        (
            ["Info", "trx_query"],
            "select",
            "%%{}%%",
            "(Info LIKE '%%select%%' OR trx_query LIKE '%%select%%')",
        ),
        # Negating multiple columns has to AND them so a thread fails every one of them
        (
            ["Info", "trx_query"],
            "!select",
            "%%{}%%",
            "(NOT (IFNULL(Info, '') LIKE '%%select%%') AND NOT (IFNULL(trx_query, '') LIKE '%%select%%'))",
        ),
    ],
)
def test_filter_sql_condition(columns, filter_value, pattern, expected):
    assert filter_sql_condition(columns, filter_value, pattern) == expected


@pytest.mark.parametrize(
    ("columns", "filter_value", "pattern", "expected_ids"),
    [
        ("user", "azure_superuser", None, [1]),
        ("user", "!azure_superuser", None, [2, 3]),
        ("db", "mydb", None, [1, 3]),
        ("db", "!mydb", None, [2]),  # Thread 2 has a NULL db, so it isn't excluded
        ("host", "10.0.", "{}%", [1, 2]),
        ("host", "!10.0.", "{}%", [3]),
        (["info", "trx_query"], "select", "%{}%", [1, 2]),
        (["info", "trx_query"], "!select", "%{}%", [3]),  # Thread 3 has no query, so it isn't excluded
    ],
)
def test_filter_sql_condition_results(columns, filter_value, pattern, expected_ids):
    # ProxySQL's admin interface is SQLite, and it shares NULL comparison rules with MySQL
    connection = sqlite3.connect(":memory:")
    connection.execute("CREATE TABLE processlist (id int, user text, db text, host text, info text, trx_query text)")
    connection.executemany(
        "INSERT INTO processlist VALUES (?, ?, ?, ?, ?, ?)",
        [
            (1, "azure_superuser", "mydb", "10.0.0.5:3306", "select 1", None),
            (2, "app_user", None, "10.0.0.6:3306", None, "select 2"),
            (3, "app_user", "mydb", "192.168.1.9:3306", None, None),
        ],
    )

    condition = filter_sql_condition(columns, filter_value, pattern)
    rows = connection.execute(f"SELECT id FROM processlist WHERE {condition} ORDER BY id").fetchall()

    assert [row[0] for row in rows] == expected_ids
