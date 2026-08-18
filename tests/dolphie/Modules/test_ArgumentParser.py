import sys

import pytest

from dolphie.Modules.ArgumentParser import ArgumentParser

CONFIG_FILE = """
[dolphie]
filters = time=5

[credential_profile_prod]
host = prod-db.example.com
user = monitor
filters = user=!azure_superuser,db=mydb

[credential_profile_reporting]
host = reporting-db.example.com
user = monitor

[myhostgroup]
1 = {"host": "prod-1.example.com", "credential_profile": "prod"}
2 = {"host": "reporting-1.example.com", "credential_profile": "reporting"}
"""


@pytest.fixture
def parse_config(tmp_path, monkeypatch):
    # Point at our own config file so the environment's config files aren't picked up
    config_file = tmp_path / "dolphie.cnf"
    config_file.write_text(CONFIG_FILE)
    monkeypatch.delenv("DOLPHIE_CONFIG", raising=False)

    def parse(*args):
        monkeypatch.setattr(sys, "argv", ["dolphie", "--config-file", str(config_file), *args])

        return ArgumentParser("test").config

    return parse


@pytest.fixture
def argument_parser(tmp_path, monkeypatch):
    # Point at an empty config file so the environment's config files aren't picked up
    config_file = tmp_path / "dolphie.cnf"
    config_file.write_text("[dolphie]\n")
    monkeypatch.setattr(sys, "argv", ["dolphie", "--config-file", str(config_file)])

    return ArgumentParser("test")


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, {}),
        ("", {}),
        ("user=azure_superuser", {"user": "azure_superuser"}),
        ("user=!azure_superuser", {"user": "!azure_superuser"}),  # A leading ! excludes what it matches
        ("time=5", {"time": 5}),  # Query time is the one filter stored as an int
        ("hostgroup=!1", {"hostgroup": "!1"}),
        ("user=bob,db=mydb,host=10.0.0.5", {"user": "bob", "db": "mydb", "host": "10.0.0.5"}),
        (" user = bob , db = mydb ", {"user": "bob", "db": "mydb"}),
        ("user=bob,", {"user": "bob"}),
        # A bare name= unsets the filter when the sources are merged
        ("time=", {"time": None}),
        ("user=bob,db=", {"user": "bob", "db": None}),
        # A comma only starts another filter when what follows it is a name=value pair
        ("query=select a,b from t", {"query": "select a,b from t"}),
        (
            "query=insert into t (a,b) values (1,2),user=bob",
            {"query": "insert into t (a,b) values (1,2)", "user": "bob"},
        ),
    ],
)
def test_parse_filters(argument_parser, value, expected):
    assert argument_parser.parse_filters("test", value) == expected


@pytest.mark.parametrize(
    ("value", "expected_error"),
    [
        ("bogus=x", "Invalid filter"),
        ("bogus=", "Invalid filter"),  # Unsetting is still limited to the filters that exist
        ("user=bob,bogus=x", "Invalid filter"),  # A typo isn't swallowed into the value before it
        ("user", "must be in the format name=value"),
        ("time=abc", "must be an integer"),
        ("hostgroup=abc", "must be an integer"),
        ("time=!5", "doesn't support"),  # Query time is a minimum, so excluding a value from it means nothing
    ],
)
def test_parse_filters_rejects_bad_input(argument_parser, capsys, value, expected_error):
    with pytest.raises(SystemExit):
        argument_parser.parse_filters("test", value)

    assert expected_error in capsys.readouterr().out


@pytest.mark.parametrize(
    ("args", "expected"),
    [
        # Dolphie's config on its own
        ([], {"time": 5}),
        # A credential profile keeps the filters it doesn't set
        (["-C", "prod"], {"time": 5, "user": "!azure_superuser", "db": "mydb"}),
        (["--filters", "user=bob"], {"time": 5, "user": "bob"}),
        # The command-line only overrides the filters it sets
        (["-C", "prod", "--filters", "user=bob,time=10"], {"time": 10, "user": "bob", "db": "mydb"}),
        (
            ["-C", "prod", "--filters", "host=10.0.0.5"],
            {"time": 5, "user": "!azure_superuser", "db": "mydb", "host": "10.0.0.5"},
        ),
        # A bare name= drops what an earlier source set
        (["--filters", "time="], {}),
        (["-C", "prod", "--filters", "time=,db="], {"user": "!azure_superuser"}),
    ],
)
def test_filters_merge_from_least_to_most_specific(parse_config, args, expected):
    assert parse_config(*args).filter_values == expected


def test_merged_filters_are_saved_back_to_the_option(parse_config):
    # So the filters option reflects what tabs will actually start with
    config = parse_config("-C", "prod", "--filters", "time=10")

    assert config.filters == "time=10,user=!azure_superuser,db=mydb"


def test_debug_options_shows_the_filters_each_hostgroup_host_starts_with(parse_config, capsys, monkeypatch):
    # Hosts merge their credential profile's filters when their tab is created, which is after
    # debug options are printed, so the merged option alone doesn't cover them
    monkeypatch.setenv("COLUMNS", "300")  # Wide enough that the table doesn't wrap or truncate

    with pytest.raises(SystemExit):
        parse_config("-H", "myhostgroup", "--debug-options")

    rows = [" ".join(line.split()) for line in capsys.readouterr().out.split("\n")]

    # Hosts are labeled by the hostgroup and the key they're listed under in the config
    assert "hostgroup myhostgroup:1 filters time=5,user=!azure_superuser,db=mydb" in rows
    assert "hostgroup myhostgroup:2 filters time=5" in rows

    # The rows above cover every host, so the merged option on its own would only be noise
    assert not [row for row in rows if row.startswith("merged filters")]
