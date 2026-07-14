from configparser import RawConfigParser
from decimal import Decimal
from io import StringIO
from typing import get_args

import pytest
from rich.console import Console

from dolphie.DataTypes import (
    ConnectionSource,
    ConnectionSourceType,
    ConnectionStatus,
    ConnectionStatusType,
    ProcesslistThread,
    ProxySQLProcesslistThread,
)
from dolphie.Modules.ArgumentParser import ArgumentParser, Config, CredentialProfile
from dolphie.Modules.Functions import coerce_float, coerce_int, coerce_str, format_bytes, format_number, format_time


def test_connection_namespaces_remain_string_values():
    assert ConnectionSource.mysql == "MySQL"
    assert ConnectionStatus.connected == "CONNECTED"
    assert set(get_args(ConnectionSourceType)) == {"MySQL", "ProxySQL", "MariaDB"}
    assert set(get_args(ConnectionStatusType)) == {"CONNECTING", "CONNECTED", "DISCONNECTED", "R/W", "RO"}


def test_database_scalar_coercion_uses_safe_defaults():
    assert coerce_int("42") == 42
    assert coerce_int(None, default=7) == 7
    assert coerce_int("not-a-number", default=7) == 7
    assert coerce_float(Decimal("1.25")) == 1.25
    assert coerce_float(float("inf"), default=2.5) == 2.5
    assert coerce_str(None, default="N/A") == "N/A"


def test_format_helpers_accept_the_values_they_format():
    assert format_time(1.5) == "00:00:01"
    assert format_time(None) == "N/A"
    assert format_bytes(Decimal("1024"), color=False) == "1KB"
    assert format_bytes("N/A") == "N/A"
    assert format_number(Decimal("0.5"), color=False) == "0.50"


def test_processlist_threads_coerce_nullable_database_scalars():
    mysql_thread = ProcesslistThread({"id": 7, "time": "3", "query": None, "user": None})
    proxysql_thread = ProxySQLProcesslistThread(
        {
            "id": 8,
            "hostgroup": None,
            "time": "1500",
            "query": None,
            "frontend_host": None,
            "backend_host": None,
        }
    )

    assert mysql_thread.id == "7"
    assert mysql_thread.time == 3
    assert mysql_thread.user == ""
    assert mysql_thread.formatted_query.code == ""
    assert proxysql_thread.hostgroup == 0
    assert proxysql_thread.time == 1.5
    assert proxysql_thread.frontend_host == "[$dark_gray]N/A"


@pytest.mark.parametrize(
    ("query_time", "color"),
    [(1, "$green"), (6, "$yellow"), (11, "$red")],
)
def test_processlist_time_colors_use_textual_theme_variables(query_time: int, color: str):
    thread = ProcesslistThread({"id": 7, "time": query_time, "query": "SELECT 1"})

    assert thread.formatted_time == f"[{color}]00:00:{query_time:02d}[/{color}]"


def test_hostgroup_without_host_exits_cleanly():
    parser = object.__new__(ArgumentParser)
    parser.config = Config("test")
    parser.console = Console(file=StringIO())
    config = RawConfigParser()
    config.read_dict({"cluster": {"primary": '{"port": 3306}'}})

    with pytest.raises(SystemExit):
        parser.parse_hostgroup(config, "cluster", "dolphie.cnf")


def test_hostgroup_port_and_profile_are_typed_values():
    parser = object.__new__(ArgumentParser)
    parser.config = Config("test")
    parser.config.credential_profiles["production"] = CredentialProfile("production")
    parser.console = Console(file=StringIO())
    config = RawConfigParser()
    config.read_dict(
        {
            "cluster": {
                "primary": '{"host": "db.example.com:3307", "credential_profile": "production"}',
            }
        }
    )

    member = parser.parse_hostgroup(config, "cluster", "dolphie.cnf")[0]

    assert member.host == "db.example.com"
    assert member.port == 3307
    assert member.credential_profile == "production"
