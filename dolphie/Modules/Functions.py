from __future__ import annotations

import os
import re
from decimal import Decimal

import charset_normalizer
from pygments.style import Style
from pygments.token import (
    Comment,
    Error,
    Generic,
    Keyword,
    Name,
    Number,
    Operator,
    Punctuation,
    String,
    Whitespace,
)
from rich.markup import escape as markup_escape
from rich.syntax import Syntax


class NordModifiedTheme(Style):
    nord0 = "#0f1525"
    nord1 = "#3b4252"
    nord2 = "#434c5e"
    nord3 = "#4c566a"
    nord3_bright = "#616e87"

    nord4 = "#d8dee9"
    nord5 = "#e5e9f0"
    nord6 = "#eceff4"

    nord7 = "#8fbcbb"
    nord8 = "#88c0d0"
    nord9 = "#879bca"
    nord10 = "#5e81ac"

    nord11 = "#bf616a"
    nord12 = "#d08770"
    nord13 = "#81c194"
    nord14 = "#ac8bdd"
    nord15 = "#ca87a5"

    background_color = nord0
    default = nord4

    styles = {
        Whitespace: nord4,
        Comment: f"italic {nord3_bright}",
        Comment.Preproc: nord10,
        Keyword: f"bold {nord9}",
        Keyword.Pseudo: f"nobold {nord9}",
        Keyword.Type: f"nobold {nord9}",
        Operator: nord9,
        Operator.Word: f"bold {nord9}",
        Name: nord4,
        Name.Builtin: nord9,
        Name.Function: nord8,
        Name.Class: nord7,
        Name.Namespace: nord7,
        Name.Exception: nord11,
        Name.Variable: nord4,
        Name.Constant: nord7,
        Name.Label: nord7,
        Name.Entity: nord12,
        Name.Attribute: nord7,
        Name.Tag: nord9,
        Name.Decorator: nord12,
        Punctuation: nord6,
        String: nord14,
        String.Doc: nord3_bright,
        String.Interpol: nord14,
        String.Escape: nord13,
        String.Regex: nord13,
        String.Symbol: nord14,
        String.Other: nord14,
        Number: nord15,
        Generic.Heading: f"bold {nord8}",
        Generic.Subheading: f"bold {nord8}",
        Generic.Deleted: nord11,
        Generic.Inserted: nord14,
        Generic.Error: nord11,
        Generic.Emph: "italic",
        Generic.Strong: "bold",
        Generic.Prompt: f"bold {nord3}",
        Generic.Output: nord4,
        Generic.Traceback: nord11,
        Error: nord11,
    }


def format_query(query: str, minify: bool = True) -> Syntax:
    if not query:
        return Syntax(code="", lexer="sql", word_wrap=True, theme=NordModifiedTheme)

    query = markup_escape(re.sub(r"\s+", " ", query)) if minify else query

    formatted_query = Syntax(code=query, lexer="sql", word_wrap=True, theme=NordModifiedTheme)

    return formatted_query


def minify_query(query: str) -> str:
    if not query:
        return ""

    return markup_escape(re.sub(r"\s+", " ", query))


def format_bytes(bytes_value, color=True, decimal=2):
    if isinstance(bytes_value, str):
        return bytes_value

    units = ["B", "KB", "MB", "GB", "TB"]
    unit_index = 0

    while bytes_value >= 1024 and unit_index < len(units) - 1:
        bytes_value /= 1024
        unit_index += 1

    # Format with specified rounding precision
    formatted_value = f"{bytes_value:.{decimal}f}"

    # Remove unnecessary ".00" if rounding results in a whole number
    if formatted_value.endswith(f".{'0' * decimal}"):
        formatted_value = formatted_value[: -decimal - 1]

    if bytes_value == 0:
        return "0"
    elif color:
        return f"{formatted_value}[highlight]{units[unit_index]}[/highlight]"
    else:
        return f"{formatted_value}{units[unit_index]}"


def format_time(time: int, picoseconds=False):
    if time is None:
        return "N/A"

    seconds = time / 1e12 if picoseconds else time

    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    return f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"


def format_picoseconds(ps: int) -> str:
    units = [
        ("d", 24 * 60 * 60 * 1_000_000_000_000),  # days
        ("h", 60 * 60 * 1_000_000_000_000),  # hours
        ("m", 60 * 1_000_000_000_000),  # minutes
        ("s", 1_000_000_000_000),  # seconds
        ("ms", 1_000_000_000),  # milliseconds
        ("µs", 1_000_000),  # microseconds
        ("ns", 1_000),  # nanoseconds
        ("ps", 1),  # picoseconds
    ]

    for unit, factor in units:
        if ps >= factor:
            value = ps / factor
            return f"{value:.2f}[highlight]{unit}"
    return "[dark_gray]0"


def load_host_cache_file(host_cache_file: str):
    host_cache = {}
    if os.path.exists(host_cache_file):
        with open(host_cache_file) as file:
            for line in file:
                line = line.strip()
                error_message = f"Host cache entry '{line}' is not properly formatted! Format: ip=hostname"

                if "=" not in line:
                    raise Exception(error_message)

                host, hostname = line.split("=", maxsplit=1)
                host = host.strip()
                hostname = hostname.strip()

                if not host or not hostname:
                    raise Exception(error_message)

                host_cache[host] = hostname

    return host_cache


def detect_encoding(text):
    # Since BLOB/BINARY data can be involved, we need to auto-detect what the encoding is
    # for queries since it can be anything. If I let pymsql use unicode by default I got
    # consistent crashes due to unicode errors for utf8 so we have to go this route
    result = charset_normalizer.detect(text)
    encoding = result["encoding"]

    if encoding is None:
        encoding = "latin1"
    elif encoding == "utf-16be":
        encoding = "utf-8"

    return encoding


def round_num(n, decimal=2):
    n = Decimal(n)
    return n.to_integral() if n == n.to_integral() else round(n.normalize(), decimal)


# This is from https://pypi.org/project/numerize
def format_number(n, decimal=2, color=True):
    if not n or n == "0":
        return "0"

    # fmt: off
    sufixes = ["", "K", "M", "B", "T", "Qa", "Qu", "S", "Oc", "No",
               "D", "Ud", "Dd", "Td", "Qt", "Qi", "Se", "Od", "Nd", "V",
               "Uv", "Dv", "Tv", "Qv", "Qx", "Sx", "Ox", "Nx", "Tn", "Qa",
               "Qu", "S", "Oc", "No", "D", "Ud", "Dd", "Td", "Qt", "Qi",
               "Se", "Od", "Nd", "V", "Uv", "Dv", "Tv", "Qv", "Qx", "Sx",
               "Ox", "Nx", "Tn", "x", "xx", "xxx", "X", "XX", "XXX", "END"]

    sci_expr = [1e0, 1e3, 1e6, 1e9, 1e12, 1e15, 1e18, 1e21, 1e24, 1e27,
                1e30, 1e33, 1e36, 1e39, 1e42, 1e45, 1e48, 1e51, 1e54, 1e57,
                1e60, 1e63, 1e66, 1e69, 1e72, 1e75, 1e78, 1e81, 1e84, 1e87,
                1e90, 1e93, 1e96, 1e99, 1e102, 1e105, 1e108, 1e111, 1e114, 1e117,
                1e120, 1e123, 1e126, 1e129, 1e132, 1e135, 1e138, 1e141, 1e144, 1e147,
                1e150, 1e153, 1e156, 1e159, 1e162, 1e165, 1e168, 1e171, 1e174, 1e177]
    # fmt: on

    # Convert string to a number format if needed
    if isinstance(n, str):
        try:
            n = float(n)
        except ValueError:
            return n

    n = abs(n)
    for x in range(len(sci_expr)):
        if n >= sci_expr[x] and n < sci_expr[x + 1]:
            sufix = sufixes[x]
            num = str(round_num(n / sci_expr[x], decimal)) if n >= 1000.0 else str(round_num(n, 0))
            if color:
                return f"{num}[highlight]{sufix}[/highlight]" if sufix else num
            else:
                return f"{num}{sufix}" if sufix else num


def format_sys_table_memory(data):
    parsed_data = data.strip().split(" ")
    if len(parsed_data) == 2:
        value, suffix = parsed_data[0], parsed_data[1][:1]

        if value == "0":
            suffix = ""
        elif suffix != "b":
            suffix += "B"
        elif suffix == "b":
            suffix = "B"

        return f"{value}[highlight]{suffix}"

    return data


def escape_markup(text: str) -> str:
    """Escape Rich markup characters in the given text.

    Args:
        text (str): The text to escape.

    Returns:
        str: The escaped text.
    """
    return text.replace("[", r"\[")


def parse_filter(filter_value: str) -> tuple[str, bool]:
    """Split a leading ! off a filter value, which means "exclude what matches this".

    Args:
        filter_value (str): The filter value as the user typed it.

    Returns:
        tuple[str, bool]: The value without the ! and whether the filter is negated.
    """
    filter_value = str(filter_value)
    if filter_value.startswith("!"):
        return filter_value[1:], True

    return filter_value, False


def merge_filters(*filter_sets: dict) -> dict:
    """Merge filters from the least specific source to the most, so each one only overrides
    the filters it sets.

    Args:
        *filter_sets (dict): Filters to merge, least specific first. A filter set to None unsets
                             it, which is how a bare name= removes one an earlier set applied.

    Returns:
        dict: The filters left in effect.
    """
    merged = {}
    for filters in filter_sets:
        merged.update(filters)

    return {filter_name: value for filter_name, value in merged.items() if value is not None}


def filter_excludes(filter_value: str, thread_value: str, partial: bool = False) -> bool:
    """Determine if a thread should be hidden by a filter, honoring ! negation.

    Args:
        filter_value (str): The filter value as the user typed it.
        thread_value (str): The thread's value to compare against.
        partial (bool): Match the filter anywhere in the thread's value instead of exactly.

    Returns:
        bool: True if the thread doesn't pass the filter.
    """
    value, negate = parse_filter(filter_value)
    matched = value in (thread_value or "") if partial else value == thread_value

    return matched if negate else not matched


def filter_sql_condition(columns: str | list[str], filter_value: str, pattern: str = None) -> str:
    """Build a WHERE clause condition for a filter value, honoring ! negation.

    Args:
        columns (str | list[str]): Column(s) to compare. Multiple columns are OR'd together
                                   (AND'd when negated, so a thread has to fail every column).
        filter_value (str): The filter value as the user typed it.
        pattern (str): A LIKE pattern with {} where the value goes (i.e. "%{}%").
                       Compares with = when not specified.

    Returns:
        str: The condition to add to a WHERE clause.
    """
    value, negate = parse_filter(filter_value)
    comparison = f"LIKE '{pattern.format(value)}'" if pattern else f"= '{value}'"

    conditions = []
    for column in [columns] if isinstance(columns, str) else columns:
        if negate:
            # IFNULL so threads with a NULL column aren't excluded by SQL's NULL comparison rules
            conditions.append(f"NOT (IFNULL({column}, '') {comparison})")
        else:
            conditions.append(f"{column} {comparison}")

    if len(conditions) == 1:
        return conditions[0]

    return f"({' AND '.join(conditions)})" if negate else f"({' OR '.join(conditions)})"
