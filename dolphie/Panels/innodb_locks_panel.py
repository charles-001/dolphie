import re

from dolphie import Dolphie
from dolphie.Functions import detect_encoding, format_number
from rich import box
from rich.style import Style
from rich.table import Table


def create_panel(dolphie: Dolphie):
    innodb_lock_threads = {}

    dolphie.db.cursor.execute(dolphie.innodb_locks_sql)
    threads = dolphie.db.cursor.fetchall()

    for counter, thread in enumerate(threads):
        w_query = re.sub(r"\s+", " ", thread["waiting_query"].decode(detect_encoding(thread["waiting_query"])))
        b_query = re.sub(r"\s+", " ", thread["blocking_query"].decode(detect_encoding(thread["blocking_query"])))

        innodb_lock_threads[counter] = {
            "w_thread": thread["waiting_thread"].decode(),
            "w_query": re.sub(r"\s+", " ", w_query),
            "w_rows_modified": thread["waiting_rows_modified"].decode(),
            "w_age": thread["waiting_age"].decode(),
            "w_wait_secs": thread["waiting_wait_secs"].decode(),
            "b_thread": thread["blocking_thread"].decode(),
            "b_query": re.sub(r"\s+", " ", b_query),
            "b_rows_modified": thread["blocking_rows_modified"].decode(),
            "b_age": thread["blocking_age"].decode(),
            "b_wait_secs": thread["blocking_wait_secs"].decode(),
            "lock_mode": thread["lock_mode"].decode(),
            "lock_type": thread["lock_type"].decode(),
        }

    table = Table(header_style="bold white", box=box.SIMPLE_HEAVY, style="steel_blue1")

    columns = {}
    columns["[W](W) Thread ID"] = {"field": "w_thread", "width": 13, "format_number": False}
    columns["[W]Age"] = {"field": "w_age", "width": 4, "format_number": False}
    columns["[W]Wait"] = {"field": "w_wait_secs", "width": 5, "format_number": False}
    columns["[W]Query"] = {"field": "w_query", "width": None, "format_number": False}
    columns["[B](B) Thread ID"] = {"field": "b_thread", "width": 13, "format_number": False}
    columns["[B]Age"] = {"field": "b_age", "width": 4, "format_number": False}
    columns["[B]Wait"] = {"field": "b_wait_secs", "width": 5, "format_number": False}
    columns["[B]Rows Mod"] = {"field": "b_rows_modified", "width": 8, "format_number": True}
    columns["[B]Query"] = {"field": "b_query", "width": None, "format_number": False}
    columns["Mode"] = {"field": "lock_mode", "width": 7, "format_number": False}
    columns["Type"] = {"field": "lock_type", "width": 8, "format_number": False}

    total_width = 0
    for column, data in columns.items():
        if "Query" in column:
            overflow = "crop"
        else:
            overflow = "ellipsis"

        if "[B]" in column:
            column = column.replace("[B]", "")
            row_style = Style(color="red")
        elif "[W]" in column:
            column = column.replace("[W]", "")
            row_style = Style(color="magenta")
        else:
            row_style = Style(color="grey93")

        if data["width"]:
            table.add_column(column, width=data["width"], no_wrap=True, header_style=row_style, overflow=overflow)
            total_width += data["width"]
        else:
            table.add_column(column, no_wrap=True, header_style=row_style, overflow=overflow)

    # This variable is to cut off query so it's the perfect width for the auto-sized column that matches terminal width
    query_characters = round((dolphie.console.size.width / 2) - ((len(columns) * 4) + 6))

    for id, thread in innodb_lock_threads.items():
        b_wait_secs = "0s"
        if thread["b_wait_secs"]:
            b_wait_secs = "%ss" % thread["b_wait_secs"]

        w_wait_secs = "0s"
        if thread["w_wait_secs"]:
            w_wait_secs = "%ss" % thread["w_wait_secs"]

        waiting_query = thread["w_query"]
        if not waiting_query:
            waiting_query = waiting_query.ljust(query_characters)

        elif len(waiting_query) < query_characters:
            waiting_query = waiting_query.ljust(dolphie.console.size.width)

        blocking_query = thread["b_query"]
        if not blocking_query:
            blocking_query = blocking_query.ljust(query_characters)

        elif len(blocking_query) < query_characters:
            blocking_query = blocking_query.ljust(dolphie.console.size.width)

        # Change values to what we want
        thread["w_age"] = "%ss" % thread["w_age"]
        thread["b_age"] = "%ss" % thread["b_age"]
        thread["w_query"] = waiting_query[0:query_characters]
        thread["b_query"] = blocking_query[0:query_characters]
        thread["w_wait_secs"] = w_wait_secs
        thread["b_wait_secs"] = b_wait_secs

        # Add rows to the table
        row_values = []
        for column, data in columns.items():
            if data["format_number"]:
                row_values.append(format_number(thread[data["field"]]))
            else:
                row_values.append(thread[data["field"]])

        table.add_row(*row_values, style="grey93")

    # Add an invisible row to keep query columns sized correctly
    if len(innodb_lock_threads) == 0:
        empty_values = []
        for column, data in columns.items():
            if data["width"]:
                empty_values.append("")
            else:
                empty_values.append("".ljust(query_characters))

        table.add_row(*empty_values)

    return table
