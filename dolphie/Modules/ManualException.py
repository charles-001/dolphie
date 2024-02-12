from rich import box
from rich.syntax import Syntax
from rich.table import Table


class ManualException(Exception):
    def __init__(self, reason, query=""):
        self.reason = reason
        self.query = query

    def output(self):
        table_exception = Table(box=box.SQUARE, show_header=True, style="#ec8888")

        table_exception.add_column("MySQL Connection Error")
        if self.query:
            table_exception.add_row("[red]Failed to execute query:[/red]")
            table_exception.add_row(
                Syntax(
                    self.query.strip(),
                    "sql",
                    line_numbers=False,
                    word_wrap=True,
                    theme="monokai",
                    background_color="#131626",
                )
            )
            table_exception.add_row("")

        if self.reason:
            # pymysql for some reason returns "ny connections" instead of "Too many connections"
            if isinstance(self.reason, str):
                self.reason = self.reason.replace("ny connections", "Too many connections")

            table_exception.add_row(self.reason)

        return table_exception
