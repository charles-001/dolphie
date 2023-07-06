from rich import box
from rich.style import Style
from rich.syntax import Syntax
from rich.table import Table


class ManualException(Exception):
    def __init__(self, message, query="", reason=""):
        self.message = message
        self.reason = reason
        self.query = query

    def output(self):
        table_exception = Table(
            box=box.SIMPLE_HEAVY,
            style="grey70",
            title="  Dolphie :dolphin:",
            title_style=Style(color="grey93", bold=True),
            title_justify="left",
            header_style=Style(color="indian_red", bold=True),
            show_header=True,
        )

        table_exception.add_column("Error", justify="left")
        table_exception.add_row(self.message)

        if self.query:
            self.query = Syntax(
                self.query,
                "sql",
                line_numbers=False,
                word_wrap=True,
                theme="monokai",
                background_color="default",
            )
            table_exception.add_row(self.query)

        table_exception.add_row("")

        if self.reason:
            table_exception.add_row("[indian_red]%s" % self.reason)

        return table_exception
