from dolphie.Modules.Functions import format_query, minify_query
from loguru import logger
from rich import box
from rich.table import Table


class ManualException(Exception):
    def __init__(self, reason: str, query: str = ""):
        self.reason = reason
        self.query = query

    def output(self):
        table_exception = Table(box=box.SQUARE, show_header=True, style="#ec8888")

        table_exception.add_column("MySQL Connection Error", overflow="fold")

        logger_message = []

        if self.query:
            table_exception.add_row("[red]Failed to execute query:[/red]")
            table_exception.add_row(format_query(self.query, minify=False))
            table_exception.add_row("")

            logger_message.append(f"Query: {minify_query(self.query)}")

        if self.reason:
            # pymysql for some reason returns "ny connections" instead of "Too many connections"
            if isinstance(self.reason, str):
                self.reason = self.reason.replace("ny connections", "Too many connections")

            table_exception.add_row(self.reason)
            logger_message.append(self.reason)

        if logger_message:
            logger.critical("\n".join(logger_message))

        return table_exception
