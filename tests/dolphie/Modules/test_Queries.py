from dolphie.Modules.Queries import MySQLQueries


def test_error_log_query_orders_newest_events_first() -> None:
    assert MySQLQueries.error_log.rstrip().endswith("timestamp DESC")
