from dolphie.Modules.Queries import MySQLQueries


def test_error_log_query_orders_newest_events_first() -> None:
    assert MySQLQueries.error_log.rstrip().endswith("timestamp DESC")


def test_clusterset_role_uses_compatible_primary_cluster_metadata() -> None:
    assert "primary_cluster = 1" in MySQLQueries.determine_cluster_type_81
    assert "member_role AS clusterset_role" not in MySQLQueries.determine_cluster_type_81
