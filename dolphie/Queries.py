# MySQL queries used
Queries = {
    "pl_query": """
        /* dolphie */ SELECT
            id,
            IFNULL(User, "")                    AS user,
            IFNULL(Host, "")                    AS host,
            IFNULL(db, "")                      AS db,
            IFNULL(Command, "")                 As command,
            IFNULL(Time, "0")                   AS time,
            IFNULL(Info, "")                    AS query,
            IFNULL(State, "")                   AS state,
            IFNULL(trx_query, "")               AS trx_query,
            IFNULL(trx_state, "")               AS trx_state,
            IFNULL(trx_operation_state, "")     AS trx_operation_state,
            IFNULL(trx_rows_locked, "0")        AS trx_rows_locked,
            IFNULL(trx_rows_modified, "0")      AS trx_rows_modified,
            IFNULL(trx_concurrency_tickets, "") AS trx_concurrency_tickets
        FROM
            information_schema.PROCESSLIST pl
            LEFT JOIN information_schema.innodb_trx ON trx_mysql_thread_id = pl.Id
        WHERE 1 $placeholder
    """,
    "ps_query": """
        /* dolphie */ SELECT
            processlist_id                      AS id,
            IFNULL(processlist_user, "")        AS user,
            IFNULL(processlist_host, "")        AS host,
            IFNULL(processlist_db, "")          AS db,
            IFNULL(processlist_command, "")     As command,
            IFNULL(processlist_time, "0")       AS time,
            IFNULL(processlist_info, "")        AS query,
            IFNULL(processlist_state, "")       AS state,
            IFNULL(trx_query, "")               AS trx_query,
            IFNULL(trx_state, "")               AS trx_state,
            IFNULL(trx_operation_state, "")     AS trx_operation_state,
            IFNULL(trx_rows_locked, "0")        AS trx_rows_locked,
            IFNULL(trx_rows_modified, "0")      AS trx_rows_modified,
            IFNULL(trx_concurrency_tickets, "") AS trx_concurrency_tickets
        FROM
            performance_schema.threads t
            LEFT JOIN information_schema.innodb_trx tx ON trx_mysql_thread_id = t.processlist_id
        WHERE
            processlist_id IS NOT NULL AND
            processlist_time IS NOT NULL AND
            processlist_command != 'Daemon'
            $placeholder
    """,
    "locks_query-5": """
        /* dolphie */ SELECT
            IFNULL(r.trx_mysql_thread_id, "")                            AS waiting_thread,
            IFNULL(r.trx_query, "")                                      AS waiting_query,
            IFNULL(r.trx_rows_modified, "")                              AS waiting_rows_modified,
            IFNULL(TIMESTAMPDIFF(SECOND, r.trx_started, NOW()), "")      AS waiting_age,
            IFNULL(TIMESTAMPDIFF(SECOND, r.trx_wait_started, NOW()), "") AS waiting_wait_secs,
            IFNULL(b.trx_mysql_thread_id, "")                            AS blocking_thread,
            IFNULL(b.trx_query, "")                                      AS blocking_query,
            IFNULL(b.trx_rows_modified, "")                              AS blocking_rows_modified,
            IFNULL(TIMESTAMPDIFF(SECOND, b.trx_started, NOW()), "")      AS blocking_age,
            IFNULL(TIMESTAMPDIFF(SECOND, b.trx_wait_started, NOW()), "") AS blocking_wait_secs,
            IFNULL(lock_mode, "")                                        AS lock_mode,
            IFNULL(lock_type, "")                                        AS lock_type
        FROM
            INFORMATION_SCHEMA.INNODB_LOCK_WAITS w
            JOIN INFORMATION_SCHEMA.INNODB_TRX b   ON b.trx_id  = w.blocking_trx_id
            JOIN INFORMATION_SCHEMA.INNODB_TRX r   ON r.trx_id  = w.requesting_trx_id
            JOIN INFORMATION_SCHEMA.INNODB_LOCKS l ON l.lock_id = w.requested_lock_id
        ORDER BY
            TIMESTAMPDIFF(SECOND, r.trx_wait_started, NOW()) DESC
    """,
    "locks_query-8": """
        /* dolphie */ SELECT
            IFNULL(r.trx_mysql_thread_id, "")                            AS waiting_thread,
            IFNULL(r.trx_query, "")                                      AS waiting_query,
            IFNULL(r.trx_rows_modified, "")                              AS waiting_rows_modified,
            IFNULL(TIMESTAMPDIFF(SECOND, r.trx_started, NOW()), "")      AS waiting_age,
            IFNULL(TIMESTAMPDIFF(SECOND, r.trx_wait_started, NOW()), "") AS waiting_wait_secs,
            IFNULL(b.trx_mysql_thread_id, "")                            AS blocking_thread,
            IFNULL(b.trx_query, "")                                      AS blocking_query,
            IFNULL(b.trx_rows_modified, "")                              AS blocking_rows_modified,
            IFNULL(TIMESTAMPDIFF(SECOND, b.trx_started, NOW()), "")      AS blocking_age,
            IFNULL(TIMESTAMPDIFF(SECOND, b.trx_wait_started, NOW()), "") AS blocking_wait_secs,
            IFNULL(lock_mode, "")                                        AS lock_mode,
            IFNULL(lock_type, "")                                        AS lock_type
        FROM
            performance_schema.data_lock_waits w
            JOIN INFORMATION_SCHEMA.INNODB_TRX b ON b.trx_id         = w.blocking_engine_transaction_id
            JOIN INFORMATION_SCHEMA.INNODB_TRX r ON r.trx_id         = w.requesting_engine_transaction_id
            JOIN performance_schema.data_locks l ON l.engine_lock_id = w.requesting_engine_lock_id
        ORDER BY
            TIMESTAMPDIFF(SECOND, r.trx_wait_started, NOW()) DESC
    """,
    "ps_replica_lag": """
        /* dolphie */ SELECT
            IFNULL(TIMESTAMPDIFF(
                SECOND,
                MIN(APPLYING_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP),
            NOW()), "0") AS secs_behind
        FROM
            performance_schema.replication_applier_status_by_worker
        WHERE
            APPLYING_TRANSACTION != ''
    """,
    "heartbeat_replica_lag": """
        /* dolphie */ SELECT
            TIMESTAMPDIFF(SECOND, MAX(ts), NOW()) AS secs_behind
        FROM
            $placeholder
    """,
    "ps_find_replicas": """
        /* dolphie */ SELECT
            processlist_id   AS id,
            processlist_user AS user,
            processlist_host AS host
        FROM
            performance_schema.threads
        WHERE
            processlist_command LIKE 'Binlog Dump%'
    """,
    "pl_find_replicas": """
        /* dolphie */ SELECT
            Id   AS id,
            User AS user,
            Host AS host
        FROM
            information_schema.PROCESSLIST
        WHERE
            Command Like 'Binlog Dump%'
    """,
    "ps_user_statisitics": """
        /* dolphie */ SELECT
            u.user AS user,
            total_connections,
            current_connections,
            sum_rows_affected,
            sum_rows_sent,
            sum_rows_examined,
            sum_created_tmp_disk_tables,
            sum_created_tmp_tables
        FROM
            performance_schema.users u
            JOIN performance_schema.events_statements_summary_by_user_by_event_name ess ON u.user = ess.user
        WHERE
            current_connections != 0
        ORDER BY
            current_connections DESC
    """,
    "userstat_user_statisitics": """
        /* dolphie */ SELECT
            user,
            total_connections,
            concurrent_connections,
            denied_connections,
            binlog_bytes_written,
            rows_fetched,
            rows_updated,
            table_rows_read,
            select_commands,
            update_commands,
            other_commands,
            commit_transactions,
            rollback_transactions,
            access_denied
        FROM
            information_schema.user_statistics
        WHERE
            concurrent_connections != 0
        ORDER BY
            concurrent_connections DESC
    """,
    "status": "/* dolphie */ SHOW GLOBAL STATUS",
    "variables": "/* dolphie */ SHOW GLOBAL VARIABLES",
    "primary_status": "/* dolphie */ SHOW MASTER STATUS",
    "replica_status": "/* dolphie */ SHOW SLAVE STATUS",
    "databases": "/* dolphie */ SHOW DATABASES",
    "innodb_status": "/* dolphie */ SHOW ENGINE INNODB STATUS",
}
