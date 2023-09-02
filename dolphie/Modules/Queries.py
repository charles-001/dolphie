from dataclasses import dataclass


@dataclass
class MySQLQueries:
    pl_query: str = """
        SELECT
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
            IFNULL(trx_concurrency_tickets, "") AS trx_concurrency_tickets,
            IFNULL(TIMESTAMPDIFF(SECOND, trx_started, NOW()), "") AS trx_time
        FROM
            information_schema.PROCESSLIST pl
            LEFT JOIN information_schema.innodb_trx ON trx_mysql_thread_id = pl.Id
        WHERE
            command != 'Daemon'
            $1
    """

    ps_query: str = """
        SELECT
            processlist_id                      AS id,
            IFNULL(thread_id, "0")              AS mysql_thread_id,
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
            IFNULL(trx_concurrency_tickets, "") AS trx_concurrency_tickets,
            IFNULL(TIMESTAMPDIFF(SECOND, trx_started, NOW()), "") AS trx_time
        FROM
            performance_schema.threads t
            LEFT JOIN information_schema.innodb_trx tx ON trx_mysql_thread_id = t.processlist_id
        WHERE
            processlist_id IS NOT NULL AND
            processlist_time IS NOT NULL AND
            processlist_command != 'Daemon'
            $1
    """
    ps_replica_lag: str = """
        SELECT MAX(`lag`) AS Seconds_Behind_Master
            FROM (
                SELECT MAX(TIMESTAMPDIFF(SECOND, APPLYING_TRANSACTION_IMMEDIATE_COMMIT_TIMESTAMP, NOW())) AS `lag`
                FROM performance_schema.replication_applier_status_by_worker

                UNION

                SELECT MIN(
                    IF(
                        GTID_SUBTRACT(LAST_QUEUED_TRANSACTION, LAST_APPLIED_TRANSACTION) = '',
                        0,
                        TIMESTAMPDIFF(SECOND, LAST_APPLIED_TRANSACTION_IMMEDIATE_COMMIT_TIMESTAMP, NOW())
                    )
                ) AS `lag`
                FROM performance_schema.replication_applier_status_by_worker w
                JOIN performance_schema.replication_connection_status s ON s.channel_name = w.channel_name
            ) required
    """
    ps_disk_io: str = """
        SELECT
            CONVERT(SUM(SUM_NUMBER_OF_BYTES_READ), UNSIGNED) AS io_read,
            CONVERT(SUM(SUM_NUMBER_OF_BYTES_WRITE), UNSIGNED) AS io_write
        FROM
            `performance_schema`.`file_summary_by_event_name`
        WHERE
            `performance_schema`.`file_summary_by_event_name`.`EVENT_NAME` LIKE 'wait/io/file/%' AND
            `performance_schema`.`file_summary_by_event_name`.`COUNT_STAR` > 0
    """
    heartbeat_replica_lag: str = """
        SELECT
            TIMESTAMPDIFF(SECOND, MAX(ts), NOW()) AS Seconds_Behind_Master
        FROM
            $1
    """
    ps_find_replicas: str = """
        SELECT
            processlist_id   AS id,
            processlist_user AS user,
            processlist_host AS host
        FROM
            performance_schema.threads
        WHERE
            processlist_command LIKE 'Binlog Dump%'
    """
    pl_find_replicas: str = """
        SELECT
            Id   AS id,
            User AS user,
            Host AS host
        FROM
            information_schema.PROCESSLIST
        WHERE
            Command Like 'Binlog Dump%'
    """
    ps_user_statisitics: str = """
        SELECT
            u.user AS user,
            total_connections,
            current_connections,
            CONVERT(SUM(sum_rows_affected), UNSIGNED) AS sum_rows_affected,
            CONVERT(SUM(sum_rows_sent), UNSIGNED) AS sum_rows_sent,
            CONVERT(SUM(sum_rows_examined), UNSIGNED) AS sum_rows_examined,
            CONVERT(SUM(sum_created_tmp_disk_tables), UNSIGNED) AS sum_created_tmp_disk_tables,
            CONVERT(SUM(sum_created_tmp_tables), UNSIGNED) AS sum_created_tmp_tables,
            plugin,
            CASE
                WHEN (password_lifetime IS NULL OR password_lifetime = 0) AND @@default_password_lifetime = 0 THEN "N/A"
                ELSE CONCAT(
                    CAST(IFNULL(password_lifetime, @@default_password_lifetime) as signed) +
                    CAST(DATEDIFF(password_last_changed, NOW()) as signed),
                    " days"
                )
            END AS password_expires_in
        FROM
            performance_schema.users u
            JOIN performance_schema.events_statements_summary_by_user_by_event_name ess ON u.user = ess.user
            JOIN mysql.user mysql_user ON mysql_user.user = u.user
        WHERE
            current_connections != 0
        GROUP BY
            user
        ORDER BY
            current_connections DESC
    """
    error_log: str = """
        SELECT
            logged AS timestamp,
            prio AS level,
            subsystem,
            data AS message
        FROM
            performance_schema.error_log
        WHERE
            data != 'Could not open log file.'
            $1
        ORDER BY timestamp
    """
    memory_by_user: str = """
        SELECT
            user,
            current_allocated,
            total_allocated
        FROM
            sys.memory_by_user_by_current_bytes
        WHERE
            user != "background"
    """
    memory_by_code_area: str = """
        SELECT
            SUBSTRING_INDEX( event_name, '/', 2 ) AS code_area,
            sys.format_bytes (
            SUM( current_alloc )) AS current_allocated
        FROM
            sys.x$memory_global_by_current_bytes
        GROUP BY
            SUBSTRING_INDEX( event_name, '/', 2 )
        ORDER BY
            SUM( current_alloc ) DESC
    """
    memory_by_host: str = """
        SELECT
            host,
            current_allocated,
            total_allocated
        FROM
            sys.memory_by_host_by_current_bytes
        WHERE
            host != "background"
    """
    databases: str = """
        SELECT
            SCHEMA_NAME
        FROM
            information_schema.SCHEMATA
        ORDER BY
            SCHEMA_NAME
    """
    innodb_metrics: str = """
        SELECT
            NAME,
            COUNT
        FROM
            information_schema.INNODB_METRICS
    """
    checkpoint_age: str = """
        SELECT
            STORAGE_ENGINES ->> '$."InnoDB"."LSN"' - STORAGE_ENGINES ->> '$."InnoDB"."LSN_checkpoint"' AS checkpoint_age
        FROM
            performance_schema.log_status
    """
    active_redo_logs: str = """
        SELECT
            COUNT(*) AS count
        FROM
            performance_schema.file_instances
        WHERE
            file_name LIKE '%innodb_redo/%' AND
            file_name NOT LIKE '%_tmp'
    """
    thread_transaction_history: str = """
        SELECT
            DATE_SUB(
                NOW(),
                INTERVAL (
                    SELECT variable_value
                    FROM performance_schema.global_status
                    WHERE variable_name = 'UPTIME'
                ) - TIMER_START * 10e-13 SECOND
            ) AS start_time,
            sql_text
        FROM
            performance_schema.events_statements_history
        WHERE
            nesting_event_id = (
                SELECT EVENT_ID
                FROM performance_schema.events_transactions_current t
                WHERE t.thread_id = $1
            )
        ORDER BY
            event_id;
    """
    replication_applier_status: str = """
        SELECT
            worker_id,
            FORMAT_PICO_TIME(
                (applier_status.LAST_APPLIED_TRANSACTION_END_APPLY_TIMESTAMP -
                applier_status.LAST_APPLIED_TRANSACTION_START_APPLY_TIMESTAMP) * 1000000000000
            ) AS apply_time,
            applier_status.LAST_APPLIED_TRANSACTION AS last_applied_transaction,
            CONVERT(SUM(thread_events.COUNT_STAR), UNSIGNED) AS total_thread_events
        FROM
            `performance_schema`.replication_applier_status_by_worker applier_status
        JOIN
            `performance_schema`.events_transactions_summary_by_thread_by_event_name thread_events ON
            applier_status.THREAD_ID = thread_events.THREAD_ID
        WHERE
            applier_status.THREAD_ID IN (
                SELECT THREAD_ID FROM `performance_schema`.replication_applier_status_by_worker
            )
        GROUP BY worker_id
        WITH ROLLUP
        ORDER BY worker_id
    """
    group_replication_member_status: str = """
        SELECT
            member_role
        FROM
            performance_schema.replication_group_members
        WHERE
            member_id = '$1'
    """
    
    # Group Replication Event Horizon and Protocol
    group_replication_get_write_concurrency: str = """
        SELECT group_replication_get_write_concurrency() eh,
               group_replication_get_communication_protocol() protocol 
    """

    get_group_members: str = "SELECT * FROM performance_schema.replication_group_members"

    status: str = "SHOW GLOBAL STATUS"
    variables: str = "SHOW GLOBAL VARIABLES"
    binlog_status: str = "SHOW MASTER STATUS"
    replication_status: str = "SHOW SLAVE STATUS"
    innodb_status: str = "SHOW ENGINE INNODB STATUS"
    get_replicas: str = "SHOW SLAVE HOSTS"
