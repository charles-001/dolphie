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
            IFNULL(TIMESTAMPDIFF(SECOND, trx_started, NOW()), "") AS trx_time,
            "" AS connection_type
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
            IFNULL(TIMESTAMPDIFF(SECOND, trx_started, NOW()), "") AS trx_time,
            IFNULL(connection_type, "")         AS connection_type
        FROM
            performance_schema.threads t
            LEFT JOIN information_schema.innodb_trx tx ON trx_mysql_thread_id = t.processlist_id
        WHERE
            processlist_id IS NOT NULL AND
            processlist_time IS NOT NULL AND
            processlist_command != 'Daemon'
            $1
    """
    locks_query: str = """
        SELECT /*+ MAX_EXECUTION_TIME(10000) */
            wait_age,
            locked_type,
            waiting_pid,
            waiting_trx_age,
            waiting_trx_rows_modified,
            waiting_trx_rows_locked,
            waiting_lock_mode,
            IFNULL(waiting_query, "")  AS waiting_query,
            blocking_pid,
            blocking_trx_age,
            blocking_trx_rows_modified,
            blocking_trx_rows_locked,
            blocking_lock_mode,
            IFNULL(blocking_query, "") AS blocking_query
        FROM
            sys.innodb_lock_waits
    """
    ps_replica_lag: str = """
        SELECT MAX(`lag`) AS Seconds_Behind_Master
            FROM (
                SELECT MAX(TIMESTAMPDIFF(SECOND, APPLYING_TRANSACTION_IMMEDIATE_COMMIT_TIMESTAMP, NOW())) AS `lag`
                FROM performance_schema.replication_applier_status_by_worker

                UNION

                SELECT MIN(
                    CASE
                        WHEN
                            LAST_QUEUED_TRANSACTION = 'ANONYMOUS' OR
                            LAST_APPLIED_TRANSACTION = 'ANONYMOUS' OR
                            GTID_SUBTRACT(LAST_QUEUED_TRANSACTION, LAST_APPLIED_TRANSACTION) = ''
                            THEN 0
                        ELSE
                            TIMESTAMPDIFF(SECOND, LAST_APPLIED_TRANSACTION_IMMEDIATE_COMMIT_TIMESTAMP, NOW())
                    END
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
            t.THREAD_ID AS id,
            t.PROCESSLIST_USER AS user,
            t.PROCESSLIST_HOST AS host,
            CONVERT (
                CAST( CONVERT ( uvt.VARIABLE_VALUE USING latin1 ) AS BINARY ) USING utf8
            ) AS replica_uuid
        FROM
            `performance_schema`.threads AS t JOIN
            `performance_schema`.user_variables_by_thread AS uvt ON t.THREAD_ID = uvt.THREAD_ID
        WHERE
            t.PROCESSLIST_COMMAND LIKE 'Binlog Dump%'
            AND uvt.VARIABLE_NAME = 'slave_uuid'
    """
    pl_find_replicas: str = """
        SELECT
            Id   AS id,
            User AS user,
            Host AS host,
            '' AS replica_uuid
        FROM
            information_schema.PROCESSLIST
        WHERE
            Command Like 'Binlog Dump%'
    """
    ps_user_statisitics: str = """
        SELECT
            u.user AS user,
            ANY_VALUE(total_connections) AS total_connections,
            ANY_VALUE(current_connections) AS current_connections,
            CONVERT(SUM(sum_rows_affected), UNSIGNED) AS rows_affected,
            CONVERT(SUM(sum_rows_sent), UNSIGNED) AS rows_sent,
            CONVERT(SUM(sum_rows_examined), UNSIGNED) AS rows_examined,
            CONVERT(SUM(sum_created_tmp_disk_tables), UNSIGNED) AS created_tmp_disk_tables,
            CONVERT(SUM(sum_created_tmp_tables), UNSIGNED) AS created_tmp_tables,
            ANY_VALUE(plugin) AS plugin,
            ANY_VALUE(CASE
                WHEN (password_lifetime IS NULL OR password_lifetime = 0) AND @@default_password_lifetime = 0 THEN "N/A"
                ELSE CONCAT(
                    CAST(IFNULL(password_lifetime, @@default_password_lifetime) as signed) +
                    CAST(DATEDIFF(password_last_changed, NOW()) as signed),
                    " days"
                )
            END) AS password_expires_in
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
    ps_user_statisitics_56: str = """
        SELECT
            u.user AS user,
            total_connections,
            current_connections,
            CONVERT(SUM(sum_rows_affected), UNSIGNED) AS rows_affected,
            CONVERT(SUM(sum_rows_sent), UNSIGNED) AS rows_sent,
            CONVERT(SUM(sum_rows_examined), UNSIGNED) AS rows_examined,
            CONVERT(SUM(sum_created_tmp_disk_tables), UNSIGNED) AS created_tmp_disk_tables,
            CONVERT(SUM(sum_created_tmp_tables), UNSIGNED) AS created_tmp_tables,
            plugin
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
    user_thread_attributes: str = """
        SELECT
            ATTR_NAME,
            ATTR_VALUE
        FROM
            `performance_schema`.session_connect_attrs sca
            JOIN `performance_schema`.threads t ON sca.PROCESSLIST_ID = t.processlist_id
        WHERE
            t.processlist_id = $1
        ORDER BY
            ATTR_NAME
    """
    ddls: str = """
        SELECT
            t.processlist_id,
            ANY_VALUE(stmt.sql_text) AS sql_text,
            ANY_VALUE(stage.event_name) AS state,
            ANY_VALUE(CONCAT(ROUND(100 * stage.work_completed / stage.work_estimated, 2), "%")) AS percentage_completed,
            ANY_VALUE(stmt.timer_wait) AS started_ago,
            ANY_VALUE(CONVERT(stmt.timer_wait / ROUND(100 * stage.work_completed / stage.work_estimated, 2) * 100,
                UNSIGNED)) AS estimated_full_time,
            ANY_VALUE(CONVERT((stmt.timer_wait / ROUND(100 * stage.work_completed / stage.work_estimated, 2) * 100)
                - stmt.timer_wait, UNSIGNED)) AS estimated_remaining_time,
            CONVERT(SUM(`mt`.`CURRENT_NUMBER_OF_BYTES_USED`), UNSIGNED) AS memory
        FROM
            `performance_schema`.`events_statements_current` stmt JOIN
            `performance_schema`.`events_stages_current` stage ON stage.nesting_event_id = stmt.event_id JOIN
            `performance_schema`.`memory_summary_by_thread_by_event_name` `mt` ON `mt`.thread_id = stmt.thread_id JOIN
            `performance_schema`.`threads` t ON t.thread_id = stmt.thread_id
        WHERE
            stage.event_name LIKE 'stage/innodb/alter%'
        GROUP BY
            t.processlist_id
    """
    metadata_locks: str = """
        SELECT
            ANY_VALUE(OBJECT_INSTANCE_BEGIN) AS id,
            OBJECT_TYPE,
            ANY_VALUE(OBJECT_SCHEMA) AS OBJECT_SCHEMA,
            GROUP_CONCAT(OBJECT_NAME ORDER BY OBJECT_NAME) AS OBJECT_NAME,
            LOCK_TYPE,
            LOCK_STATUS,
            ANY_VALUE(SOURCE) AS CODE_SOURCE,
            ANY_VALUE(NAME) AS THREAD_SOURCE,
            ANY_VALUE(PROCESSLIST_ID) AS PROCESSLIST_ID,
            ANY_VALUE(PROCESSLIST_USER) AS PROCESSLIST_USER,
            ANY_VALUE(PROCESSLIST_TIME) AS PROCESSLIST_TIME,
            ANY_VALUE(PROCESSLIST_INFO) AS PROCESSLIST_INFO
        FROM
            `performance_schema`.`metadata_locks` mlb JOIN
            `performance_schema`.`threads` t ON mlb.OWNER_THREAD_ID = t.THREAD_ID
        WHERE
            NOT (
                OBJECT_TYPE = 'TABLE' AND
                LOCK_STATUS = 'GRANTED' AND
                LOCK_TYPE LIKE 'SHARED%' AND
                PROCESSLIST_TIME <= 2
            ) AND
            OBJECT_TYPE != 'COLUMN STATISTICS'
            $1
        GROUP BY
            THREAD_ID,
            OBJECT_TYPE,
            LOCK_TYPE,
            LOCK_STATUS
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
        ORDER BY
            timestamp
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
            sys.format_bytes( SUM(current_alloc) ) AS current_allocated
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
        WHERE
            name IN ('adaptive_hash_searches', 'adaptive_hash_searches_btree', 'trx_rseg_history_len')
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
            ANY_VALUE(FORMAT_PICO_TIME(
                (applier_status.LAST_APPLIED_TRANSACTION_END_APPLY_TIMESTAMP -
                applier_status.LAST_APPLIED_TRANSACTION_START_APPLY_TIMESTAMP) * 1000000000000
            )) AS apply_time,
            ANY_VALUE(applier_status.LAST_APPLIED_TRANSACTION) AS last_applied_transaction,
            CONVERT(SUM(thread_events.COUNT_STAR), UNSIGNED) AS total_thread_events
        FROM
            `performance_schema`.replication_applier_status_by_worker applier_status JOIN
            `performance_schema`.events_transactions_summary_by_thread_by_event_name thread_events USING (THREAD_ID)
        WHERE
            applier_status.THREAD_ID IN (
                SELECT THREAD_ID FROM `performance_schema`.replication_applier_status_by_worker
            )
        GROUP BY
            worker_id
        WITH ROLLUP
        ORDER BY
            worker_id
    """

    # Group Replication Event Horizon and Protocol
    group_replication_get_write_concurrency: str = """
        SELECT
            group_replication_get_write_concurrency() write_concurrency,
            group_replication_get_communication_protocol() protocol_version
    """
    get_group_replication_members: str = """
        SELECT
            *
        FROM
            performance_schema.replication_group_members LEFT JOIN
            performance_schema.replication_group_member_stats USING(MEMBER_ID)
    """
    replicaset_find_replicas: str = """
        SELECT
            instance_id as id,
            address AS host,
            attributes ->> '$."replicationAccountUser"' AS user
        FROM
            mysql_innodb_cluster_metadata.instances
        WHERE
            mysql_server_uuid != @@server_uuid
    """
    determine_cluster_type_8: str = """
        SELECT
            cluster_type
        FROM
            mysql_innodb_cluster_metadata.clusters
            JOIN mysql_innodb_cluster_metadata.instances USING ( cluster_id )
        WHERE
            mysql_server_uuid = @@server_uuid
    """
    determine_cluster_type_81: str = """
        SELECT
            instance_type,
            cluster_type
        FROM
            mysql_innodb_cluster_metadata.clusters
            JOIN mysql_innodb_cluster_metadata.instances USING ( cluster_id )
            LEFT JOIN mysql_innodb_cluster_metadata.clusterset_members USING ( cluster_id )
        WHERE
            mysql_server_uuid = @@server_uuid
        ORDER BY
            view_id DESC
            LIMIT 1;
    """
    get_binlog_transaction_compression_percentage: str = """
        SELECT
            compression_percentage
        FROM
            performance_schema.binary_log_transaction_compression_stats
        WHERE
            log_type = 'BINARY' AND
            compression_type = 'ZSTD'
        LIMIT
            1
    """
    status: str = "SHOW GLOBAL STATUS"
    variables: str = "SHOW GLOBAL VARIABLES"
    binlog_status: str = "SHOW MASTER STATUS"
    replication_status: str = "SHOW SLAVE STATUS"
    innodb_status: str = "SHOW ENGINE INNODB STATUS"
    get_replicas: str = "SHOW SLAVE HOSTS"
