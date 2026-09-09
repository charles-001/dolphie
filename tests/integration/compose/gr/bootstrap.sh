#!/bin/bash

# Nodes are already healthy via depends_on service_healthy

# Reset binary logs to clear GTIDs from MySQL initialization (parallel)
for host in 172.28.1.2 172.28.1.3 172.28.1.4; do
    mysql -h"$host" -uroot -proot -e "RESET BINARY LOGS AND GTIDS;" 2>/dev/null &
done
wait

# Setup replication user and recovery channel on ALL nodes (parallel)
for host in 172.28.1.2 172.28.1.3 172.28.1.4; do
    mysql -h"$host" -uroot -proot <<-EOSQL &
        SET SQL_LOG_BIN=0;
        CREATE USER IF NOT EXISTS 'repl'@'%' IDENTIFIED BY 'repl';
        GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%';
        GRANT CONNECTION_ADMIN ON *.* TO 'repl'@'%';
        GRANT BACKUP_ADMIN ON *.* TO 'repl'@'%';
        GRANT GROUP_REPLICATION_STREAM ON *.* TO 'repl'@'%';
        FLUSH PRIVILEGES;
        SET SQL_LOG_BIN=1;

        CHANGE REPLICATION SOURCE TO
            SOURCE_USER='repl',
            SOURCE_PASSWORD='repl'
            FOR CHANNEL 'group_replication_recovery';
EOSQL
done
wait

# Bootstrap primary
mysql -h172.28.1.2 -uroot -proot -e "
    SET GLOBAL group_replication_bootstrap_group=ON;
    START GROUP_REPLICATION;
    SET GLOBAL group_replication_bootstrap_group=OFF;
"

# Wait for primary to be ONLINE
until mysql -h172.28.1.2 -uroot -proot -N -e "SELECT MEMBER_STATE FROM performance_schema.replication_group_members WHERE MEMBER_HOST='172.28.1.2'" 2>/dev/null | grep -q ONLINE; do
    sleep 0.5
done

# Start both secondaries simultaneously
for host in 172.28.1.3 172.28.1.4; do
    mysql -h"$host" -uroot -proot -e "START GROUP_REPLICATION;" &
done
wait

# Wait for all 3 to be ONLINE
until [ "$(mysql -h172.28.1.2 -uroot -proot -N -e "SELECT COUNT(*) FROM performance_schema.replication_group_members WHERE MEMBER_STATE='ONLINE'" 2>/dev/null)" = "3" ]; do
    sleep 0.5
done
echo "All 3 nodes ONLINE"

exec sleep infinity
