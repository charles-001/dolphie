#!/bin/bash
set -euo pipefail

PRIMARY_HOST=172.28.4.2
REPLICA_HOSTS=(172.28.4.3 172.28.4.4)

mariadb -h"$PRIMARY_HOST" -uroot -proot <<-EOSQL
    CREATE USER IF NOT EXISTS 'repl'@'%' IDENTIFIED BY 'repl';
    GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%';
    FLUSH PRIVILEGES;
EOSQL

for host in "${REPLICA_HOSTS[@]}"; do
    mariadb -h"$host" -uroot -proot -e "STOP ALL SLAVES;" 2>/dev/null || true
    mariadb -h"$host" -uroot -proot <<-EOSQL
        RESET SLAVE ALL;
        CHANGE MASTER TO
            MASTER_HOST='$PRIMARY_HOST',
            MASTER_PORT=3306,
            MASTER_USER='repl',
            MASTER_PASSWORD='repl',
            MASTER_USE_GTID=slave_pos,
            MASTER_CONNECT_RETRY=1;
        START SLAVE;
EOSQL
done

for host in "${REPLICA_HOSTS[@]}"; do
    until mariadb -h"$host" -uroot -proot -e "SHOW SLAVE STATUS\\G" 2>/dev/null |
        grep -q "Slave_IO_Running: Yes"; do
        sleep 1
    done

    until mariadb -h"$host" -uroot -proot -e "SHOW SLAVE STATUS\\G" 2>/dev/null |
        grep -q "Slave_SQL_Running: Yes"; do
        sleep 1
    done
done

# A separate GTID domain prevents the local fixture from conflicting with the
# primary's sequence while still producing an errant GTID from server_id 103.
mariadb -h172.28.4.4 -uroot -proot -e \
    "SET SESSION gtid_domain_id=103; CREATE DATABASE IF NOT EXISTS errant_transaction;"

touch /tmp/replication-ready
exec sleep infinity
