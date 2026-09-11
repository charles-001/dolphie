#!/bin/bash

# Create replication user on both primaries (parallel)
for host in 172.28.3.2 172.28.3.3; do
    mysql -h"$host" -uroot -proot <<-EOSQL &
        RESET BINARY LOGS AND GTIDS;
        SET SQL_LOG_BIN=0;
        CREATE USER IF NOT EXISTS 'repl'@'%' IDENTIFIED BY 'repl';
        GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%';
        FLUSH PRIVILEGES;
        SET SQL_LOG_BIN=1;
EOSQL
done
wait

# Create separate databases on each primary
mysql -h172.28.3.2 -uroot -proot -e "CREATE DATABASE IF NOT EXISTS source_a;"
mysql -h172.28.3.3 -uroot -proot -e "CREATE DATABASE IF NOT EXISTS source_b;"

# Wait for the replica to be ready
until mysql -h172.28.3.4 -uroot -proot -e "SELECT 1" 2>/dev/null; do
    sleep 1
done

# Reset replica's binary logs
mysql -h172.28.3.4 -uroot -proot -e "RESET BINARY LOGS AND GTIDS;" 2>/dev/null

# Configure multi-source replication channels on the replica
mysql -h172.28.3.4 -uroot -proot <<-EOSQL
    CHANGE REPLICATION SOURCE TO
        SOURCE_HOST='172.28.3.2',
        SOURCE_USER='repl',
        SOURCE_PASSWORD='repl',
        SOURCE_AUTO_POSITION=1,
        GET_SOURCE_PUBLIC_KEY=1
        FOR CHANNEL 'channel_a';

    CHANGE REPLICATION SOURCE TO
        SOURCE_HOST='172.28.3.3',
        SOURCE_USER='repl',
        SOURCE_PASSWORD='repl',
        SOURCE_AUTO_POSITION=1,
        GET_SOURCE_PUBLIC_KEY=1
        FOR CHANNEL 'channel_b';

    START REPLICA FOR CHANNEL 'channel_a';
    START REPLICA FOR CHANNEL 'channel_b';
EOSQL

echo "Multi-source replication configured (channel_a -> primary-a, channel_b -> primary-b)"

exec sleep infinity
