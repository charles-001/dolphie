#!/bin/bash

# Find current primary, trying all known primary cluster nodes
get_primary() {
    for host in 172.28.2.2 172.28.2.3 172.28.2.4; do
        result=$(mysql -h"$host" -uroot -proot -N -e "
            SELECT MEMBER_HOST
            FROM performance_schema.replication_group_members
            WHERE MEMBER_ROLE='PRIMARY' LIMIT 1" 2>/dev/null) && [ -n "$result" ] && echo "$result" && return
    done
}

# Wait for GR to be fully online (3 members)
until [ "$(get_primary)" != "" ]; do
    sleep 2
done

PRIMARY_HOST=$(get_primary)

# Create database and table on primary
mysql -h"$PRIMARY_HOST" -uroot -proot -e "
    CREATE DATABASE IF NOT EXISTS test;
"
mysql -h"$PRIMARY_HOST" -uroot -proot test -e "
    CREATE TABLE IF NOT EXISTS load_test (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        node VARCHAR(50),
        data VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
"

while true; do
    PRIMARY_HOST=$(get_primary)
    if [ -z "$PRIMARY_HOST" ]; then
        sleep 2
        continue
    fi

    mysql -h"$PRIMARY_HOST" -uroot -proot test -e \
        "INSERT INTO load_test (node, data) VALUES ('$PRIMARY_HOST', MD5(RAND()))" 2>/dev/null

    # Occasional batch insert
    if (( RANDOM % 5 == 0 )); then
        mysql -h"$PRIMARY_HOST" -uroot -proot test -e \
            "INSERT INTO load_test (node, data) SELECT '$PRIMARY_HOST', MD5(RAND()) FROM load_test LIMIT 50" 2>/dev/null
    fi

    # Occasional update (random row via MAX(id) instead of ORDER BY RAND)
    if (( RANDOM % 3 == 0 )); then
        mysql -h"$PRIMARY_HOST" -uroot -proot test -e \
            "UPDATE load_test SET data = MD5(RAND()) WHERE id >= FLOOR(1 + RAND() * (SELECT MAX(id) FROM load_test)) LIMIT 1" 2>/dev/null
    fi

    # Occasional delete to keep table bounded
    if (( RANDOM % 10 == 0 )); then
        mysql -h"$PRIMARY_HOST" -uroot -proot test -e \
            "DELETE FROM load_test ORDER BY id LIMIT 100" 2>/dev/null
    fi

    sleep 0.5
done
