#!/bin/bash
set -e

# Wait for the cluster to be ready
until mariadb -h172.28.0.2 -uroot -proot -e "SELECT 1" &>/dev/null; do
    sleep 2
done

# Create table
mariadb -h172.28.0.2 -uroot -proot test <<-EOSQL
    CREATE TABLE IF NOT EXISTS load_test (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        node VARCHAR(10),
        data VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
EOSQL

NODES=("172.28.0.2" "172.28.0.3" "172.28.0.4")

while true; do
    for node in "${NODES[@]}"; do
        mariadb -h"$node" -uroot -proot test -e \
            "INSERT INTO load_test (node, data) VALUES ('$node', MD5(RAND()))" 2>/dev/null &
    done
    wait

    # Occasional batch insert
    if (( RANDOM % 5 == 0 )); then
        node=${NODES[$((RANDOM % 3))]}
        mariadb -h"$node" -uroot -proot test -e \
            "INSERT INTO load_test (node, data) SELECT '$node', MD5(RAND()) FROM load_test LIMIT 50" 2>/dev/null
    fi

    # Occasional update
    if (( RANDOM % 3 == 0 )); then
        node=${NODES[$((RANDOM % 3))]}
        mariadb -h"$node" -uroot -proot test -e \
            "UPDATE load_test SET data = MD5(RAND()) WHERE id = (SELECT id FROM (SELECT id FROM load_test ORDER BY RAND() LIMIT 1) t)" 2>/dev/null
    fi

    # Occasional delete to keep table bounded
    if (( RANDOM % 10 == 0 )); then
        mariadb -h172.28.0.2 -uroot -proot test -e \
            "DELETE FROM load_test ORDER BY id LIMIT 100" 2>/dev/null
    fi

    sleep 0.5
done
