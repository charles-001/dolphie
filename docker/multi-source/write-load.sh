#!/bin/bash

# Wait for replication to be running
until mysql -h172.28.2.4 -uroot -proot -N -e "SELECT COUNT(*) FROM performance_schema.replication_connection_status WHERE SERVICE_STATE='ON'" 2>/dev/null | grep -q 2; do
    sleep 2
done

# Create tables on each primary
mysql -h172.28.2.2 -uroot -proot source_a -e "
    CREATE TABLE IF NOT EXISTS load_test (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        data VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
"
mysql -h172.28.2.3 -uroot -proot source_b -e "
    CREATE TABLE IF NOT EXISTS load_test (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        data VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
"

while true; do
    # Write to primary-a
    mysql -h172.28.2.2 -uroot -proot source_a -e \
        "INSERT INTO load_test (data) VALUES (MD5(RAND()))" 2>/dev/null

    # Write to primary-b
    mysql -h172.28.2.3 -uroot -proot source_b -e \
        "INSERT INTO load_test (data) VALUES (MD5(RAND()))" 2>/dev/null

    # Occasional batch insert on primary-a
    if (( RANDOM % 5 == 0 )); then
        mysql -h172.28.2.2 -uroot -proot source_a -e \
            "INSERT INTO load_test (data) SELECT MD5(RAND()) FROM load_test LIMIT 50" 2>/dev/null
    fi

    # Occasional batch insert on primary-b
    if (( RANDOM % 5 == 0 )); then
        mysql -h172.28.2.3 -uroot -proot source_b -e \
            "INSERT INTO load_test (data) SELECT MD5(RAND()) FROM load_test LIMIT 50" 2>/dev/null
    fi

    # Occasional update
    if (( RANDOM % 3 == 0 )); then
        mysql -h172.28.2.2 -uroot -proot source_a -e \
            "UPDATE load_test SET data = MD5(RAND()) WHERE id >= FLOOR(1 + RAND() * (SELECT MAX(id) FROM load_test)) LIMIT 1" 2>/dev/null
    fi

    # Occasional delete to keep tables bounded
    if (( RANDOM % 10 == 0 )); then
        mysql -h172.28.2.3 -uroot -proot source_b -e \
            "DELETE FROM load_test ORDER BY id LIMIT 100" 2>/dev/null
    fi

    sleep 0.5
done
