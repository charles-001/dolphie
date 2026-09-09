#!/bin/bash
set -euo pipefail

PRIMARY_HOST=172.28.4.2

mariadb -h"$PRIMARY_HOST" -uroot -proot test <<-EOSQL
    CREATE TABLE IF NOT EXISTS load_test (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        data VARCHAR(255) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
EOSQL

while true; do
    mariadb -h"$PRIMARY_HOST" -uroot -proot test -e \
        "INSERT INTO load_test (data) VALUES (MD5(RAND()))" 2>/dev/null

    if (( RANDOM % 5 == 0 )); then
        mariadb -h"$PRIMARY_HOST" -uroot -proot test -e \
            "INSERT INTO load_test (data) SELECT MD5(RAND()) FROM load_test LIMIT 50" 2>/dev/null
    fi

    if (( RANDOM % 3 == 0 )); then
        mariadb -h"$PRIMARY_HOST" -uroot -proot test -e \
            "UPDATE load_test SET data = MD5(RAND()) ORDER BY id DESC LIMIT 1" 2>/dev/null
    fi

    if (( RANDOM % 10 == 0 )); then
        mariadb -h"$PRIMARY_HOST" -uroot -proot test -e \
            "DELETE FROM load_test ORDER BY id LIMIT 100" 2>/dev/null
    fi

    sleep 0.5
done
