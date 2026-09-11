#!/bin/bash
set -e

mariadb -uroot -p"$MARIADB_ROOT_PASSWORD" <<-EOSQL
    CHANGE MASTER TO
        MASTER_HOST='${MASTER_HOST}',
        MASTER_USER='root',
        MASTER_PASSWORD='${MARIADB_ROOT_PASSWORD}',
        MASTER_USE_GTID=slave_pos;
    START SLAVE;
EOSQL
