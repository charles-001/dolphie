#!/bin/bash
set -e

mysql -uroot -p"$MYSQL_ROOT_PASSWORD" <<-EOSQL
    CHANGE REPLICATION SOURCE TO
        SOURCE_HOST='172.28.1.2',
        SOURCE_USER='repl',
        SOURCE_PASSWORD='repl',
        SOURCE_AUTO_POSITION=1,
        GET_SOURCE_PUBLIC_KEY=1;
    START REPLICA;
    SET PERSIST super_read_only=ON;
EOSQL
