# Dolphie
<p align="center">
<img src="https://user-images.githubusercontent.com/13244625/187600748-19d2ad15-42e8-4f9c-ada5-a153cdcf4070.png" width="120"><br>
An intuitive feature-rich top tool for monitoring MySQL in real time
</p>

## Installation
Requires Python 3.8+

Using PyPi:
```shell
pip install dolphie
```

Using Poetry:
```shell
curl -sSL https://install.python-poetry.org | python3 -

poetry install
```

## Using Docker container
1. `docker pull ghcr.io/charles-001/dolphie:latest`
2. `docker run -dit --name dolphie ghcr.io/charles-001/dolphie:latest`
3. `docker exec -it dolphie dolphie -h host.docker.internal -u root --ask-pass` (pass any additional parameters you'd like)

## Usage
```
options:
  --help                show this help message and exit
  -u USER, --user USER  Username for MySQL
  -p PASSWORD, --password PASSWORD
                        Password for MySQL
  --ask-pass            Ask for password (hidden text)
  -h HOST, --host HOST  Hostname/IP address for MySQL
  -P PORT, --port PORT  Port for MySQL (Socket has precendence)
  -S SOCKET, --socket SOCKET
                        Socket file for MySQL
  -c CONFIG_FILE, --config-file CONFIG_FILE
                        Absolute config file path to use. This should use [client] section. See below for options support [default: ~/.my.cnf]
  -f HOST_CACHE_FILE, --host-cache-file HOST_CACHE_FILE
                        Resolve IPs to hostnames when your DNS is unable to. Each IP/hostname pair should be on its own line using format: ip=hostname [default: None]
  -l LOGIN_PATH, --login-path LOGIN_PATH
                        Specify login path to use mysql_config_editor's file ~/.mylogin.cnf for encrypted login credentials. Supercedes config file [default: client]
  -r REFRESH_INTERVAL, --refresh_interval REFRESH_INTERVAL
                        How much time to wait in seconds between each refresh [default: 1]
  -R REFRESH_INTERVAL_INNODB_STATUS, --refresh_interval_innodb_status REFRESH_INTERVAL_INNODB_STATUS
                        How much time to wait in seconds to execute SHOW ENGINE INNODB STATUS to refresh data its responsible for [default: 1]
  -H HEARTBEAT_TABLE, --heartbeat-table HEARTBEAT_TABLE
                        If your hosts use pt-heartbeat, specify table in format db.table to use the timestamp it has for replication lag instead of Seconds_Behind_Master from SHOW SLAVE STATUS
  --ssl-mode SSL_MODE   Desired security state of the connection to the host. Supports: REQUIRED/VERIFY_CA/VERIFY_IDENTITY [default: OFF]
  --ssl-ca SSL_CA       Path to the file that contains a PEM-formatted CA certificate
  --ssl-cert SSL_CERT   Path to the file that contains a PEM-formatted client certificate
  --ssl-key SSL_KEY     Path to the file that contains a PEM-formatted private key for the client certificate
  --hide-dashboard      Start without showing dashboard. This is good to use if you want to reclaim terminal space and not execute the additional queries for it
  --show-trxs-only      Start with only showing queries that are running a transaction
  --additional-columns  Start with additional columns in processlist panel
  --use-processlist     Start with using Processlist instead of Performance Schema for listing queries
  -V, --version         Display version and exit

Config file with [client] section supports these options:
    host
    user
    password
    port
    socket
    ssl_mode REQUIRED/VERIFY_CA/VERIFY_IDENTITY
    ssl_ca
    ssl_cert
    ssl_key

Login path file supports these options:
    host
    user
    password
    port
    socket

Environment variables support these options:
    DOLPHIE_USER
    DOLPHIE_PASSWORD
    DOLPHIE_HOST
    DOLPHIE_PORT
    DOLPHIE_SOCKET

```

## Supported MySQL versions
- MySQL/Percona Server 5.5/5.6/5.7/8.0
- MariaDB 10+
- RDS/Aurora

## Grants required
#### Least privilege
1. PROCESS
2. SELECT to `performance_schema` (if used) + `pt-heartbeat table` (if used)
3. REPLICATION CLIENT

#### Recommended
1. PROCESS
2. Global SELECT access (good for explaining queries, listing all databases, etc)
4. REPLICATION CLIENT
5. SUPER (only required if you want to kill queries)

## Features
- Dolphie uses panels to present groups of data. They can all be turned on/off to have a view of your database server that you prefer (see Help screenshot for panels available)
- Prefers Performance Schema over Processlist if it's turned on for listing queries. Can be switched to use Processlist by pressing key "1" (or using parameter) since P_S can truncate query length for explaining queries
- 3 options for finding replica lag in this order of precedence:
  - `Performance Schema` (MySQL 8 only - most accurate, especially for multi-threaded replication)
  - `pt-heartbeat table` (specified by parameter)
  - `SHOW SLAVE STATUS`
- Host cache file. This provides users a way to specify hostnames for IPs when their network's DNS can't resolve them. An example use case for this is when you connect to your work's VPN and DNS isn't available to resolve IPs. In my opinion, it's a lot easier to look at hostnames than IPs!
- Supports encrypted login credentials via `mysql_config_editor`
- Automatic conversion of large numbers & bytes to human-readable
- Notifies when new version is available
- Many commands at your fingertips (see Help screenshot)
- Many valuable statisitics across the available panels that can help you troubleshoot issues and be proactive against them

## Things to note
Order of precedence for variables passed to Dolphie:
1. Command-line
2. Environment variables
3. ~/.mylogin.cnf (`mysql_config_editor`)
4. ~/.my.cnf

## Feedback
I welcome all questions, bug reports, and requests. If you enjoy Dolphie, please let me know! I'd love to hear from you :smiley:

## Dashboard with processlist
<img width="1699" alt="Screenshot 2023-07-06 at 5 52 02 AM" src="https://github.com/charles-001/dolphie/assets/13244625/9db68fa9-7020-40e9-9045-9e44deb300e8">

## Dashboard with InnoDB statisitics + processlist
<img width="1702" alt="Screenshot 2023-07-06 at 5 50 24 AM" src="https://github.com/charles-001/dolphie/assets/13244625/5dd0935e-0608-4f5b-b53e-90d1a5a4c069">

## Dashboard with replicas
<img width="1704" alt="Screenshot 2023-07-06 at 5 24 32 AM" src="https://github.com/charles-001/dolphie/assets/13244625/5234fef4-f141-4058-94fe-73c20d42133c">

## Explain query
<img width="1702" alt="Screenshot 2023-07-06 at 5 33 54 AM" src="https://github.com/charles-001/dolphie/assets/13244625/da4ea77f-c26f-449a-b959-280e88756981">

## Users list
<img width="1701" alt="Screenshot 2023-07-06 at 5 00 41 AM" src="https://github.com/charles-001/dolphie/assets/13244625/9f870376-7425-4bee-a81b-940193b84e6c">

## Variable wildcard searching
<img width="1701" alt="Screenshot 2023-07-06 at 5 01 06 AM" src="https://github.com/charles-001/dolphie/assets/13244625/43e84f80-a77d-415a-acff-207f520aa249">

## Help 
<img width="1695" alt="Screenshot 2023-07-06 at 5 01 39 AM" src="https://github.com/charles-001/dolphie/assets/13244625/37b3d87f-53db-4cbd-a160-94cd930f09aa">



