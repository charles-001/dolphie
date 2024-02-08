# Dolphie
<p align="center">
  <img src="https://user-images.githubusercontent.com/13244625/187600748-19d2ad15-42e8-4f9c-ada5-a153cdcf4070.png" width="120"><br>
  An intuitive feature-rich terminal tool for monitoring MySQL in real-time<br><br>
  <img src="https://github.com/charles-001/dolphie/assets/13244625/88a41290-f52c-4b8e-97f8-3b7ef5096eae" width="30">
  <img src="https://github.com/charles-001/dolphie/assets/13244625/1d94502a-9abf-4436-a7d0-cb2b08c105c1" width="30">
  <img src="https://github.com/charles-001/dolphie/assets/13244625/9b1aadc8-cabb-4256-92f9-fe4d04451b83" width="30">
</p>
<p align="center">
  <a href="https://www.buymeacoffee.com/charlesthompson">
    <img src="https://www.buymeacoffee.com/assets/img/custom_images/orange_img.png" alt="Buy Me A Coffee">
  </a>
</p>

![Untitled](https://github.com/charles-001/dolphie/assets/13244625/d1292ddc-146c-413c-8a15-9d0cc928ab50)
<p></p>
<img width="1498" alt="Screenshot 2024-02-05 at 6 05 46 AM" src="https://github.com/charles-001/dolphie/assets/13244625/d35ab166-dc80-4911-ab78-66b4085c01de">
<p></p>
<img width="1498" alt="Screenshot 2024-02-05 at 6 06 06 AM" src="https://github.com/charles-001/dolphie/assets/13244625/3afca5ce-f8e0-44a9-96a4-a5b27fb5d29b">
<p></p>
<img width="1500" alt="Screenshot 2024-02-05 at 6 07 41 AM" src="https://github.com/charles-001/dolphie/assets/13244625/5fbb21e2-a442-4d5d-8b40-f33546fb8b2e">
<p></p>
<img width="1496" alt="Screenshot 2024-02-05 at 6 08 17 AM" src="https://github.com/charles-001/dolphie/assets/13244625/afea5a1f-5b3e-454c-8b81-ba177c61dd56">
<p></p>
<img width="1500" alt="Screenshot 2024-02-05 at 6 14 14 AM" src="https://github.com/charles-001/dolphie/assets/13244625/27b57485-a6ad-44a6-8f9d-cfeed6a5dc65">
<p></p>
<img width="1503" alt="Screenshot 2024-02-05 at 6 08 36 AM" src="https://github.com/charles-001/dolphie/assets/13244625/89a02467-869c-45ca-9863-aaed333397f5">
<p></p>
<img width="1496" alt="Screenshot 2024-02-05 at 6 08 55 AM" src="https://github.com/charles-001/dolphie/assets/13244625/c707bfcd-f7d9-4868-a907-e7fb72929bd7">
<p></p>
<img width="1500" alt="Screenshot 2024-02-05 at 6 09 18 AM" src="https://github.com/charles-001/dolphie/assets/13244625/0164cac3-c55d-412f-841a-9b1ff0ea21c0">


## Installation
Requires Python 3.8+

#### Using PyPi
```shell
$ pip install dolphie
```

#### Using Poetry
```shell
$ curl -sSL https://install.python-poetry.org | python3 -

$ poetry install
```

#### Using Homebrew

If you are a [Homebrew](https://brew.sh/) user, you can install [dolphie](https://formulae.brew.sh/formula/dolphie) via

```sh
$ brew install dolphie
```

#### Using Docker

```sh
$ docker pull ghcr.io/charles-001/dolphie:latest
$ docker run -dit --name dolphie ghcr.io/charles-001/dolphie:latest
$ docker exec -it dolphie dolphie -h host.docker.internal -u root --ask-pass
```

## Usage
```
positional arguments:
  uri                   Use a URI string for credentials - format: mysql://user:password@host:port (port is optional with default 3306)

options:
  --help                show this help message and exit
  -u , --user           Username for MySQL
  -p , --password       Password for MySQL
  -h , --host           Hostname/IP address for MySQL
  -P , --port           Port for MySQL (Socket has precendence)
  -S , --socket         Socket file for MySQL
  --config-file         Dolphie's config file to use [default: ~/.dolphie]
  --mycnf-file          MySQL config file path to use. This should use [client] section. See below for options support [default: ~/.my.cnf]
  -f , --host-cache-file
                        Resolve IPs to hostnames when your DNS is unable to. Each IP/hostname pair should be on its own line using format: ip=hostname [default: ~/dolphie_host_cache]
  -q , --host-setup-file
                        Specify location of file that stores the available hosts to use in host setup modal [default: ~/dolphie_hosts]
  -l , --login-path     Specify login path to use mysql_config_editor's file ~/.mylogin.cnf for encrypted login credentials. Supercedes config file [default: client]
  -r , --refresh_interval
                        How much time to wait in seconds between each refresh [default: 1]
  -H , --heartbeat-table
                        If your hosts use pt-heartbeat, specify table in format db.table to use the timestamp it has for replication lag instead of Seconds_Behind_Master from SHOW SLAVE STATUS
  --ssl-mode            Desired security state of the connection to the host. Supports: REQUIRED/VERIFY_CA/VERIFY_IDENTITY [default: OFF]
  --ssl-ca              Path to the file that contains a PEM-formatted CA certificate
  --ssl-cert            Path to the file that contains a PEM-formatted client certificate
  --ssl-key             Path to the file that contains a PEM-formatted private key for the client certificate
  --panels              What panels to display on startup separated by a comma. Supports: dashboard/processlist/graphs/replication/locks/ddl [default: dashboard,processlist]
  --graph-marker        What marker to use for graphs (available options: https://tinyurl.com/dolphie-markers) [default: braille]
  --pypi-repository     What PyPi repository to use when checking for a new version. If not specified, it will use Dolphie's PyPi repository
  --show-trxs-only      Start with only showing threads that have an active transaction
  --additional-columns  Start with additional columns in Processlist panel
  --historical-locks    Always run the locks query so it can save historical data to its graph instead of only when the Locks panel is open. This query can be expensive in some environments
  -V, --version         Display version and exit

MySQL my.cnf file supports these options under [client] section:
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

Dolphie config file supports these options under [dolphie] section:
	(str) user
	(str) password
	(str) host
	(int) port
	(str) socket
	(str) ssl_mode
	(str) ssl_ca
	(str) ssl_cert
	(str) ssl_key
	(str) mycnf_file
	(str) login_path
	(str) host_cache_file
	(str) host_setup_file
	(int) refresh_interval
	(str) heartbeat_table
	(str) startup_panels
	(str) graph_marker
	(str) pypi_repository
	(bool) show_trxs_only
	(bool) show_additional_query_columns
	(bool) historical_locks
```

## Supported MySQL versions
- MySQL/Percona Server 5.6/5.7/8.0+
- RDS/Aurora

## Grants required
#### Least privilege
1. PROCESS (if you don't use `performance_schema`)
2. SELECT to `performance_schema` (if used) + `pt-heartbeat table` (if used)
3. REPLICATION CLIENT/REPLICATION SLAVE
4. BACKUP_ADMIN (MySQL 8 only)

#### Recommended
1. PROCESS (if you don't use `performance_schema`)
2. Global SELECT access (good for explaining queries, listing all databases, etc)
4. REPLICATION CLIENT/REPLICATION SLAVE
5. SUPER (required if you want to kill queries)
6. BACKUP_ADMIN (MySQL 8 only)

## Features
- Tabs at the top for multiple connections
- Dolphie uses panels to present groups of data. They can all be turned on/off to have a view of your database server that you prefer
- Graphs for many metrics that can give you great insight into how your database is performing
- Sparkline to show queries per second in a live view
- 2 options for finding replica lag in this order of precedence:
  - `pt-heartbeat table` (specified by `--heartbeat-table`)
  - `SHOW SLAVE STATUS`
- Keeps a history of the servers you connect to that provides autocompletion for hostnames in the Host Setup modal
- Host cache file. This provides users a way to specify hostnames for IPs when their network's DNS can't resolve them. An example use case for this is when you connect to your work's VPN and DNS isn't available to resolve IPs. In my opinion, it's a lot easier to look at hostnames than IPs!
- Supports encrypted login credentials via `mysql_config_editor`
- Automatic conversion of large numbers & bytes to human-readable
- Notifies when new version is available
- Many commands at your fingertips with autocompletion for their input


## Things to note
Order of precedence for variables passed to Dolphie:
1. Command-line
2. Environment variables
3. Dolphie's config file
4. ~/.mylogin.cnf (`mysql_config_editor`)
5. ~/.my.cnf

## Feedback
I welcome all questions, bug reports, and requests. If you enjoy Dolphie, please let me know! I'd love to hear from you :smiley:
