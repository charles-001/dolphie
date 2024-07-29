# Dolphie
<p align="center">
  <img src="https://user-images.githubusercontent.com/13244625/187600748-19d2ad15-42e8-4f9c-ada5-a153cdcf4070.png" width="120"><br>
  Your single pane of glass for real-time analytics into MySQL/MariaDB & ProxySQL<br><br>
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
<img width="1558" alt="Screenshot 2024-02-12 at 8 52 32 AM" src="https://github.com/charles-001/dolphie/assets/13244625/8d6b98f5-0d0a-47b2-a538-9255ae46c393">
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
  uri                   Use a URI string for credentials (mysql/proxysql) - format: mysql://user:password@host:port (port is optional with default 3306, or 6032 for ProxySQL)

options:
  --help                show this help message and exit
  --host-setup          Start Dolphie by showing the Host Setup modal instead of automatically connecting
  -u , --user           Username
  -p , --password       Password
  -h , --host           Hostname/IP address
  -P , --port           Port (socket has precedence)
  -S , --socket         Socket file
  --config-file         Dolphie's config file to use. Options are read from these files in the given order: ['/etc/dolphie.cnf', '~/.dolphie.cnf']
  --mycnf-file          MySQL config file path to use. This should use [client] section [default: ~/.my.cnf]
  -f , --host-cache-file
                        Resolve IPs to hostnames when your DNS is unable to. Each IP/hostname pair should be on its own line using format ip=hostname [default: ~/dolphie_host_cache]
  -q , --host-setup-file
                        Specify location of file that stores the available hosts to use in host setup modal [default: ~/dolphie_hosts]
  -l , --login-path     Specify login path to use with mysql_config_editor's file ~/.mylogin.cnf for encrypted login credentials [default: client]
  -r , --refresh_interval
                        How much time to wait in seconds between each refresh [default: 1]
  -H , --heartbeat-table
                        (MySQL only) If your hosts use pt-heartbeat, specify table in format db.table to use the timestamp it has for replication lag instead of Seconds_Behind_Master from SHOW REPLICA STATUS
  --ssl-mode            Desired security state of the connection to the host. Supports: REQUIRED/VERIFY_CA/VERIFY_IDENTITY [default: OFF]
  --ssl-ca              Path to the file that contains a CA (certificate authority)
  --ssl-cert            Path to the file that contains a certificate
  --ssl-key             Path to the file that contains a private key for the certificate
  --panels              What panels to display on startup separated by a comma. Supports:  dashboard,processlist,graphs,replication,metadata_locks,ddl,proxysql_hostgroup_summary,proxysql_mysql_query_rules,proxysql_command_stats [default: dashboard,processlist]
  --graph-marker        What marker to use for graphs (available options: https://tinyurl.com/dolphie-markers) [default: braille]
  --pypi-repository     What PyPi repository to use when checking for a new version. If not specified, it will use Dolphie's PyPi repository
  --hostgroup           This is used for creating tabs and connecting to them for hosts you specify in Dolphie's config file under a hostgroup section. As an example, you'll have a section called [cluster1] then below it you will list each host on a new line in the format key=host (keys have no meaning). Hosts support optional port (default is whatever port parameter is) in the format host:port. You can also name the tabs by suffixing ~tab_name to the host (i.e. 1=host~tab_name)
  --replay-file         Specify the full path of sqlite database file to replay data from
  --replay-dir          Directory of the sqlite database file to save replay data to for later playback. Note, this will enable replays and can use up a lot of disk space
  --replay-retention-hours
                        Number of hours to keep replay data [default: 48]
  --daemon              Starts Dolphie in daemon mode so replays are readily available. This will not show the TUI and should be put into the background with whatever solution you decide to use
  --debug-daemon        Enables debug mode for daemon mode. This will show additional information in the logs
  --daemon-log-file     Full path of the log file for daemon mode
  --show-trxs-only      (MySQL only) Start with only showing threads that have an active transaction
  --additional-columns  Start with additional columns in Processlist panel
  --debug-options       Display options that are set and what they're set by (command-line, dolphie config, etc) then exit. WARNING: This will show passwords and other sensitive information in plain text
  -V, --version         Display version and exit

Order of precedence for methods that pass options to Dolphie:
	1. Command-line
	2. Environment variables
	3. Dolphie's config (set by --config-file)
	4. ~/.mylogin.cnf (mysql_config_editor)
	5. ~/.my.cnf (set by --mycnf-file)

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

Dolphie's config supports these options under [dolphie] section:
	(bool) host_setup
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
	(str) hostgroup
	(bool) show_trxs_only
	(bool) show_additional_query_columns
	(bool) daemon_mode
	(bool) daemon_mode_debug
	(str) daemon_mode_log_file
	(str) replay_file
	(str) replay_dir
	(int) replay_retention_hours
```
## Supported ProxySQL versions
- ProxySQL 2.6+ (could work on previous versions but not tested)

Note: Use `admin` user instead of `stats` user so you can use all features

## Supported MariaDB versions
- MariaDB 5.5/10.0/11.0+
- RDS MariaDB

## Supported MySQL versions
- MySQL/Percona Server 5.6/5.7/8.0+/8.4+
- RDS MySQL & Aurora + Azure

## MySQL Grants required
#### Least privilege
1. PROCESS (only if you switch to using processlist via `P` command)
2. SELECT to `performance_schema` + `pt-heartbeat table` (if used)
3. REPLICATION CLIENT/REPLICATION SLAVE
4. BACKUP_ADMIN (MySQL 8 only)

#### Recommended
1. PROCESS (only if you switch to using processlist via `P` command)
2. Global SELECT access (good for explaining queries, listing all databases, etc)
4. REPLICATION CLIENT/REPLICATION SLAVE
5. SUPER (required if you want to kill queries)
6. BACKUP_ADMIN (MySQL 8 only)

## Replay
Have you ever wished you could view the process list from a specific moment in time? Perhaps during a database stall that led to an incident, and your monitoring tools failed to identify the root cause? Well, you're in luck! Dolphie introduces a powerful Replay feature that lets you do just that.

Starting with version 5.1.0, you can instruct Dolphie to record its data into a local SQLite database file that's compressed with ZSTD by using the `--relay-dir` option. When you're ready to replay this data, simply pass the `--replay-file` option, and you can interact with it as if you were watching it live! Within the Replay interface, you can navigate with the controls: back, forward, play/pause, or seek to a specific datetime. While some features are disabled during replay, the essential functionalities remain intact. For a complete list of available commands, just press `?` to access the help menu.

## Daemon Mode
Also introduced in version 5.1.0 is the ability to run Dolphie in daemon mode using the `--daemon` option. This mode does not load the TUI and creates a log file for messages, focusing solely on recording data to a replay file. You can manage the amount of retained data by setting the `--replay-retention-hours` option. It’s up to you how you’d like to run Dolphie in the background; I personally prefer using `systemctl`, but you can also utilize `nohup`, `tmux`, etc. If Dolphie gets disconnected, it will attempt to reconnect 3 times before exiting.

## Hostgroups
Hostgroups are a way to easily connect to multiple hosts at once. To set this up, you will create a section in Dolphie's config file with the name you want the hostgroup to be and list each host on a new line in the format `key=host` (keys have no meaning). Hosts support optional port (default is whatever `port` parameter is) in the format `host:port`. You can also name the tabs by suffixing `~tab_name` to the host. Once ready, you will use the parameter `hostgroup` or `Host Setup` modal to see it in action!

Note: Colors can be used in the tab name by using the format `[color]text[/color]` (i.e. `[red]production[/red]`). You can also use emojis supported by Rich (can see them by running `python -m rich.emoji`) by using the format `:emoji:` (i.e. `:ghost:`). Rich supports the normal emoji shortcodes.

Example:
```ini
[cluster1]
1=host1
2=host2:3307
3=host3:3308~[red]production[/red] :ghost:
```

## Things to note
Order of precedence for methods that pass options to Dolphie:
1. Command-line
2. Environment variables
3. Dolphie's config (set by `--config-file`)
4. ~/.mylogin.cnf (`mysql_config_editor`)
5. ~/.my.cnf (set by `--mycnf-file`)

## Feedback
I welcome all questions, bug reports, and requests. If you enjoy Dolphie, please let me know! I'd love to hear from you :smiley:
