# Dolphie

<p align="center">
  <img src="https://user-images.githubusercontent.com/13244625/187600748-19d2ad15-42e8-4f9c-ada5-a153cdcf4070.png" width="120"><br>
  Your single pane of glass for real-time analytics into MySQL/MariaDB & ProxySQL<br><br>
  <img src="https://github.com/charles-001/dolphie/assets/13244625/88a41290-f52c-4b8e-97f8-3b7ef5096eae" width="30">
  <img src="https://github.com/charles-001/dolphie/assets/13244625/1d94502a-9abf-4436-a7d0-cb2b08c105c1" width="30">
  <img src="https://github.com/charles-001/dolphie/assets/13244625/9b1aadc8-cabb-4256-92f9-fe4d04451b83" width="30">
</p>

![Untitled](https://github.com/user-attachments/assets/b23426ad-060e-4a3a-bb10-66cf0ac95bd0)
<p></p>
<video src='https://github.com/user-attachments/assets/0818485e-f290-4ac4-95d4-8fdc25bb1124'></video>
<p></p>
<video src='https://github.com/user-attachments/assets/9eba7a32-1084-43de-9f62-268ad5f0f922'></video>

## Installation

Requires Python 3.8.1+

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
$ docker exec -it dolphie dolphie --tab-setup
```

## Usage

```
positional arguments:
  uri                   Use a URI string for credentials (mysql/proxysql) - format: mysql://user:password@host:port (port is optional with default 3306, or 6032 for ProxySQL)

options:
  --help                show this help message and exit
  --tab-setup           Start Dolphie by showing the Tab Setup modal instead of automatically connecting with the specified options
  -C , --cred-profile   Credential profile to use. See below for more information
  -u , --user           Username
  -p , --password       Password
  -h , --host           Hostname/IP address
  -P , --port           Port (socket has precedence)
  -S , --socket         Socket file
  -c , --config-file    Dolphie's config file to use. Options are read from these files in the given order: ['/etc/dolphie.cnf', '/etc/dolphie/dolphie.cnf', '~/.dolphie.cnf']
  -m , --mycnf-file     MySQL config file path to use. This should use [client] section [default: ~/.my.cnf]
  -l , --login-path     Specify login path to use with mysql_config_editor's file ~/.mylogin.cnf for encrypted login credentials [default: client]
  -r , --refresh-interval
                        The time, in seconds, between each data collection and processing cycle [default: 1]
  --host-cache-file     Resolve IPs to hostnames when your DNS is unable to. Each IP/hostname pair should be on its own line using format ip=hostname [default: ~/dolphie_host_cache]
  --tab-setup-file      Specify location of file that stores the available hosts to use in Tab Setup modal [default: ~/dolphie_hosts]
  --heartbeat-table     (MySQL only) If your hosts use pt-heartbeat, specify table in format db.table to use the timestamp it has for replication lag instead of Seconds_Behind_Master from SHOW REPLICA STATUS
  --ssl-mode            Desired security state of the connection to the host. Supports: REQUIRED/VERIFY_CA/VERIFY_IDENTITY [default: OFF]
  --ssl-ca              Path to the file that contains a CA (certificate authority)
  --ssl-cert            Path to the file that contains a certificate
  --ssl-key             Path to the file that contains a private key for the certificate
  --panels              What panels to display on startup separated by a comma. Supports: ['dashboard', 'processlist', 'graphs', 'replication', 'metadata_locks', 'ddl', 'pfs_metrics', 'statements_summary', 'proxysql_hostgroup_summary', 'proxysql_mysql_query_rules', 'proxysql_command_stats'], [default: ['dashboard', 'processlist']]
  --graph-marker        What marker to use for graphs (available options: https://tinyurl.com/dolphie-markers) [default: braille]
  --pypi-repo           What PyPi repository to use when checking for a new version default: [https://pypi.org/pypi/dolphie/json]
  -H , --hostgroup      This is used for creating tabs and connecting to them for hosts you specify in Dolphie's config file under a hostgroup section. As an example, you'll have a section called [cluster1] then below it you will list each host on a new line in the format key=host (keys have no meaning). Hosts support optional port (default is whatever port parameter is) in the format host:port. You can also name the tabs by suffixing ~tab_name to the host (i.e. 1=host~tab_name)
  -R, --record          Enables recording of Dolphie's data to a replay file. Note: This can use significant disk space. Monitor accordingly!
  -D, --daemon          Starts Dolphie in daemon mode. This will not show the TUI and is designed be put into the background with whatever solution you decide to use. Automatically enables --record. This mode is solely used for recording data to a replay file
  --daemon-log-file     Full path of the log file for daemon mode
  --daemon-panels       Which panels to run queries for in daemon mode separated by a comma. This can control significant load if the queries are responsible. Dashboard/Replication panels cannot be turned off. Supports: ['processlist', 'metadata_locks', 'pfs_metrics', 'statements_summary', 'proxysql_hostgroup_summary'], [default: ['processlist', 'metadata_locks', 'pfs_metrics']]
  --replay-file         Specify the full path of the replay file to load and enable replay mode
  --replay-dir          Directory to store replay data files
  --replay-retention-hours
                        Number of hours to keep replay data. Data will be purged every hour [default: 48]
  --exclude-notify-vars
                        Dolphie will let you know when a global variable has been changed. If you have variables that change frequently and you don't want to see them, you can specify which ones with this option separated by a comma (i.e. --exclude-notify-vars=variable1,variable2)
  --show-trxs-only      (MySQL only) Start with only showing threads that have an active transaction
  --additional-columns  Start with additional columns in Processlist panel
  --debug-options       Display options that are set and what they're set by (command-line, dolphie config, etc) then exit. WARNING: This will show passwords and other sensitive information in plain text
  -V, --version         Display version and exit

Order of precedence for methods that pass options to Dolphie:
	1. Command-line
	2. Credential profile (set by --cred-profile)
	3. Environment variables
	4. Dolphie's config (set by --config-file)
	5. ~/.mylogin.cnf (mysql_config_editor)
	6. ~/.my.cnf (set by --mycnf-file)

Credential profiles can be defined in Dolphie's config file as a way to store credentials for easy access.
A profile can be created by adding a section in the config file with the format: [credential_profile_<name>]
When using a credential profile, do not include the prefix 'credential_profile' (i.e. -C production)
The following options are supported in credential profiles:
	user
	password
	socket
	ssl_mode REQUIRED/VERIFY_CA/VERIFY_IDENTITY
	ssl_ca
	ssl_cert
	ssl_key
	mycnf_file
	login_path

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
	DOLPHIE_SSL_MODE REQUIRED/VERIFY_CA/VERIFY_IDENTITY
	DOLPHIE_SSL_CA
	DOLPHIE_SSL_CERT
	DOLPHIE_SSL_KEY

Dolphie's config supports these options under [dolphie] section:
	(bool) tab_setup
	(str) credential_profile
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
	(str) tab_setup_file
	(int) refresh_interval
	(str) heartbeat_table
	(comma-separated str) startup_panels
	(str) graph_marker
	(str) pypi_repository
	(str) hostgroup
	(bool) show_trxs_only
	(bool) show_additional_query_columns
	(bool) record_for_replay
	(bool) daemon_mode
	(comma-separated str) daemon_mode_panels
	(str) daemon_mode_log_file
	(str) replay_file
	(str) replay_dir
	(int) replay_retention_hours
	(comma-separated str) exclude_notify_global_vars
```

## Supported MySQL versions

- MySQL/Percona Server 5.6/5.7/8.x/9.x
- AWS RDS/Aurora
- Azure MySQL

## Supported MariaDB versions

- MariaDB 5.5/10.0/11.0+
- AWS RDS
- Azure MariaDB

## Supported ProxySQL versions

- ProxySQL 2.6+ (could work on previous versions but not tested)

Note: Use `admin` user instead of `stats` user so you can use all features

## MySQL Grants required

#### Least privilege

1. PROCESS (only if you switch to using processlist via `P` command)
2. SELECT to `performance_schema` + `pt-heartbeat table` (if used)
3. REPLICATION CLIENT/REPLICATION SLAVE

#### Recommended

1. PROCESS (only if you switch to using processlist via `P` command)
2. Global SELECT access (good for explaining queries, listing all databases, etc)
3. REPLICATION CLIENT/REPLICATION SLAVE
4. SUPER (required if you want to kill queries)

## Record & Replay

Dolphie is capable of recording your live session data that can be used in a future replay if needed. To begin recording, specify the `--record` option along with `--replay-dir` and you're good to go! The data will be saved in a SQLite database compressed with ZSTD for efficient storage management.

To view a replay from either a live session or daemon mode, specify the `--replay-file` option or bring up the `Tab Setup` modal. Replays enable you to navigate through the recorded data as if you were observing Dolphie in real-time at the exact time you need to investigate. The replay interface features intuitive controls for stepping backward, moving forward, playing/pausing, and jumping to specific timestamps. While some commands or features may be restricted in replay mode, all core functionalities for effective review and troubleshooting remain accessible.

## Daemon Mode

If you need Dolphie running incognito while always recording data to capture those critical moments when a database stall causes an incident or a tricky performance issue slips past other monitoring tools, then look no further! Daemon mode is the solution. Purpose-built for nonstop recording, it ensures you never miss the insights that matter most.

To activate Daemon mode, specify the `--daemon` option, which will automatically enable `--record`. This will transform Dolphie into a resource-efficient, passive, always-on monitoring process that continuously records data. It removes Textual's TUI and creates a log file for messages while also printing them to the console.

To run Dolphie in the background using daemon mode, I recommend `systemctl` for its flexibility and management capabilities. To see how to set that up, refer to the [service configuration example](https://github.com/charles-001/dolphie/blob/main/examples/dolphie.service). While alternatives like `nohup` or `tmux` can be used, they are not advisable due to their limited management features. Additionally, check out the [config example](https://github.com/charles-001/dolphie/blob/main/examples/dolphie-daemon.cnf) as a helpful starting point for setting up this mode.

In Daemon mode, metrics are retained for the last 10 minutes to support graphing, with performance schema metric deltas automatically reset at 10-minute intervals. This approach keeps data fresh and relevant, providing an accurate view of recent activity.

**Note**: Daemon mode's replay file can consume significant disk space, particularly on busy servers. To minimize disk usage, adjust the `--replay-retention-hours` and `--refresh-interval` options to control data retention and collection frequency.

Example log messages in daemon mode:

```
[INFO] Starting Dolphie in daemon mode with a refresh interval of 1s
[INFO] Log file: /var/log/dolphie/dolphie.log
[INFO] Connected to MySQL with Process ID 324
[INFO] Replay SQLite file: /var/lib/dolphie/replays/localhost/daemon.db (24 hours retention)
[INFO] Connected to SQLite
[INFO] Replay database metadata - Host: localhost, Port: 3306, Source: MySQL (Percona Server), Dolphie: 6.3.0
[INFO] ZSTD compression dictionary trained with 10 samples (size: 52.56KB)
[WARNING] Read-only mode changed: R/W -> RO
[INFO] Global variable innodb_io_capacity changed: 1000 -> 2000
```

## System Utilization in the Dashboard Panel

The System Utilization section in the Dashboard panel will only display when Dolphie is running on the same host as the server you're connected to. It displays the following information:
- Uptime
- CPU Usage (Percentage, # of cores)
- Load Averages (1, 5, and 15 minutes)
- Memory Usage (Percentage, Used/Total)
- Swap Usage (Used/Total)
- Network Traffic (Down, Up)

Example:

<img width="235" alt="Screenshot 2024-11-03 at 1 19 00 AM" src="https://github.com/user-attachments/assets/664269bc-c303-4222-b0d2-fb1e65ce6202">


## Credential Profiles

Credential profiles can be defined in Dolphie's config file as a way to store credentials for easy access. A profile can be created by adding a section in the config file with the format: `[credential_profile_<name>]`

The following options are supported in credential profiles:

- host
- port (default is 3306)
- user
- password
- socket
- ssl_mode REQUIRED/VERIFY_CA/VERIFY_IDENTITY
- ssl_ca
- ssl_cert
- ssl_key
- mycnf_file
- login_path

Example:

```ini
[credential_profile_dev]
user = dev_user
password = dev_password

[credential_profile_prod]
mycnf_file = /secure/path/to/prod.cnf
```

To use a credential profile, you can specify it with `-C`/`--cred-profile` option without using the prefix `credential_profile` (i.e. `-C prod`) when starting Dolphie. Hostgroups can also use credential profiles (see below)

## Hostgroups

Hostgroups are a way to easily connect to multiple hosts at once. To set this up, you will create a section in Dolphie's config file with the name you want the hostgroup to be and list each host on a new line in the format `key=<json>` (keys have no meaning). Hosts support optional port (default is whatever `port` parameter is) in the format `host:port`. Once ready, you will use the parameter `hostgroup` or `Host Setup` modal to see it in action!

Note: Colors can be used in the tab name by using the format `[color]text[/color]` (i.e. `[red]production[/red]`). You can also use emojis supported by Rich (can see them by running `python -m rich.emoji`) by using the format `:emoji:` (i.e. `:ghost:`). Rich supports the normal emoji shortcodes.

Example:

```ini
[cluster1]
1={"host": "host1", "tab_title": "[yellow]host1[/yellow] :ghost:", "credential_profile": "dev"}
2={"host": "host2", "tab_title": "[blue]host2[/blue] :ghost:", "credential_profile": "dev"}
3={"host": "host3:3307", "tab_title": "[red]production[/red]", "credential_profile": "prod"}
4={"host": "host4"}
```

## Feedback

I welcome all questions, bug reports, and requests. If you enjoy Dolphie, please let me know! I'd love to hear from you :dolphin:
