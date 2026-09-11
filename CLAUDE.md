# Dolphie

Textual TUI for real-time MySQL, MariaDB, and ProxySQL monitoring. Python 3.10+, uv, hatchling. Setup and the check commands are in `README.md` under Development. Write code that reads like the surrounding code.

## Workflow

- Before you push, run `uv run ruff format .`, `uv run ruff check .`, `uv run basedpyright`, and `uv run pytest`. CI runs the same checks on Python 3.10 and 3.14. basedpyright has no baseline and includes `tests/`, so fix new diagnostics instead of suppressing them.
- Python 3.10 is the floor. No `datetime.UTC`, `tomllib`, or other 3.11+ stdlib.
- Manage dependencies with `uv add` and `uv remove`. Commit `pyproject.toml` and `uv.lock` together.
- The version lives only in `pyproject.toml`. Runtime reads it with `importlib.metadata`.
- Conventional commits. No AI or tool attribution. Do not commit or push unless the request asks for it.

## Threads and tabs

- Main, replica, and replay pollers are `@work(thread=True)` workers in `WorkerManager`. A worker may mutate `dolphie.*` state. Any widget mutation, mount, or `notify` from a worker must go through `self.app.call_from_thread`. The main thread refreshes the UI in `on_worker_state_changed`.
- A disconnect from inside a worker must pass `wait_for_workers=False` to `disconnect_tab`, or it deadlocks.
- Each tab owns a `Dolphie` instance, but widgets are mounted once and shared. `Tab.save_references_to_components` points every tab at the same containers. Data shown for one tab must be cleared or rebound on tab switch.
- Each tab has two connections. `main_db_connection` belongs to the poll worker. `secondary_db_connection` runs hotkey commands. Never run a hotkey query on the main connection.
- Panel visibility is both `panels.<name>.visible` and the shared widget `.display`. `App.PANEL_MAPPING` selects the MySQL or ProxySQL panel module.
- `CommandManager` is the catalog for the help screen and command palette. `KeyEventManager` implements the behavior. A new key needs both. Keys `4` to `6` map to different panels for MySQL and ProxySQL.

## MySQL, MariaDB, ProxySQL

- A successful `SELECT @@admin-version` means ProxySQL. MariaDB is `connection_source_alt`, not a separate connection source.
- Branch on `connection_source_alt`, `host_version`, and Performance Schema availability before you read a column or run a query. `Queries.py` holds per-flavor SQL. MariaDB has no `replication_applier_status_by_worker` and no processlist UUID for replica discovery, so never pair replicas by list position.
- MySQL sessions run `SET SESSION sql_mode=''` on connect. Keep it.
- Kill statements come from `Dolphie.build_kill_query`. RDS and Aurora use `mysql.rds_kill`, Azure uses `mysql.az_kill`, ProxySQL uses `KILL CONNECTION`.

## Metrics and graphs

- A new graphable metric needs a `MetricDefinitions` entry and a `GRAPH_TABS` slot in `MetricGraphDefinitions`. `validate_graph_definitions()` runs at import and fails on any unassigned graphable series.
- Metrics default to `per_second_calculation=True`. The first poll only sets a baseline. A negative delta is a counter reset: rate 0 and a new baseline.
- `MetricManager.reset()` must not clear `global_status`, `global_variables`, or `replication_status`. `Dolphie` owns them and shares them by reference.
- Metric timestamps are `METRIC_DATETIME_FORMAT` strings in UTC, not ISO.
- Graph colors come from `MetricColor` RGB tuples and `Theme` constants. Textual `$variable` markup does not reach plotext.

## Replay

- Replay files are SQLite with ZSTD-compressed orjson rows. `ReplayManager.schema_version` is the compatibility gate. Bump it for any incompatible table or payload change. Daemon mode renames a mismatched file and starts fresh. Playback refuses it.
- Playback opens the file with `mode=ro` and never runs DDL, `VACUUM`, `chmod`, or a purge. A daemon may be writing the same file. The compression dictionary is raw content loaded through `ZstdCompressionDict` auto-detection, so old and new files read with one code path.
- Daemon recordings store latest-only `_delta` metrics, so a seek rebuilds a window. Live recordings store full history snapshots.
- A daemon file holds one host and one connection source. Never mix MySQL and ProxySQL data in one file.

## Config and theme

- Option precedence is command line, credential profile, environment, Dolphie config, `~/.mylogin.cnf`, then `~/.my.cnf`. The `ArgumentParser` epilog is copied into the README Usage block, so update both.
- Hostgroup entries are JSON objects, for example `{"host": "host1:3307", "credential_profile": "prod"}`.
- Colors are `Theme.py` constants and `THEME_VARIABLES`. Rich output needs `themed_text`, `ThemedTable`, or `ThemedDataTable` to resolve `$variables`. Do not hardcode hex in panel markup.

## Tests

- Unit tests never open a live database. They use `MagicMock` connections and canned status dicts. Rate, baseline, and counter-reset tests run several polls.
- `tests/integration` runs the real `DolphieApp` headless against Docker servers (see README, Integration tests). `uv run pytest` excludes it through `addopts`, so pass `-m integration` or `-m topology`. Use `tests/integration/harness.py`: `harness.press` and `harness.wait_for`, never `Pilot.press` or `Pilot.pause`, which wait for CPU idle that a polling app never reaches.
- Tests are plain `async def` functions. `asyncio_mode = "auto"` runs them, so never call `asyncio.run` in a test. A `DeprecationWarning` attributed to a `dolphie` module fails the run.
- `tests/dolphie/test_Snapshots.py` renders every panel from the daemon recordings in `tests/dolphie/replays` and compares SVGs. An intended rendering change needs `--snapshot-update` and a review of the report. Re-record the replays with `--daemon-panels` covering every daemon panel, under load, when the replay schema changes.
- Panel row assertions need a counter delta between two polls. Induce activity on a side connection and wait on `filtered_data`, not `internal_data`.
- A new server version belongs in `compose/standalone/docker-compose.yml`, `servers.py`, the CI matrix in `.github/workflows/main.yml`, and the README list.
- A bug fix includes a regression test that fails on the original behavior.
