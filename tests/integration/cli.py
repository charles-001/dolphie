"""Helpers for running the installed `dolphie` executable as a subprocess."""

from __future__ import annotations

import os
import signal
import sqlite3
import subprocess
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from tests.integration.harness import shared_options
from tests.integration.servers import Server

CREDENTIAL_PROFILE = "integration"


def write_config(server: Server, tmp_path: Path) -> Path:
    """A Dolphie config with the host under [dolphie] and the login in a credential profile."""
    options = {"host": server.host, "port": server.port, **shared_options(tmp_path)}
    config = tmp_path / "dolphie.cnf"
    config.write_text(
        "[dolphie]\n"
        + "".join(f"{key} = {value}\n" for key, value in options.items())
        + f"\n[credential_profile_{CREDENTIAL_PROFILE}]\n"
        f"user = {server.user}\n"
        f"password = {server.password}\n"
    )
    return config


def dolphie_command(config: Path, *args: str) -> list[str]:
    executable = str(Path(sys.executable).parent / "dolphie")
    return [executable, "--config-file", str(config), "--cred-profile", CREDENTIAL_PROFILE, *args]


def isolated_env(tmp_path: Path, **extra: str) -> dict[str, str]:
    """An environment whose HOME keeps ~/.my.cnf, ~/.mylogin.cnf, and ~/.dolphie.cnf out of the run."""
    env = {**os.environ, "HOME": str(tmp_path), **extra}
    env.pop("DOLPHIE_CONFIG", None)
    return env


def daemon_replay_file(server: Server, tmp_path: Path) -> Path:
    return tmp_path / "replays" / f"{server.host}_{server.port}" / "daemon.db"


def replay_row_count(replay_file: Path) -> int:
    # Read-only mode refuses to create the file, so a daemon that has not written yet counts as zero.
    try:
        with sqlite3.connect(f"file:{replay_file}?mode=ro", uri=True) as connection:
            return connection.execute("SELECT COUNT(*) FROM replay_data").fetchone()[0]
    except sqlite3.OperationalError:
        return 0


def _output(process: subprocess.Popen[str]) -> str:
    return process.stdout.read() if process.stdout else ""


@contextmanager
def daemon(server: Server, tmp_path: Path, *extra_args: str) -> Iterator[tuple[subprocess.Popen[str], Path]]:
    """Run `dolphie --daemon` from the real CLI, then stop it with SIGINT and require a clean exit."""
    config = write_config(server, tmp_path)
    process = subprocess.Popen(
        dolphie_command(config, "--daemon", *extra_args),
        cwd=tmp_path,
        env=isolated_env(tmp_path),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        yield process, daemon_replay_file(server, tmp_path)
    finally:
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)

    assert process.returncode == 0, f"dolphie --daemon exited with {process.returncode}:\n{_output(process)}"


def wait_for_rows(process: subprocess.Popen[str], replay_file: Path, count: int) -> None:
    timeout = 90.0
    deadline = time.monotonic() + timeout
    while replay_row_count(replay_file) < count:
        if process.poll() is not None:
            raise AssertionError(f"dolphie --daemon exited early with {process.returncode}:\n{_output(process)}")
        if time.monotonic() > deadline:
            raise AssertionError(f"daemon recorded {replay_row_count(replay_file)} rows in {timeout:.0f}s")
        time.sleep(0.5)


def run_daemon(server: Server, tmp_path: Path, *extra_args: str, min_rows: int = 5) -> Path:
    with daemon(server, tmp_path, *extra_args) as (process, replay_file):
        wait_for_rows(process, replay_file, min_rows)
    return replay_file
