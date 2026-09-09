"""The `dolphie` executable under a real pseudo-terminal: argument parsing, connection, render, quit."""

from __future__ import annotations

import fcntl
import os
import pty
import select
import struct
import subprocess
import termios
import time
from pathlib import Path

from tests.integration.cli import dolphie_command, isolated_env, write_config
from tests.integration.servers import Server


def set_window_size(fd: int, rows: int, cols: int) -> None:
    fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0))


def read_until(fd: int, needle: bytes, timeout: float) -> bytes:
    deadline = time.monotonic() + timeout
    output = b""
    while needle not in output:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise AssertionError(f"{needle!r} never appeared on the terminal:\n{output.decode(errors='replace')}")
        ready, _, _ = select.select([fd], [], [], min(remaining, 0.5))
        if ready:
            try:
                output += os.read(fd, 65536)
            except OSError as error:
                raise AssertionError(f"pty closed before {needle!r}:\n{output.decode(errors='replace')}") from error
    return output


def test_tui_starts_renders_the_host_and_quits(server: Server, tmp_path: Path) -> None:
    config = write_config(server, tmp_path)
    controller, terminal = pty.openpty()
    set_window_size(terminal, 50, 200)

    process = subprocess.Popen(
        dolphie_command(config),
        stdin=terminal,
        stdout=terminal,
        stderr=terminal,
        env=isolated_env(tmp_path, TERM="xterm-256color"),
        cwd=tmp_path,
        start_new_session=True,
    )
    os.close(terminal)

    try:
        # The topbar shows host:port at tab setup. Uptime only renders after the first poll succeeds.
        output = read_until(controller, f"{server.host}:{server.port}".encode(), timeout=60)
        output += read_until(controller, b"Uptime", timeout=60)
        assert b"Traceback" not in output, output.decode(errors="replace")

        os.write(controller, b"q")
        # Keep draining the terminal, or the app blocks on a full pty buffer and never sees the key.
        deadline = time.monotonic() + 30
        while process.poll() is None and time.monotonic() < deadline:
            ready, _, _ = select.select([controller], [], [], 0.2)
            if ready:
                try:
                    os.read(controller, 65536)
                except OSError:
                    break
        process.wait(timeout=5)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=10)
        os.close(controller)

    assert process.returncode == 0
