from __future__ import annotations

import functools
import os
import shutil
import subprocess
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from tests.integration.servers import (
    STANDALONE_DIR,
    STANDALONE_PROJECT,
    STANDALONE_SERVERS,
    TOPOLOGIES,
    Server,
    Topology,
    selected_server_ids,
    selected_topology_ids,
)

INTEGRATION_ROOT = Path(__file__).parent
TOPOLOGY_ROOT = INTEGRATION_ROOT / "topologies"
# Tests marked flavor_agnostic exercise no code that branches on the server version, so they run
# against one MySQL and one MariaDB only. With neither selected they are deselected, not skipped.
FLAVOR_AGNOSTIC_SERVERS = {"mysql84", "mariadb123"}

# Filled during collection so the session fixture can start every needed container in one go.
NEEDED_PROFILES = pytest.StashKey[set[str]]()
NEEDED_TOPOLOGIES = pytest.StashKey[set[str]]()


@functools.cache
def host_can_run(image: str, platform: str) -> bool:
    """Whether Docker on this host can execute ``image`` built for ``platform`` (natively or emulated)."""
    result = subprocess.run(
        ["docker", "run", "--rm", "--platform", platform, "--entrypoint", "true", image],
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def pytest_itemcollected(item: pytest.Item) -> None:
    """Mark the suite so addopts can exclude it from a plain `pytest` run."""
    if TOPOLOGY_ROOT in item.path.parents:
        item.add_marker(pytest.mark.topology)
    elif INTEGRATION_ROOT in item.path.parents:
        item.add_marker(pytest.mark.integration)


# trylast runs after pytest's own -m deselection, so a unit-test run never probes Docker.
@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Record which servers the selected tests use, and drop tests whose server this host cannot run."""
    profiles: set[str] = set()
    topologies: set[str] = set()
    deselected: list[pytest.Item] = []
    selected_topologies = selected_topology_ids()
    for item in items:
        if INTEGRATION_ROOT not in item.path.parents:
            continue
        params = getattr(getattr(item, "callspec", None), "params", {})
        if "topology" in params:
            if params["topology"] not in selected_topologies:
                deselected.append(item)
                continue
            topologies.add(params["topology"])
        if "server" in params:
            server = STANDALONE_SERVERS.get(params["server"])
            # With no MySQL server selected, the parametrize list is empty and pytest leaves a
            # placeholder item (param NOTSET) that it already marks as skipped.
            if server is None:
                continue
            if server.platform and not host_can_run(server.image, server.platform):
                item.add_marker(pytest.mark.skip(reason=f"this Docker host cannot run {server.platform} images"))
                continue
            if item.get_closest_marker("flavor_agnostic") and server.id not in FLAVOR_AGNOSTIC_SERVERS:
                deselected.append(item)
                continue
            profiles.add(server.id)
        if "proxysql_server" in getattr(item, "fixturenames", ()):
            profiles.add("proxysql")

    if deselected:
        config.hook.pytest_deselected(items=deselected)
        items[:] = [item for item in items if item not in deselected]

    config.stash[NEEDED_PROFILES] = profiles
    config.stash[NEEDED_TOPOLOGIES] = topologies


def _compose(directory: Path, project: str, profiles: set[str], *args: str) -> None:
    command = ["docker", "compose", "-p", project]
    for profile in sorted(profiles):
        command += ["--profile", profile]
    command += list(args)
    result = subprocess.run(command, cwd=directory, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"{' '.join(command)} failed in {directory}:\n{result.stdout}\n{result.stderr}")


def _require_docker() -> None:
    if shutil.which("docker") is None:
        pytest.fail("The integration suite needs the docker CLI on PATH")
    result = subprocess.run(["docker", "compose", "version"], capture_output=True, text=True)
    if result.returncode != 0:
        pytest.fail(f"docker compose is not available: {result.stderr.strip()}")


@pytest.fixture(scope="session", autouse=True)
def compose(request: pytest.FixtureRequest) -> Iterator[None]:
    """Bring up every server the selected tests need, all at once, and tear them down at the end."""
    _require_docker()

    profiles = request.config.stash.get(NEEDED_PROFILES, set())
    topologies = request.config.stash.get(NEEDED_TOPOLOGIES, set())
    projects: list[tuple[Path, str, set[str]]] = []
    if profiles:
        projects.append((STANDALONE_DIR, STANDALONE_PROJECT, profiles))
    for topology_id in sorted(topologies):
        topology = TOPOLOGIES[topology_id]
        projects.append((topology.directory, topology.project, set()))

    # Each compose project starts its own containers in parallel. Run the projects in parallel too.
    with ThreadPoolExecutor(max_workers=max(1, len(projects))) as pool:
        list(pool.map(lambda project: _compose(*project, "up", "-d", "--wait", "--quiet-pull"), projects))

    yield

    if os.environ.get("DOLPHIE_IT_KEEP", "") != "1":
        for project in projects:
            _compose(*project, "down", "-v", "--remove-orphans")


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    if "server" in metafunc.fixturenames:
        ids = [server_id for server_id in selected_server_ids() if server_id != "proxysql"]
        metafunc.parametrize("server", ids, indirect=True)


@pytest.fixture
def server(request: pytest.FixtureRequest) -> Server:
    """One MySQL or MariaDB server from the standalone matrix."""
    return STANDALONE_SERVERS[request.param]


@pytest.fixture
def proxysql_server() -> Server:
    if "proxysql" not in selected_server_ids():
        pytest.skip("proxysql is not in DOLPHIE_IT_SERVERS")
    return STANDALONE_SERVERS["proxysql"]


@pytest.fixture
def topology(request: pytest.FixtureRequest) -> Topology:
    """One multi-node topology, chosen by ``@pytest.mark.parametrize("topology", [...], indirect=True)``."""
    return TOPOLOGIES[request.param]
