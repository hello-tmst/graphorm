import os
import time

import pytest
import redis
from testcontainers.core.container import DockerContainer


@pytest.fixture(scope="session")
def falkordb_container():
    """Start FalkorDB container using testcontainers."""
    image = "falkordb/falkordb-server:latest"
    container = DockerContainer(image).with_exposed_ports(6379)

    container.start()
    host = container.get_container_host_ip()
    port = container.get_exposed_port(6379)

    # Wait for server to be ready
    r = redis.Redis(host=host, port=int(port), decode_responses=False)
    for _ in range(30):
        try:
            r.ping()
            break
        except (redis.exceptions.ConnectionError, redis.exceptions.BusyLoadingError):
            time.sleep(1)
    else:
        pytest.fail("Could not start FalkorDB container")

    yield {
        "host": host,
        "port": int(port),
    }

    container.stop()


@pytest.fixture(scope="session", autouse=True)
def configure_logging():
    import logging

    logging.getLogger("pika").setLevel(logging.WARNING)


@pytest.fixture(scope="session", autouse=True)
def set_test_environments(falkordb_container, session_mocker):
    """Set environment variables for test compatibility."""
    session_mocker.patch.dict(
        os.environ,
        {
            "REDIS_HOST": falkordb_container["host"],
            "REDIS_PORT": str(falkordb_container["port"]),
            "WDM_LOG": "0",
        },
    )


@pytest.fixture(scope="function")
def graph(falkordb_container):
    """Create a Graph instance for testing."""
    import uuid

    from graphorm.graph import Graph

    G = Graph(
        str(uuid.uuid4()),
        host=falkordb_container["host"],
        port=falkordb_container["port"],
    )
    G.create()
    yield G
    G.delete()


@pytest.fixture(scope="function")
def empty_graph(falkordb_container):
    """Create a Graph instance without calling create() (for idempotency tests)."""
    import uuid

    from graphorm.graph import Graph

    G = Graph(
        str(uuid.uuid4()),
        host=falkordb_container["host"],
        port=falkordb_container["port"],
    )
    yield G
    G.delete()
