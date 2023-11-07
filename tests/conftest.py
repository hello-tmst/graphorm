import os

import pytest
from pytest_redis import factories

db_conn = factories.redis_noproc()


@pytest.fixture(scope="session", autouse=True)
def configure_logging():
    import logging

    logging.getLogger("pika").setLevel(logging.WARNING)


@pytest.fixture(scope="session", autouse=True)
def set_test_environments(session_mocker):

    session_mocker.patch.dict(
        os.environ,
        {
            "REDIS_HOST": "localhost",
            "REDIS_PORT": "6379",
        },
    )

    session_mocker.patch.dict(
        os.environ,
        {
            "WDM_LOG": "0",
        },
    )


@pytest.fixture(scope="function")
def graph(db_conn):
    from graphorm.graph import Graph
    import uuid

    G = Graph(str(uuid.uuid4()), host=db_conn.host, port=db_conn.port)
    G.create()
    yield G
    G.delete()
