import os

import pytest


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
def graph():
    from graphorm.graph import Graph
    import redis

    G = Graph("test", redis.Redis())
    G.create()
    yield G
    G.delete()
