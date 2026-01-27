"""
Tests for RedisDriver error handling and edge cases.
"""

from unittest.mock import (
    Mock,
    patch,
)

import pytest
import redis

from graphorm.drivers.redis import RedisDriver
from graphorm.exceptions import (
    ConnectionError,
    QueryExecutionError,
)
from graphorm.types import CMD


def test_redis_driver_build_params_header():
    """Test RedisDriver._build_params_header() method."""
    driver = RedisDriver("localhost", 6379)

    # Test with simple params
    params = {"name": "test", "value": 42}
    header = driver._build_params_header(params)

    assert header.startswith("CYPHER")
    assert "name=" in header
    assert "value=" in header

    # Test with list params
    params = {"items": [1, 2, 3]}
    header = driver._build_params_header(params)

    assert "items=" in header

    # Test with dict params
    params = {"data": {"key": "value"}}
    header = driver._build_params_header(params)

    assert "data=" in header

    # Test with None
    params = {"value": None}
    header = driver._build_params_header(params)

    assert "null" in header

    # Test TypeError for non-dict
    with pytest.raises(TypeError, match="'params' must be a dict"):
        driver._build_params_header("not a dict")


def test_redis_driver_commit_batch_size_zero(graph):
    """Test RedisDriver.commit() with batch_size=0 (disable batching)."""
    from graphorm import Node

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    # Create nodes
    page1 = Page(path="/page1")
    page2 = Page(path="/page2")

    graph.add_node(page1)
    graph.add_node(page2)

    # Commit with batch_size=0 (should commit all at once)
    result = graph.driver.commit(graph, batch_size=0)

    # Should return QueryResult
    assert result is not None


def test_redis_driver_commit_empty_items(graph):
    """Test RedisDriver.commit() with empty items list."""
    result = graph.driver.commit(graph, items=[])

    # Should return None for empty items
    assert result is None


def test_redis_driver_commit_with_custom_items(graph):
    """Test RedisDriver.commit() with custom items list."""
    from graphorm import Node

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    # Create nodes outside of graph
    page1 = Page(path="/page1")
    page2 = Page(path="/page2")

    # Commit custom items list
    result = graph.driver.commit(graph, items=[page1, page2], batch_size=1)

    # Should return QueryResult
    assert result is not None


def test_redis_driver_query_with_params(graph):
    """Test RedisDriver.query() with parameters."""
    result = graph.driver.query(
        CMD.QUERY,
        graph.name,
        "RETURN $value AS result",
        params={"value": 42},
        graph=graph,
    )

    assert result is not None


def test_redis_driver_query_with_timeout(graph):
    """Test RedisDriver.query() with timeout."""
    result = graph.driver.query(
        CMD.RO_QUERY,
        graph.name,
        "MATCH (n) RETURN n LIMIT 1",
        timeout=1000,
        read_only=True,
        graph=graph,
    )

    assert result is not None


def test_redis_driver_query_read_only(graph):
    """Test RedisDriver.query() with read_only=True."""
    result = graph.driver.query(
        CMD.QUERY, graph.name, "MATCH (n) RETURN n LIMIT 1", read_only=True, graph=graph
    )

    assert result is not None


@patch("graphorm.drivers.redis.redis.Redis")
def test_redis_driver_connection_error(mock_redis):
    """Test RedisDriver.query() handling ConnectionError."""
    # Setup mock to raise ConnectionError
    mock_redis_instance = Mock()
    mock_redis_instance.execute_command.side_effect = redis.exceptions.ConnectionError(
        "Connection failed"
    )
    mock_redis.return_value = mock_redis_instance

    driver = RedisDriver("localhost", 6379)
    driver.connection = mock_redis_instance

    from graphorm.types import CMD

    with pytest.raises(ConnectionError, match="Failed to connect to Redis"):
        driver.query(CMD.QUERY, "test_graph", "RETURN 0")


@patch("graphorm.drivers.redis.redis.Redis")
def test_redis_driver_timeout_error(mock_redis):
    """Test RedisDriver.query() handling TimeoutError."""
    # Setup mock to raise TimeoutError
    mock_redis_instance = Mock()
    mock_redis_instance.execute_command.side_effect = redis.exceptions.TimeoutError(
        "Query timeout"
    )
    mock_redis.return_value = mock_redis_instance

    driver = RedisDriver("localhost", 6379)
    driver.connection = mock_redis_instance

    from graphorm.types import CMD

    with pytest.raises(QueryExecutionError, match="Query timeout"):
        driver.query(CMD.QUERY, "test_graph", "RETURN 0")


@patch("graphorm.drivers.redis.redis.Redis")
def test_redis_driver_unknown_command_fallback(mock_redis):
    """Test RedisDriver.query() fallback when RO_QUERY is unknown."""
    # Setup mock to raise ResponseError for unknown command, then succeed
    mock_redis_instance = Mock()

    # First call raises error about unknown command, second succeeds
    mock_redis_instance.execute_command.side_effect = [
        redis.exceptions.ResponseError("unknown command 'GRAPH.RO_QUERY'"),
        Mock(),  # Successful response
    ]
    mock_redis.return_value = mock_redis_instance

    driver = RedisDriver("localhost", 6379)
    driver.connection = mock_redis_instance

    from graphorm.types import CMD

    # Should fallback to non-read-only query
    result = driver.query(CMD.RO_QUERY, "test_graph", "RETURN 0", read_only=True)

    # Should have attempted fallback (second call)
    assert mock_redis_instance.execute_command.call_count == 2


@patch("graphorm.drivers.redis.redis.Redis")
def test_redis_driver_response_error(mock_redis):
    """Test RedisDriver.query() handling ResponseError."""
    # Setup mock to raise ResponseError
    mock_redis_instance = Mock()
    mock_redis_instance.execute_command.side_effect = redis.exceptions.ResponseError(
        "Query syntax error"
    )
    mock_redis.return_value = mock_redis_instance

    driver = RedisDriver("localhost", 6379)
    driver.connection = mock_redis_instance

    from graphorm.types import CMD

    with pytest.raises(QueryExecutionError, match="Query failed"):
        driver.query(CMD.QUERY, "test_graph", "INVALID QUERY")


@patch("graphorm.drivers.redis.redis.Redis")
def test_redis_driver_timeout_validation(mock_redis):
    """Test RedisDriver.query() timeout validation."""
    mock_redis_instance = Mock()
    mock_redis.return_value = mock_redis_instance

    driver = RedisDriver("localhost", 6379)
    driver.connection = mock_redis_instance

    from graphorm.types import CMD

    # Test with non-integer timeout
    with pytest.raises(
        QueryExecutionError, match="Timeout argument must be a positive integer"
    ):
        driver.query(CMD.QUERY, "test_graph", "RETURN 0", timeout="not an int")


def test_redis_driver_call_procedure(graph):
    """Test RedisDriver.call_procedure() method."""
    result = graph.driver.call_procedure(
        graph.name, "db.labels", read_only=True, graph=graph
    )

    assert result is not None


def test_redis_driver_call_procedure_with_yield(graph):
    """Test RedisDriver.call_procedure() with YIELD clause."""
    result = graph.driver.call_procedure(
        graph.name, "db.propertyKeys", y=["propertyKey"], read_only=True, graph=graph
    )

    assert result is not None


def test_redis_driver_commit_error_handling(graph):
    """Test RedisDriver.commit() error handling and continuation."""
    from graphorm import Node

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    # Create many nodes to test batch error handling
    pages = [Page(path=f"/page{i}") for i in range(10)]

    # Add to graph
    for page in pages:
        graph.add_node(page)

    # Commit should handle errors gracefully and continue
    # Note: This tests the error handling logic, actual errors depend on Redis state
    result = graph.driver.commit(graph, batch_size=2)

    # Should return result even if some batches had issues
    # (implementation continues on error)
