"""Tests for Graph(name, driver=...) injection and dialect usage."""

from unittest.mock import (
    Mock,
    patch,
)

import pytest

from graphorm import Graph
from graphorm.drivers.base import Driver
from graphorm.drivers.dialects import FalkorDBDialect
from graphorm.query_result import QueryResult


def test_graph_uses_passed_driver():
    """Graph(name, driver=...) uses the passed driver and does not create RedisDriver."""
    mock_driver = Mock(spec=Driver)
    mock_driver.dialect = FalkorDBDialect()

    g = Graph("test_graph", driver=mock_driver)

    assert g.driver is mock_driver
    assert g.name == "test_graph"
    # RedisDriver was never instantiated (no redis.Redis call)
    mock_driver.query.assert_not_called()


def test_graph_driver_injection_conflict_connection():
    """Cannot specify both driver and connection."""
    mock_driver = Mock(spec=Driver)
    conn = Mock()

    with pytest.raises(ValueError, match="Cannot specify both 'driver' and connection parameters"):
        Graph("g", driver=mock_driver, connection=conn)


def test_graph_driver_injection_conflict_host():
    """Cannot specify both driver and host."""
    mock_driver = Mock(spec=Driver)

    with pytest.raises(ValueError, match="Cannot specify both 'driver' and connection parameters"):
        Graph("g", driver=mock_driver, host="localhost")


def test_graph_driver_injection_conflict_password():
    """Cannot specify both driver and password."""
    mock_driver = Mock(spec=Driver)

    with pytest.raises(ValueError, match="Cannot specify both 'driver' and connection parameters"):
        Graph("g", driver=mock_driver, password="secret")


def test_graph_driver_injection_conflict_port():
    """Cannot specify both driver and non-default port."""
    mock_driver = Mock(spec=Driver)

    with pytest.raises(ValueError, match="Cannot specify both 'driver' and connection parameters"):
        Graph("g", driver=mock_driver, port=7000)


def test_node_create_index_uses_driver_dialect():
    """Node.create_index uses graph.driver.dialect.create_index_sql (no FalkorDBDialect import in node)."""
    from graphorm import Node

    class User(Node):
        __primary_key__ = ["id"]
        id: int
        email: str

    mock_driver = Mock(spec=Driver)
    mock_dialect = Mock(spec=FalkorDBDialect)
    mock_dialect.create_index_sql.return_value = "CREATE INDEX ON :User(email)"
    mock_driver.dialect = mock_dialect
    mock_driver.query.return_value = Mock(spec=QueryResult)

    g = Graph("g", driver=mock_driver)
    # Graph uses __slots__; patch list_indexes on the class so create_index sees no existing index
    with patch.object(Graph, "list_indexes", return_value=[]):
        result = User.create_index("email", g)

    mock_dialect.create_index_sql.assert_called_once_with("User", "email")
    mock_driver.query.assert_called_once()
    call_args = mock_driver.query.call_args
    assert call_args[0][2] == "CREATE INDEX ON :User(email)"  # query string passed to query()


def test_graph_drop_index_uses_driver_dialect():
    """Graph.drop_index uses driver.dialect.drop_index_sql."""
    mock_driver = Mock(spec=Driver)
    mock_dialect = Mock(spec=FalkorDBDialect)
    mock_dialect.drop_index_sql.return_value = "DROP INDEX ON :User(email)"
    mock_driver.dialect = mock_dialect
    mock_driver.query.return_value = Mock(spec=QueryResult)

    g = Graph("g", driver=mock_driver)
    g.drop_index("User", "email")

    mock_dialect.drop_index_sql.assert_called_once_with("User", "email")
    mock_driver.query.assert_called_once()
    assert mock_driver.query.call_args[0][2] == "DROP INDEX ON :User(email)"


def test_graph_list_indexes_uses_driver_dialect():
    """Graph.list_indexes uses driver.dialect.procedure_indexes() and parses (label, properties)."""
    mock_driver = Mock(spec=Driver)
    mock_dialect = FalkorDBDialect()
    mock_driver.dialect = mock_dialect
    # result_set: rows of (label, properties) per procedure_indexes() YIELD label, properties
    mock_result = Mock()
    mock_result.result_set = [["User", ["email"]], ["Post", ["slug", "created_at"]]]
    mock_driver.call_procedure.return_value = mock_result

    g = Graph("g", driver=mock_driver)
    indexes = g.list_indexes()

    mock_driver.call_procedure.assert_called_once()
    call_kw = mock_driver.call_procedure.call_args[1]
    assert call_kw["read_only"] is True
    # procedure is the full call from dialect (second positional: graph_name, procedure)
    assert mock_driver.call_procedure.call_args[0][1] == "db.indexes() YIELD label, properties"
    assert indexes == [
        {"label": "User", "properties": ["email"]},
        {"label": "Post", "properties": ["slug", "created_at"]},
    ]
