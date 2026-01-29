"""
Tests for Graph coverage: connection init, add_node/add_edge return 0, get_label/get_property refresh.
"""

import uuid
from unittest.mock import patch

import pytest
import redis

from graphorm import Edge, Node
from graphorm.graph import Graph


def test_graph_without_connection_or_host_raises():
    """Graph() without connection or host raises ValueError."""
    with pytest.raises(ValueError, match="Either connection or host must be provided"):
        Graph(str(uuid.uuid4()))


def test_graph_with_connection_uses_connection_pool(falkordb_container):
    """Graph(connection=...) uses connection_pool.connection_kwargs."""
    conn = redis.Redis(
        host=falkordb_container["host"],
        port=falkordb_container["port"],
        decode_responses=False,
    )
    name = str(uuid.uuid4())
    G = Graph(name, connection=conn)
    G.create()
    try:
        assert G.driver is not None
        assert G.name == name
    finally:
        G.delete()
    conn.close()


def test_graph_connection_without_pool_fallback():
    """Graph(connection=obj_without_pool) falls back to localhost/6379/None."""
    class NoPool:
        pass

    G = Graph(str(uuid.uuid4()), connection=NoPool())
    assert G._driver is not None
    # RedisDriver only stores .connection; host/port/password are passed to Redis() at init
    assert G._driver.connection is not None


def test_add_node_returns_zero_when_already_exists(graph):
    """add_node returns 0 when node with same primary key already exists."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    page1 = Page(path="/same")
    first = graph.add_node(page1)
    assert first == 1
    # Same primary key, different alias (new instance)
    page2 = Page(path="/same")
    second = graph.add_node(page2)
    assert second == 0


def test_add_edge_returns_zero_when_already_exists(graph):
    """add_edge returns 0 when same edge already exists."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    class Linked(Edge):
        pass

    page1 = Page(path="/p1")
    page2 = Page(path="/p2")
    graph.add_node(page1)
    graph.add_node(page2)
    graph.flush()

    edge1 = Linked(page1, page2)
    first = graph.add_edge(edge1)
    assert first == 1
    edge2 = Linked(page1, page2)
    second = graph.add_edge(edge2)
    assert second == 0


def test_get_label_refreshes_on_index_error(graph):
    """get_label(idx) calls _refresh_labels when index not in cache."""
    graph._labels = []

    def mock_refresh_labels():
        graph._labels = ["Page"]
        return graph._labels

    with patch.object(Graph, "_refresh_labels", side_effect=mock_refresh_labels):
        label = graph.get_label(0)
    assert label == "Page"


def test_get_property_refreshes_on_index_error(graph):
    """get_property(idx) calls _refresh_property_keys when index not in cache."""
    graph._property_keys = []

    def mock_refresh_property_keys():
        graph._property_keys = ["path"]
        return graph._property_keys

    with patch.object(
        Graph, "_refresh_property_keys", side_effect=mock_refresh_property_keys
    ):
        prop = graph.get_property(0)
    assert prop == "path"
