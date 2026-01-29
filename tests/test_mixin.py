"""
Tests for mixin classes: NodeMixin, EdgeMixin, GraphMixin.
"""

from graphorm import (
    Edge,
    Node,
)
from graphorm.drivers.redis import RedisDriver
from graphorm.mixin import (
    EdgeMixin,
    GraphMixin,
    NodeMixin,
)


class _TestDriver(NodeMixin, EdgeMixin, GraphMixin, RedisDriver):
    """Test driver that combines all mixins with RedisDriver."""
    __test__ = False

    def get(self, graph, entity):
        """Dispatch to NodeMixin.get or EdgeMixin.get by entity type."""
        if isinstance(entity, Edge):
            return EdgeMixin.get(self, graph, entity)
        return NodeMixin.get(self, graph, entity)


# Alias for backward compatibility in tests
TestDriver = _TestDriver


def test_node_mixin_get_existing_node(graph):
    """Test NodeMixin.get() with existing node."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        parsed: bool = False

    # Create and add node
    page = Page(path="/test", parsed=True)
    graph.add_node(page)
    graph.flush()

    # Create test driver with mixins
    driver = TestDriver(
        graph.driver.connection.connection_pool.connection_kwargs.get(
            "host", "localhost"
        ),
        graph.driver.connection.connection_pool.connection_kwargs.get("port", 6379),
    )

    # Create node instance with alias for query
    test_node = Page(path="/test")
    test_node.set_alias("p")

    # Test getting existing node
    found_node = driver.get(graph, test_node)

    assert found_node is not None
    assert isinstance(found_node, Page)
    assert found_node.properties["path"] == "/test"
    assert found_node.properties["parsed"] is True

    assert found_node is not None
    assert isinstance(found_node, Page)
    assert found_node.properties["path"] == "/test"
    assert found_node.properties["parsed"] is True


def test_node_mixin_get_nonexistent_node(graph):
    """Test NodeMixin.get() with non-existent node."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    # Create test driver with mixins
    driver = TestDriver(
        graph.driver.connection.connection_pool.connection_kwargs.get(
            "host", "localhost"
        ),
        graph.driver.connection.connection_pool.connection_kwargs.get("port", 6379),
    )

    # Create node instance with alias
    test_node = Page(path="/nonexistent")
    test_node.set_alias("p")

    # Test getting non-existent node
    found_node = driver.get(graph, test_node)

    assert found_node is None


def test_edge_mixin_get_existing_edge(graph):
    """Test EdgeMixin.get() with existing edge."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    class Linked(Edge):
        pass

    # Create and add nodes and edge
    page1 = Page(path="/page1")
    page2 = Page(path="/page2")
    graph.add_node(page1)
    graph.add_node(page2)
    graph.flush()

    link = Linked(page1, page2)
    graph.add_edge(link)
    graph.flush()

    # Create test driver
    driver = TestDriver(
        graph.driver.connection.connection_pool.connection_kwargs.get(
            "host", "localhost"
        ),
        graph.driver.connection.connection_pool.connection_kwargs.get("port", 6379),
    )

    # Create edge instance with aliases for query
    test_edge = Linked(page1, page2)
    test_edge.set_alias("r")
    page1.set_alias("p1")
    page2.set_alias("p2")

    found_edge = driver.get(graph, test_edge)
    assert found_edge is not None
    assert isinstance(found_edge, Linked)


def test_edge_mixin_get_nonexistent_edge(graph):
    """Test EdgeMixin.get() with non-existent edge."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    class Linked(Edge):
        pass

    # Create and add nodes
    page1 = Page(path="/page1")
    page2 = Page(path="/page2")
    graph.add_node(page1)
    graph.add_node(page2)
    graph.flush()

    # Create test driver
    driver = TestDriver(
        graph.driver.connection.connection_pool.connection_kwargs.get(
            "host", "localhost"
        ),
        graph.driver.connection.connection_pool.connection_kwargs.get("port", 6379),
    )

    # Create edge instance with aliases
    test_edge = Linked(page1, page2)
    test_edge.set_alias("r")
    page1.set_alias("p1")
    page2.set_alias("p2")

    found_edge = driver.get(graph, test_edge)
    assert found_edge is None


def test_get_edge_by_node_ids(graph):
    """Test get_edge with edge whose src_node/dst_node are int IDs (e.g. from parsed result)."""

    class Linked(Edge):
        pass

    # Edge with node IDs (as when loaded from query result)
    edge = Linked(123, 456)
    edge.set_alias("r")

    # _build_edge_match_clause should produce WHERE id(src)=123 AND id(dst)=456
    match_clause, params = graph._build_edge_match_clause(edge)
    assert "id(src) = 123" in match_clause
    assert "id(dst) = 456" in match_clause
    assert params == {}

    # get_edge should not crash; no such edge in graph so result is None
    found = graph.get_edge(edge)
    assert found is None


def test_graph_mixin_create(graph):
    """Test GraphMixin.create()."""
    import uuid

    from graphorm.graph import Graph

    # Create test driver
    driver = TestDriver(
        graph.driver.connection.connection_pool.connection_kwargs.get(
            "host", "localhost"
        ),
        graph.driver.connection.connection_pool.connection_kwargs.get("port", 6379),
    )

    # Create new graph
    test_graph = Graph(
        str(uuid.uuid4()),
        host=graph.driver.connection.connection_pool.connection_kwargs.get(
            "host", "localhost"
        ),
        port=graph.driver.connection.connection_pool.connection_kwargs.get(
            "port", 6379
        ),
    )

    # Test create
    result = driver.create(test_graph)

    assert result is not None

    # Cleanup
    test_graph.delete()


def test_graph_mixin_create_with_timeout(graph):
    """Test GraphMixin.create() with timeout."""
    import uuid

    from graphorm.graph import Graph

    # Create test driver
    driver = TestDriver(
        graph.driver.connection.connection_pool.connection_kwargs.get(
            "host", "localhost"
        ),
        graph.driver.connection.connection_pool.connection_kwargs.get("port", 6379),
    )

    # Create new graph
    test_graph = Graph(
        str(uuid.uuid4()),
        host=graph.driver.connection.connection_pool.connection_kwargs.get(
            "host", "localhost"
        ),
        port=graph.driver.connection.connection_pool.connection_kwargs.get(
            "port", 6379
        ),
    )

    # Test create with timeout
    result = driver.create(test_graph, timeout=1000)

    assert result is not None

    # Cleanup
    test_graph.delete()


def test_graph_mixin_delete(graph):
    """Test GraphMixin.delete()."""
    import uuid

    from graphorm.graph import Graph

    # Create test driver
    driver = TestDriver(
        graph.driver.connection.connection_pool.connection_kwargs.get(
            "host", "localhost"
        ),
        graph.driver.connection.connection_pool.connection_kwargs.get("port", 6379),
    )

    # Create new graph
    test_graph = Graph(
        str(uuid.uuid4()),
        host=graph.driver.connection.connection_pool.connection_kwargs.get(
            "host", "localhost"
        ),
        port=graph.driver.connection.connection_pool.connection_kwargs.get(
            "port", 6379
        ),
    )
    test_graph.create()

    # Test delete
    result = driver.delete(test_graph)

    assert result is not None
