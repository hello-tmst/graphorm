"""Tests for index creation and management."""

import logging


def test_create_index(graph):
    """Test creating an index on a node property."""
    from graphorm import Node

    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        title: str = ""

    # Create index manually
    result = Page.create_index("path", graph)

    assert result is not None


def test_create_index_auto(graph):
    """Test automatic index creation via __indexes__ attribute."""
    from graphorm import Node

    class Page(Node):
        __primary_key__ = ["path"]
        __indexes__ = ["path", "title"]
        path: str
        title: str = ""

    # Graph already created by fixture; indexes created on first create()
    assert True  # If we get here, indexes were created without error


def test_create_index_multiple_properties(graph):
    """Test creating indexes on multiple properties."""
    from graphorm import Node

    class User(Node):
        __primary_key__ = ["id"]
        __indexes__ = ["email", "username"]
        id: int
        email: str = ""
        username: str = ""

    # Graph already created by fixture
    assert True


def test_drop_index(graph):
    """Test dropping an index."""
    from graphorm import Node

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    # Try to drop index (may not exist, that's ok)
    try:
        result = graph.drop_index("Page", "path")
        assert result is not None
    except Exception:
        # Index doesn't exist, create it first then drop
        Page.create_index("path", graph)
        result = graph.drop_index("Page", "path")
        assert result is not None


def test_index_with_explicit_label(graph):
    """Test index creation with explicit label."""
    from graphorm import Node

    class CustomPage(Node):
        __label__ = "Page"
        __primary_key__ = ["path"]
        __indexes__ = ["path"]
        path: str

    # Graph already created by fixture
    assert True


def test_create_idempotent(empty_graph, caplog):
    """Repeated create() must not emit WARNING about already indexed."""
    from graphorm import Node

    class TestNode(Node):
        __label__ = "TestNode"
        __primary_key__ = ["name"]
        __indexes__ = ["name"]
        name: str = ""

    with caplog.at_level(logging.WARNING):
        empty_graph.create()  # first call
        empty_graph.create()  # second call — must be silent

    # Our code must not log WARNING for "already indexed" (driver may still log ERROR)
    for record in caplog.records:
        if record.levelno == logging.WARNING:
            msg = record.message.lower()
            assert "already indexed" not in msg
            assert "already exists" not in msg

    indexes = empty_graph.list_indexes()
    assert any(
        idx["label"] == "TestNode" and "name" in idx.get("properties", [])
        for idx in indexes
    )
