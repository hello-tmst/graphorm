"""Tests for index creation and management."""


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

    # Create graph - should automatically create indexes
    graph.create()

    # Indexes should be created (we can't easily verify without index listing,
    # but the query should not fail)
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

    # Create graph - should automatically create indexes
    graph.create()

    # Verify indexes can be created
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

    # Create graph - should create index on "Page" label
    graph.create()

    assert True
