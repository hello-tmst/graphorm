"""
Tests for list functions: size, head, tail, last.
"""

from graphorm import (
    Node,
    head,
    last,
    select,
    size,
    tail,
)


def test_size_function(graph):
    """Test size() function."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        tags: list = []

    # Create and add node
    page = Page(path="/test", tags=["python", "graph", "orm"])
    graph.add_node(page)
    graph.flush()

    # Query with size()
    PageAlias = Page.alias("p")
    stmt = (
        select()
        .match(PageAlias)
        .where(size(PageAlias.tags) > 0)
        .returns(PageAlias, size(PageAlias.tags).label("tag_count"))
    )

    cypher = stmt.to_cypher()

    assert "SIZE" in cypher.upper()
    assert "tag_count" in cypher


def test_head_function(graph):
    """Test head() function."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        tags: list = []

    # Create and add node
    page = Page(path="/test", tags=["first", "second", "third"])
    graph.add_node(page)
    graph.flush()

    # Query with head()
    PageAlias = Page.alias("p")
    stmt = (
        select()
        .match(PageAlias)
        .returns(PageAlias, head(PageAlias.tags).label("first_tag"))
    )

    cypher = stmt.to_cypher()

    assert "HEAD" in cypher.upper()
    assert "first_tag" in cypher


def test_tail_function(graph):
    """Test tail() function."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        tags: list = []

    # Create and add node
    page = Page(path="/test", tags=["first", "second", "third"])
    graph.add_node(page)
    graph.flush()

    # Query with tail()
    PageAlias = Page.alias("p")
    stmt = (
        select()
        .match(PageAlias)
        .returns(PageAlias, tail(PageAlias.tags).label("rest_tags"))
    )

    cypher = stmt.to_cypher()

    assert "TAIL" in cypher.upper()
    assert "rest_tags" in cypher


def test_last_function(graph):
    """Test last() function."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        tags: list = []

    # Create and add node
    page = Page(path="/test", tags=["first", "second", "third"])
    graph.add_node(page)
    graph.flush()

    # Query with last()
    PageAlias = Page.alias("p")
    stmt = (
        select()
        .match(PageAlias)
        .returns(PageAlias, last(PageAlias.tags).label("last_tag"))
    )

    cypher = stmt.to_cypher()

    assert "LAST" in cypher.upper()
    assert "last_tag" in cypher
