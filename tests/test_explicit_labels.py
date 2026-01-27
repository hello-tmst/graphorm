"""
Tests for explicit label and relation name support in graphorm.
"""

import pytest

from graphorm import (
    Edge,
    Node,
)


def test_explicit_node_label(graph):
    """Test that explicit __label__ works correctly."""

    class CustomPage(Node):
        __label__ = "Page"
        __primary_key__ = ["path"]
        path: str

    page = CustomPage(path="/test")
    assert "Page" in page.labels
    assert page.labels == {"Page"}

    graph.add_node(page)
    graph.flush()

    # Query should work with explicit label
    result = graph.query("MATCH (p:Page) WHERE p.path = '/test' RETURN p")
    assert len(result.result_set) == 1
    assert result.result_set[0][0].path == "/test"


def test_explicit_edge_relation(graph):
    """Test that explicit __relation_name__ works correctly."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    class CustomLink(Edge):
        __relation_name__ = "Linked"

    page1 = Page(path="/page1")
    page2 = Page(path="/page2")

    graph.add_node(page1)
    graph.add_node(page2)
    graph.flush()

    link = CustomLink(page1, page2)
    assert link.relation == "Linked"

    graph.add_edge(link)
    graph.flush()

    # Query should work with explicit relation
    result = graph.query("MATCH (p1:Page)-[r:Linked]->(p2:Page) RETURN p1, r, p2")
    assert len(result.result_set) == 1


def test_node_without_explicit_label_uses_class_name(graph):
    """Test that node without explicit label uses class name as-is."""

    class MyPage(Node):
        __primary_key__ = ["path"]
        path: str

    page = MyPage(path="/test")
    assert "MyPage" in page.labels
    assert page.labels == {"MyPage"}

    graph.add_node(page)
    graph.flush()

    # Query should work with class name
    result = graph.query("MATCH (p:MyPage) WHERE p.path = '/test' RETURN p")
    assert len(result.result_set) == 1


def test_edge_without_explicit_relation_uses_class_name(graph):
    """Test that edge without explicit relation uses class name as-is."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    class MyLink(Edge):
        pass

    page1 = Page(path="/page1")
    page2 = Page(path="/page2")

    graph.add_node(page1)
    graph.add_node(page2)
    graph.flush()

    link = MyLink(page1, page2)
    assert link.relation == "MyLink"

    graph.add_edge(link)
    graph.flush()

    # Query should work with class name
    result = graph.query("MATCH (p1:Page)-[r:MyLink]->(p2:Page) RETURN p1, r, p2")
    assert len(result.result_set) == 1


def test_invalid_explicit_label():
    """Test that invalid explicit label raises error."""
    with pytest.raises(ValueError, match="__label__ must be a non-empty string"):

        class InvalidPage(Node):
            __label__ = ""  # Empty string
            __primary_key__ = ["path"]
            path: str


def test_invalid_explicit_relation():
    """Test that invalid explicit relation raises error."""
    with pytest.raises(
        ValueError, match="__relation_name__ must be a non-empty string"
    ):

        class InvalidLink(Edge):
            __relation_name__ = None  # None value
            pass
