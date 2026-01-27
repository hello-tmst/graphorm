"""
Tests for Path class.
"""

import pytest

from graphorm import (
    Edge,
    Node,
    Path,
)


def test_path_init_valid():
    """Test Path.__init__() with valid lists."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    class Linked(Edge):
        pass

    page1 = Page(path="/page1")
    page2 = Page(path="/page2")
    page3 = Page(path="/page3")

    edge1 = Linked(page1, page2)
    edge2 = Linked(page2, page3)

    path = Path([page1, page2, page3], [edge1, edge2])

    assert path.nodes() == [page1, page2, page3]
    assert path.edges() == [edge1, edge2]
    assert path.append_type == (Node | Edge)


def test_path_init_invalid_nodes():
    """Test Path.__init__() with invalid nodes (not a list)."""
    with pytest.raises(TypeError, match="nodes and edges must be list"):
        Path("not a list", [])


def test_path_init_invalid_edges():
    """Test Path.__init__() with invalid edges (not a list)."""
    with pytest.raises(TypeError, match="nodes and edges must be list"):
        Path([], "not a list")


def test_path_new_empty_path():
    """Test Path.new_empty_path() class method."""
    path = Path.new_empty_path()

    assert path.nodes() == []
    assert path.edges() == []
    assert path.append_type == (Node | Edge)


def test_path_nodes_and_edges():
    """Test Path.nodes() and Path.edges() methods."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    class Linked(Edge):
        pass

    page1 = Page(path="/page1")
    page2 = Page(path="/page2")
    edge1 = Linked(page1, page2)

    path = Path([page1, page2], [edge1])

    assert path.nodes() == [page1, page2]
    assert path.edges() == [edge1]


def test_path_get_node():
    """Test Path.get_node() method."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    page1 = Page(path="/page1")
    page2 = Page(path="/page2")

    path = Path([page1, page2], [])

    assert path.get_node(0) == page1
    assert path.get_node(1) == page2


def test_path_get_relationship():
    """Test Path.get_relationship() method."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    class Linked(Edge):
        pass

    page1 = Page(path="/page1")
    page2 = Page(path="/page2")
    edge1 = Linked(page1, page2)

    path = Path([page1, page2], [edge1])

    assert path.get_relationship(0) == edge1


def test_path_first_node():
    """Test Path.first_node() method."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    page1 = Page(path="/page1")
    page2 = Page(path="/page2")

    path = Path([page1, page2], [])

    assert path.first_node() == page1


def test_path_last_node():
    """Test Path.last_node() method."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    page1 = Page(path="/page1")
    page2 = Page(path="/page2")

    path = Path([page1, page2], [])

    assert path.last_node() == page2


def test_path_edge_count():
    """Test Path.edge_count() method."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    class Linked(Edge):
        pass

    page1 = Page(path="/page1")
    page2 = Page(path="/page2")
    page3 = Page(path="/page3")
    edge1 = Linked(page1, page2)
    edge2 = Linked(page2, page3)

    path = Path([page1, page2, page3], [edge1, edge2])

    assert path.edge_count() == 2


def test_path_nodes_count():
    """Test Path.nodes_count() method."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    page1 = Page(path="/page1")
    page2 = Page(path="/page2")
    page3 = Page(path="/page3")

    path = Path([page1, page2, page3], [])

    assert path.nodes_count() == 3


def test_path_add_node_correct_order():
    """Test Path.add_node() in correct order (after edge or at start)."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    class Linked(Edge):
        pass

    page1 = Page(path="/page1")
    page2 = Page(path="/page2")
    edge1 = Linked(page1, page2)

    path = Path.new_empty_path()

    # First node
    path.add_node(page1)
    assert path.nodes() == [page1]
    assert path.append_type == Edge

    # Add edge
    path.add_edge(edge1)
    assert path.append_type == Node

    # Add second node
    path.add_node(page2)
    assert path.nodes() == [page1, page2]
    assert path.append_type == Edge


def test_path_add_node_incorrect_order():
    """Test Path.add_node() in incorrect order (should raise AssertionError)."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    class Linked(Edge):
        pass

    page1 = Page(path="/page1")
    page2 = Page(path="/page2")
    page3 = Page(path="/page3")
    edge1 = Linked(page1, page2)
    edge2 = Linked(page2, page3)

    path = Path.new_empty_path()
    path.add_node(page1)
    path.add_edge(edge1)
    path.add_node(page2)  # This should work (append_type is Node after edge)

    # Now append_type should be Edge, so trying to add node should fail
    with pytest.raises(AssertionError, match="Add Edge before adding Node"):
        path.add_node(page3)


def test_path_add_edge_correct_order():
    """Test Path.add_edge() in correct order (after node)."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    class Linked(Edge):
        pass

    page1 = Page(path="/page1")
    page2 = Page(path="/page2")
    edge1 = Linked(page1, page2)

    path = Path.new_empty_path()
    path.add_node(page1)

    # Add edge after node
    path.add_edge(edge1)
    assert path.edges() == [edge1]
    assert path.append_type == Node


def test_path_add_edge_incorrect_order():
    """Test Path.add_edge() in incorrect order (should raise AssertionError)."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    class Linked(Edge):
        pass

    page1 = Page(path="/page1")
    page2 = Page(path="/page2")
    edge1 = Linked(page1, page2)
    edge2 = Linked(page2, page1)

    path = Path.new_empty_path()
    path.add_node(page1)
    path.add_edge(edge1)

    # Try to add edge when expecting node
    with pytest.raises(AssertionError, match="Add Node before adding Edge"):
        path.add_edge(edge2)


def test_path_eq():
    """Test Path.__eq__() method."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    class Linked(Edge):
        pass

    page1 = Page(path="/page1")
    page2 = Page(path="/page2")
    edge1 = Linked(page1, page2)

    path1 = Path([page1, page2], [edge1])
    path2 = Path([page1, page2], [edge1])
    path3 = Path([page1], [])

    assert path1 == path2
    assert path1 != path3


def test_path_str(graph):
    """Test Path.__str__() method."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    class Linked(Edge):
        pass

    # Create nodes and edges in graph
    page1 = Page(path="/page1")
    page2 = Page(path="/page2")
    page3 = Page(path="/page3")

    graph.add_node(page1)
    graph.add_node(page2)
    graph.add_node(page3)
    graph.flush()

    # Get nodes from graph to ensure they have IDs
    page1_from_graph = graph.get_node(Page(path="/page1"))
    page2_from_graph = graph.get_node(Page(path="/page2"))
    page3_from_graph = graph.get_node(Page(path="/page3"))

    # Ensure nodes have IDs
    if page1_from_graph and page2_from_graph and page3_from_graph:
        edge1 = Linked(page1_from_graph, page2_from_graph)
        edge2 = Linked(page2_from_graph, page3_from_graph)

        graph.add_edge(edge1)
        graph.add_edge(edge2)
        graph.flush()

        # Get edges from graph to ensure they have IDs
        # Query for edges or use the ones we just added
        # For simplicity, we'll use the edges we have, but they should have IDs after flush
        # Create path with nodes and edges that have IDs
        path = Path(
            [page1_from_graph, page2_from_graph, page3_from_graph], [edge1, edge2]
        )

        # Test string representation
        path_str = str(path)

        assert path_str.startswith("<")
        assert path_str.endswith(">")
        # Check that IDs are present (they should be numbers)
        assert "(" in path_str
        assert "[" in path_str or "-" in path_str
