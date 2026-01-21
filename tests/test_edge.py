import logging
import pytest
from graphorm.edge import Edge
from graphorm.node import Node


def test_edge():
    class Page(Node):
        __primary_key__ = ["path", "parsed"]

        path: str
        parsed: bool

    class Linked(Edge):
        pass

    page0 = Page(path="0")
    page1 = Page(path="1")

    edge = Linked(page0, page1, _id=1)

    # logging.info(edge)


def test_edge_merge_with_composite_primary_key(graph):
    """Test Edge.merge() with composite primary key nodes."""
    class Page(Node):
        __primary_key__ = ["path", "domain"]
        path: str
        domain: str
    
    class Linked(Edge):
        weight: float = 1.0
    
    page1 = Page(path="/page1", domain="example.com")
    page2 = Page(path="/page2", domain="example.com")
    
    graph.add_node(page1)
    graph.add_node(page2)
    graph.flush()
    
    edge = Linked(page1, page2, weight=0.9)
    
    # Test merge query generation
    merge_query = edge.merge()
    
    assert "MATCH" in merge_query
    assert "MERGE" in merge_query
    assert "Linked" in merge_query


def test_edge_eq_same_id():
    """Test Edge.__eq__() with same ID."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    class Linked(Edge):
        pass
    
    page1 = Page(path="/page1")
    page2 = Page(path="/page2")
    
    edge1 = Linked(page1, page2, _id=1)
    edge2 = Linked(page1, page2, _id=1)
    
    # Edges with same ID should be equal
    assert edge1 == edge2


def test_edge_eq_different_id_same_nodes():
    """Test Edge.__eq__() with different ID but same nodes and properties."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    class Linked(Edge):
        weight: float = 1.0
    
    page1 = Page(path="/page1")
    page2 = Page(path="/page2")
    
    edge1 = Linked(page1, page2, _id=1, weight=1.0)
    edge2 = Linked(page1, page2, _id=2, weight=1.0)
    
    # Edges with same nodes, relation, and properties should be equal
    assert edge1 == edge2


def test_edge_eq_different_nodes():
    """Test Edge.__eq__() with different nodes."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    class Linked(Edge):
        pass
    
    page1 = Page(path="/page1")
    page2 = Page(path="/page2")
    page3 = Page(path="/page3")
    
    edge1 = Linked(page1, page2)
    edge2 = Linked(page1, page3)
    
    # Edges with different nodes should not be equal
    assert edge1 != edge2


def test_edge_eq_different_properties():
    """Test Edge.__eq__() with different properties."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    class Linked(Edge):
        weight: float = 1.0
    
    page1 = Page(path="/page1")
    page2 = Page(path="/page2")
    
    edge1 = Linked(page1, page2, weight=1.0)
    edge2 = Linked(page1, page2, weight=2.0)
    
    # Edges with different properties should not be equal
    assert edge1 != edge2


def test_edge_hash():
    """Test Edge.__hash__() method."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    class Linked(Edge):
        weight: float = 1.0
    
    page1 = Page(path="/page1")
    page2 = Page(path="/page2")
    
    edge1 = Linked(page1, page2, weight=1.0)
    edge2 = Linked(page1, page2, weight=1.0)
    edge3 = Linked(page1, page2, weight=2.0)
    
    # Equal edges should have same hash
    assert hash(edge1) == hash(edge2)
    
    # Different edges should have different hash (usually)
    # Note: hash collision is possible but unlikely
    assert hash(edge1) != hash(edge3)


def test_edge_alias_classmethod():
    """Test Edge._alias_classmethod() for creating aliased edge classes."""
    class Linked(Edge):
        pass
    
    # Create aliased edge class
    LinkedAlias = Linked.alias("r")
    
    assert hasattr(LinkedAlias, '_alias')
    assert LinkedAlias._alias == "r"
    assert issubclass(LinkedAlias, Linked)


def test_edge_merge_with_string_primary_key(graph):
    """Test Edge.merge() with string primary key nodes."""
    class Page(Node):
        __primary_key__ = "path"
        path: str
    
    class Linked(Edge):
        pass
    
    page1 = Page(path="/page1")
    page2 = Page(path="/page2")
    
    graph.add_node(page1)
    graph.add_node(page2)
    graph.flush()
    
    edge = Linked(page1, page2)
    
    # Test merge query generation
    merge_query = edge.merge()
    
    assert "MATCH" in merge_query
    assert "MERGE" in merge_query


def test_edge_merge_with_edge_properties(graph):
    """Test Edge.merge() with edge properties."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    class Linked(Edge):
        weight: float = 1.0
        discovered_at: str = ""
    
    page1 = Page(path="/page1")
    page2 = Page(path="/page2")
    
    graph.add_node(page1)
    graph.add_node(page2)
    graph.flush()
    
    edge = Linked(page1, page2, weight=0.9, discovered_at="2024-01-01")
    
    # Test merge query generation
    merge_query = edge.merge()
    
    assert "weight" in merge_query or "discovered_at" in merge_query
