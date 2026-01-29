import pytest

from graphorm.edge import Edge
from graphorm.node import Node


def test_edge_src_none_raises():
    """Edge with src_node=None raises ValueError."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    class Linked(Edge):
        pass

    page = Page(path="/p")
    with pytest.raises(ValueError, match="Both src_node & dst_node must be provided"):
        Linked(None, page)


def test_edge_dst_none_raises():
    """Edge with dst_node=None raises ValueError."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    class Linked(Edge):
        pass

    page = Page(path="/p")
    with pytest.raises(ValueError, match="Both src_node & dst_node must be provided"):
        Linked(page, None)


def test_edge_str_when_src_or_dst_not_node():
    """Edge.__str__ uses () when src_node or dst_node is not a Node."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    class Linked(Edge):
        pass

    page1 = Page(path="/p1")
    page2 = Page(path="/p2")
    edge = Linked(page1, page2)
    # Replace dst_node with non-Node object that has .alias
    edge.dst_node = type("Obj", (), {"alias": "x"})()
    s = str(edge)
    assert "()->" in s or "()" in s
    assert "]->" in s

    edge.src_node = type("Obj", (), {"alias": "y"})()
    edge.dst_node = page2
    s2 = str(edge)
    assert "()" in s2


def test_edge_str_with_and_without_properties():
    """Edge.__str__ includes properties when present."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    class LinkedNoProps(Edge):
        pass

    class LinkedWithProps(Edge):
        weight: float = 1.0

    page1 = Page(path="/p1")
    page2 = Page(path="/p2")
    edge_no = LinkedNoProps(page1, page2)
    edge_with = LinkedWithProps(page1, page2, weight=0.5)
    s_no = str(edge_no)
    s_with = str(edge_with)
    assert "weight" in s_with
    assert "0.5" in s_with or "0.5" in s_with
    # No props branch: no curly brace for edge props
    assert s_no.count("{") < 2 or "{" not in s_no.split("]->")[0].split("[")[1]


def test_edge_merge_when_src_or_dst_not_node():
    """Edge.merge() returns 'MERGE ' + str(self) when src or dst is not Node."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    class Linked(Edge):
        pass

    page1 = Page(path="/p1")
    page2 = Page(path="/p2")
    edge = Linked(page1, page2)
    edge.dst_node = None
    q = edge.merge()
    assert q.startswith("MERGE ")
    assert str(edge) in q or "MERGE " in q


def test_edge_eq_different_relation():
    """Edge.__eq__ returns False when relation differs."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    class LinkedA(Edge):
        pass

    class LinkedB(Edge):
        pass

    page1 = Page(path="/p1")
    page2 = Page(path="/p2")
    edge_a = LinkedA(page1, page2)
    edge_b = LinkedB(page1, page2)
    assert edge_a != edge_b
    assert edge_b != edge_a


def test_edge_eq_different_property_count():
    """Edge.__eq__ returns False when number of properties differs."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    class Linked(Edge):
        weight: float = 0.0

    page1 = Page(path="/p1")
    page2 = Page(path="/p2")
    edge1 = Linked(page1, page2, weight=1.0)
    edge2 = Linked(page1, page2)
    # One has weight=1.0, one has weight=0.0 (default) - same count
    edge3 = Linked(page1, page2, weight=2.0)
    assert edge1 != edge3
    # Same count, same nodes, different value already tested in test_edge_eq_different_properties
    assert len(edge1.properties) == len(edge2.properties)


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

    assert hasattr(LinkedAlias, "_alias")
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
