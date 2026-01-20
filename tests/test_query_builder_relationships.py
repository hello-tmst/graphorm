"""Tests for MATCH patterns with relationships in Query Builder."""

from graphorm import Node, Edge, select


def test_match_single_node(graph):
    """Test simple MATCH with single node."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    stmt = select().match(Page.alias("a"))
    cypher = stmt.to_cypher()
    
    assert "MATCH" in cypher
    assert "(a:Page)" in cypher
    assert "RETURN" in cypher


def test_match_relationship_pattern(graph):
    """Test MATCH with relationship pattern (src, edge, dst)."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    class Linked(Edge):
        pass
    
    stmt = select().match(
        (Page.alias("a"), Linked.alias("r"), Page.alias("b"))
    )
    cypher = stmt.to_cypher()
    
    assert "MATCH" in cypher
    assert "(a:Page)" in cypher
    assert "[r:Linked]" in cypher
    assert "(b:Page)" in cypher
    assert "-[r:Linked]-" in cypher or "-[r:Linked]->" in cypher


def test_match_relationship_with_where(graph):
    """Test MATCH with relationship pattern and WHERE clause."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    class Linked(Edge):
        pass
    
    PageA = Page.alias("a")
    PageB = Page.alias("b")
    
    stmt = select().match(
        (PageA, Linked.alias("r"), PageB)
    ).where(
        PageA.path == "/home"
    )
    cypher = stmt.to_cypher()
    
    assert "MATCH" in cypher
    assert "WHERE" in cypher
    assert "a.path" in cypher


def test_match_relationship_with_returns(graph):
    """Test MATCH with relationship pattern and explicit RETURN."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    class Linked(Edge):
        pass
    
    PageA = Page.alias("a")
    LinkedR = Linked.alias("r")
    PageB = Page.alias("b")
    
    stmt = select().match(
        (PageA, LinkedR, PageB)
    ).returns(
        PageA,
        LinkedR,
        PageB
    )
    cypher = stmt.to_cypher()
    
    assert "MATCH" in cypher
    assert "RETURN" in cypher
    assert "a" in cypher
    assert "r" in cypher
    assert "b" in cypher


def test_match_multiple_patterns(graph):
    """Test MATCH with multiple patterns."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    class Linked(Edge):
        pass
    
    PageA = Page.alias("a")
    PageB = Page.alias("b")
    PageC = Page.alias("c")
    
    stmt = select().match(
        PageA,
        (PageA, Linked.alias("r1"), PageB),
        (PageB, Linked.alias("r2"), PageC)
    )
    cypher = stmt.to_cypher()
    
    assert "MATCH" in cypher
    assert "(a:Page)" in cypher
    assert "(b:Page)" in cypher
    assert "(c:Page)" in cypher


def test_match_string_pattern(graph):
    """Test MATCH with string pattern for complex cases."""
    stmt = select().match("(a:Page)-[r:Linked]->(b:Page)")
    cypher = stmt.to_cypher()
    
    assert "MATCH" in cypher
    assert "(a:Page)-[r:Linked]->(b:Page)" in cypher


def test_match_relationship_with_property_access(graph):
    """Test accessing properties from relationship pattern in WHERE."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        title: str = ""
    
    class Linked(Edge):
        weight: float = 1.0
    
    PageA = Page.alias("a")
    LinkedR = Linked.alias("r")
    PageB = Page.alias("b")
    
    stmt = select().match(
        (PageA, LinkedR, PageB)
    ).where(
        (PageA.path == "/home") & (PageB.title.contains("About"))
    )
    cypher = stmt.to_cypher()
    
    assert "MATCH" in cypher
    assert "WHERE" in cypher
    assert "a.path" in cypher
    assert "b.title" in cypher


def test_match_relationship_execution(graph):
    """Test executing a query with relationship pattern."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    class Linked(Edge):
        pass
    
    # Create test data
    page1 = Page(path="/home")
    page2 = Page(path="/about")
    graph.add_node(page1)
    graph.add_node(page2)
    graph.add_edge(Linked(page1, page2))
    graph.flush()
    
    # Query with relationship pattern
    PageA = Page.alias("a")
    PageB = Page.alias("b")
    
    stmt = select().match(
        (PageA, Linked.alias("r"), PageB)
    ).returns(
        PageA,
        PageB
    )
    
    result = graph.execute(stmt)
    
    assert not result.is_empty()
    assert len(result.result_set) > 0
