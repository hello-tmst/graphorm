"""
Tests for WITH clause in queries.
"""
import pytest
from graphorm import Node, Edge, select, outdegree, indegree


def test_with_clause_simple(graph):
    """Test simple WITH clause."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    # Create and add nodes
    page1 = Page(path="/page1")
    page2 = Page(path="/page2")
    graph.add_node(page1)
    graph.add_node(page2)
    graph.flush()
    
    # Query with WITH
    PageAlias = Page.alias("p")
    stmt = select().match(PageAlias).with_(PageAlias, outdegree(PageAlias).label("deg"))
    
    cypher = stmt.to_cypher()
    
    assert "WITH" in cypher
    assert "OUTDEGREE" in cypher.upper()
    assert "AS deg" in cypher


def test_with_clause_filtering(graph):
    """Test WITH clause for filtering after aggregation."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    # Create and add nodes
    page1 = Page(path="/page1")
    page2 = Page(path="/page2")
    page3 = Page(path="/page3")
    graph.add_node(page1)
    graph.add_node(page2)
    graph.add_node(page3)
    graph.flush()
    
    # Query: find pages with degree > 0
    PageAlias = Page.alias("p")
    deg = outdegree(PageAlias).label("deg")
    
    stmt = (
        select()
        .match(PageAlias)
        .with_(PageAlias, deg)
        .where(deg > 0)
        .returns(PageAlias, "deg")
    )
    
    cypher = stmt.to_cypher()
    
    assert "WITH" in cypher
    assert "WHERE" in cypher
    assert "deg >" in cypher or "deg >=" in cypher


def test_with_clause_multiple_expressions(graph):
    """Test WITH clause with multiple expressions."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    # Create and add nodes
    page1 = Page(path="/page1")
    graph.add_node(page1)
    graph.flush()
    
    # Query with multiple WITH expressions
    PageAlias = Page.alias("p")
    stmt = (
        select()
        .match(PageAlias)
        .with_(PageAlias, outdegree(PageAlias).label("out"), indegree(PageAlias).label("in"))
        .returns(PageAlias, "out", "in")
    )
    
    cypher = stmt.to_cypher()
    
    assert "WITH" in cypher
    assert "OUTDEGREE" in cypher.upper()
    assert "INDEGREE" in cypher.upper()
    assert "AS out" in cypher
    assert "AS in" in cypher
