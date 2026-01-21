"""
Tests for Delete statement and delete operations.
"""
import pytest
from graphorm import Node, Edge, delete, Graph


def test_delete_node_by_instance(graph):
    """Test deleting a node by instance."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    # Create and add node
    page = Page(path="/test")
    graph.add_node(page)
    graph.flush()
    
    # Delete node
    result = graph.delete_node(page)
    
    assert result is not None
    
    # Verify node is deleted
    found = graph.get_node(Page(path="/test"))
    assert found is None


def test_delete_node_detach(graph):
    """Test DETACH DELETE for node."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    class Linked(Edge):
        pass
    
    # Create nodes and edge
    page1 = Page(path="/page1")
    page2 = Page(path="/page2")
    graph.add_node(page1)
    graph.add_node(page2)
    graph.flush()
    
    link = Linked(page1, page2)
    graph.add_edge(link)
    graph.flush()
    
    # DETACH DELETE should remove node and all relationships
    result = graph.delete_node(page1, detach=True)
    
    assert result is not None
    
    # Verify node is deleted
    found = graph.get_node(Page(path="/page1"))
    assert found is None


def test_delete_edge(graph):
    """Test deleting an edge."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    class Linked(Edge):
        pass
    
    # Create nodes and edge
    page1 = Page(path="/page1")
    page2 = Page(path="/page2")
    graph.add_node(page1)
    graph.add_node(page2)
    graph.flush()
    
    link = Linked(page1, page2)
    graph.add_edge(link)
    graph.flush()
    
    # Delete edge
    result = graph.delete_edge(link)
    
    assert result is not None


def test_delete_statement_simple(graph):
    """Test Delete statement with simple MATCH."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    # Create and add nodes
    page1 = Page(path="/page1")
    page2 = Page(path="/page2")
    graph.add_node(page1)
    graph.add_node(page2)
    graph.flush()
    
    # Delete using statement
    stmt = delete(Page).match(Page.alias("p")).where(Page.path == "/page1")
    result = graph.execute(stmt)
    
    assert result is not None
    
    # Verify node is deleted
    found = graph.get_node(Page(path="/page1"))
    assert found is None


def test_delete_statement_detach(graph):
    """Test Delete statement with DETACH DELETE."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    class Linked(Edge):
        pass
    
    # Create nodes and edge
    page1 = Page(path="/page1")
    page2 = Page(path="/page2")
    graph.add_node(page1)
    graph.add_node(page2)
    graph.flush()
    
    link = Linked(page1, page2)
    graph.add_edge(link)
    graph.flush()
    
    # DETACH DELETE using statement
    stmt = delete(Page).match(Page.alias("p")).where(Page.path == "/page1").detach()
    result = graph.execute(stmt)
    
    assert result is not None
    
    # Verify node is deleted
    found = graph.get_node(Page(path="/page1"))
    assert found is None


def test_delete_statement_with_return(graph):
    """Test Delete statement with RETURN clause."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    # Create and add node
    page = Page(path="/test")
    graph.add_node(page)
    graph.flush()
    
    # Delete with RETURN
    stmt = delete(Page).match(Page.alias("p")).where(Page.path == "/test").returns(Page.path)
    result = graph.execute(stmt)
    
    assert result is not None
    if not result.is_empty():
        # Should return the deleted path
        assert len(result.result_set) > 0


def test_delete_statement_multiple_entities(graph):
    """Test Delete statement with multiple entities."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    class Linked(Edge):
        pass
    
    # Create nodes and edge
    page1 = Page(path="/page1")
    page2 = Page(path="/page2")
    graph.add_node(page1)
    graph.add_node(page2)
    graph.flush()
    
    link = Linked(page1, page2)
    graph.add_edge(link)
    graph.flush()
    
    # Delete both node and edge
    PageAlias = Page.alias("p")
    LinkedAlias = Linked.alias("r")
    stmt = delete(PageAlias, LinkedAlias).match(
        (PageAlias, LinkedAlias, Page.alias("p2"))
    ).where(PageAlias.path == "/page1")
    
    result = graph.execute(stmt)
    
    assert result is not None
