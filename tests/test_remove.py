"""
Tests for REMOVE clause.
"""
import pytest
from graphorm import Node, select


def test_remove_property_via_update(graph):
    """Test removing property via update_node()."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        error: str = None
    
    # Create and add node
    page = Page(path="/test", error="404")
    graph.add_node(page)
    graph.flush()
    
    # Remove error property
    result = graph.update_node(page, remove=["error"])
    
    assert result is not None
    
    # Verify property is removed
    updated = graph.get_node(Page(path="/test"))
    assert updated is not None
    assert "error" not in updated.properties or updated.properties.get("error") is None


def test_remove_property_via_query_builder(graph):
    """Test removing property via Query Builder."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        error: str = None
    
    # Create and add node
    page = Page(path="/test", error="404")
    graph.add_node(page)
    graph.flush()
    
    # Remove error property using Query Builder
    PageAlias = Page.alias("p")
    stmt = (
        select()
        .match(PageAlias)
        .where(PageAlias.path == "/test")
        .remove(PageAlias.error.remove())
    )
    
    cypher = stmt.to_cypher()
    
    assert "REMOVE" in cypher
    assert "p.error" in cypher


def test_remove_multiple_properties(graph):
    """Test removing multiple properties."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        error: str = None
        warning: str = None
    
    # Create and add node
    page = Page(path="/test", error="404", warning="slow")
    graph.add_node(page)
    graph.flush()
    
    # Remove multiple properties
    result = graph.update_node(page, remove=["error", "warning"])
    
    assert result is not None
    
    # Verify properties are removed
    updated = graph.get_node(Page(path="/test"))
    assert updated is not None
    assert "error" not in updated.properties or updated.properties.get("error") is None
    assert "warning" not in updated.properties or updated.properties.get("warning") is None


def test_remove_and_update_together(graph):
    """Test removing and updating properties together."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        parsed: bool = False
        error: str = None
    
    # Create and add node
    page = Page(path="/test", parsed=False, error="404")
    graph.add_node(page)
    graph.flush()
    
    # Update parsed and remove error
    result = graph.update_node(page, properties={"parsed": True}, remove=["error"])
    
    assert result is not None
    
    # Verify changes
    updated = graph.get_node(Page(path="/test"))
    assert updated is not None
    assert updated.properties.get("parsed") is True
    assert "error" not in updated.properties or updated.properties.get("error") is None
