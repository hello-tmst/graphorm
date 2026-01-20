"""Tests for lazy loading relationships."""


def test_relationship_outgoing(graph):
    """Test outgoing relationship lazy loading."""
    from graphorm import Node, Edge, Relationship

    class Linked(Edge):
        pass

    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        linked_pages = Relationship(Linked, direction="outgoing")

    # Create nodes and link them
    page1 = Page(path="/page1")
    page2 = Page(path="/page2")
    page3 = Page(path="/page3")
    
    graph.add_node(page1)
    graph.add_node(page2)
    graph.add_node(page3)
    graph.add_edge(Linked(page1, page2))
    graph.add_edge(Linked(page1, page3))
    graph.flush()
    
    # Set graph reference on page1 (normally done by graph when retrieving)
    page1.__graph__ = graph
    
    # Access relationship - should load related pages
    related = page1.linked_pages
    
    assert len(related) == 2
    assert any(p.properties["path"] == "/page2" for p in related)
    assert any(p.properties["path"] == "/page3" for p in related)


def test_relationship_incoming(graph):
    """Test incoming relationship lazy loading."""
    from graphorm import Node, Edge, Relationship

    class Linked(Edge):
        pass

    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        linked_from = Relationship(Linked, direction="incoming")

    # Create nodes and link them
    page1 = Page(path="/page1")
    page2 = Page(path="/page2")
    
    graph.add_node(page1)
    graph.add_node(page2)
    graph.add_edge(Linked(page1, page2))
    graph.flush()
    
    # Set graph reference
    page2.__graph__ = graph
    
    # Access relationship - should load pages that link to page2
    related = page2.linked_from
    
    assert len(related) == 1
    assert related[0].properties["path"] == "/page1"


def test_relationship_caching(graph):
    """Test that relationships are cached after first access."""
    from graphorm import Node, Edge, Relationship

    class Linked(Edge):
        pass

    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        linked_pages = Relationship(Linked, direction="outgoing")

    # Create nodes
    page1 = Page(path="/page1")
    page2 = Page(path="/page2")
    
    graph.add_node(page1)
    graph.add_node(page2)
    graph.add_edge(Linked(page1, page2))
    graph.flush()
    
    page1.__graph__ = graph
    
    # First access - should load
    related1 = page1.linked_pages
    
    # Second access - should use cache
    related2 = page1.linked_pages
    
    # Should be same objects (cached)
    assert related1 is related2
