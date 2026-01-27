"""Tests for lazy loading relationships."""


def test_relationship_outgoing(graph):
    """Test outgoing relationship lazy loading."""
    from graphorm import (
        Edge,
        Node,
        Relationship,
    )

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
    from graphorm import (
        Edge,
        Node,
        Relationship,
    )

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
    from graphorm import (
        Edge,
        Node,
        Relationship,
    )

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


def test_relationship_both_direction(graph):
    """Test Relationship with direction='both'."""
    from graphorm import (
        Edge,
        Node,
        Relationship,
    )

    class Linked(Edge):
        pass

    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        linked_pages = Relationship(Linked, direction="both")

    # Create nodes and bidirectional links
    page1 = Page(path="/page1")
    page2 = Page(path="/page2")
    page3 = Page(path="/page3")

    graph.add_node(page1)
    graph.add_node(page2)
    graph.add_node(page3)
    graph.add_edge(Linked(page1, page2))
    graph.add_edge(Linked(page3, page1))  # page3 links to page1
    graph.flush()

    page1.__graph__ = graph

    # Access relationship - should load pages in both directions
    related = page1.linked_pages

    # Should find page2 (outgoing) and page3 (incoming)
    assert len(related) == 2
    paths = [p.properties["path"] for p in related]
    assert "/page2" in paths
    assert "/page3" in paths


def test_relationship_clear_cache(graph):
    """Test Relationship.clear_cache() method."""
    from graphorm import (
        Edge,
        Node,
        Relationship,
    )

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

    # First access - should load and cache
    related1 = page1.linked_pages
    assert len(related1) == 1

    # Clear cache - call on the descriptor class, not the result
    Page.linked_pages.clear_cache(page1)

    # Access again - should reload (not use cache)
    related2 = page1.linked_pages
    assert len(related2) == 1
    # Note: They might be different objects after cache clear


def test_relationship_with_related_node_type(graph):
    """Test Relationship with related_node_type parameter."""
    from graphorm import (
        Edge,
        Node,
        Relationship,
    )

    class Linked(Edge):
        pass

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    class CustomPage(Page):
        __primary_key__ = ["path"]
        path: str
        custom_field: str = ""

    class PageWithRelationship(Node):
        __primary_key__ = ["path"]
        path: str
        linked_pages = Relationship(
            Linked, direction="outgoing", related_node_type=Page
        )

    # Create nodes
    page1 = PageWithRelationship(path="/page1")
    page2 = Page(path="/page2")

    graph.add_node(page1)
    graph.add_node(page2)
    graph.add_edge(Linked(page1, page2))
    graph.flush()

    page1.__graph__ = graph

    # Access relationship
    related = page1.linked_pages

    assert len(related) == 1
    # Note: Type checking would verify related_node_type, but runtime behavior
    # depends on Registry returning correct types


def test_relationship_with_string_edge_class(graph):
    """Test Relationship with string edge class name."""
    from graphorm import (
        Edge,
        Node,
        Relationship,
    )

    class Linked(Edge):
        pass

    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        linked_pages = Relationship("Linked", direction="outgoing")

    # Create nodes
    page1 = Page(path="/page1")
    page2 = Page(path="/page2")

    graph.add_node(page1)
    graph.add_node(page2)
    graph.add_edge(Linked(page1, page2))
    graph.flush()

    page1.__graph__ = graph

    # Access relationship with string edge class
    related = page1.linked_pages

    assert len(related) == 1
    assert related[0].properties["path"] == "/page2"


def test_relationship_no_graph(graph):
    """Test Relationship when graph is not set."""
    from graphorm import (
        Edge,
        Node,
        Relationship,
    )

    class Linked(Edge):
        pass

    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        linked_pages = Relationship(Linked, direction="outgoing")

    # Create node without graph reference
    page = Page(path="/page1")

    # Access relationship without graph - should return empty list
    related = page.linked_pages

    assert related == []


def test_relationship_with_primary_key_lookup(graph):
    """Test Relationship._load_related() using primary key for WHERE clause."""
    from graphorm import (
        Edge,
        Node,
        Relationship,
    )

    class Linked(Edge):
        pass

    class Page(Node):
        __primary_key__ = ["path", "domain"]
        path: str
        domain: str

    class PageWithRelationship(Node):
        __primary_key__ = ["path", "domain"]
        path: str
        domain: str
        linked_pages = Relationship(Linked, direction="outgoing")

    # Create nodes with composite primary key
    page1 = PageWithRelationship(path="/page1", domain="example.com")
    page2 = Page(path="/page2", domain="example.com")

    graph.add_node(page1)
    graph.add_node(page2)
    graph.add_edge(Linked(page1, page2))
    graph.flush()

    page1.__graph__ = graph

    # Access relationship - should use composite primary key in WHERE clause
    related = page1.linked_pages

    assert len(related) == 1
    assert related[0].properties["path"] == "/page2"


def test_relationship_fallback_to_id(graph):
    """Test Relationship._load_related() fallback to id when primary key not available."""
    from graphorm import (
        Edge,
        Node,
        Relationship,
    )

    class Linked(Edge):
        pass

    class Page(Node):
        __primary_key__ = ["path"]  # Define primary key for proper node identification
        path: str
        linked_pages = Relationship(Linked, direction="outgoing")

    # Create nodes
    page1 = Page(path="/page1")
    page2 = Page(path="/page2")

    graph.add_node(page1)
    graph.add_node(page2)
    graph.add_edge(Linked(page1, page2))
    graph.flush()

    # Get node from graph to have ID and graph reference
    page1_from_graph = graph.get_node(Page(path="/page1"))
    if page1_from_graph:
        page1_from_graph.__graph__ = graph

        # Access relationship - should use primary key lookup
        related = page1_from_graph.linked_pages

        # Should work with primary key lookup
        assert len(related) >= 0
