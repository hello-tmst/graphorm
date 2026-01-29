"""
Tests for Delete statement and delete operations.
"""

from graphorm import (
    Edge,
    Node,
    delete,
)


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
    stmt = (
        delete(Page)
        .match(Page.alias("p"))
        .where(Page.path == "/test")
        .returns(Page.path)
    )
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
    stmt = (
        delete(PageAlias, LinkedAlias)
        .match((PageAlias, LinkedAlias, Page.alias("p2")))
        .where(PageAlias.path == "/page1")
    )

    result = graph.execute(stmt)

    assert result is not None


# --- Cypher builder coverage (to_cypher only, no execute) ---


def test_delete_optional_match_to_cypher():
    """Delete with OPTIONAL MATCH produces OPTIONAL MATCH in Cypher."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    class Other(Node):
        __primary_key__ = ["id"]
        id: str

    stmt = (
        delete(Page)
        .match(Page.alias("p"))
        .optional_match(Other.alias("o"))
        .where(Page.path == "/x")
    )
    cypher = stmt.to_cypher()
    assert "OPTIONAL MATCH" in cypher
    assert "MATCH" in cypher
    assert "DELETE" in cypher


def test_delete_raw_match_to_cypher():
    """Delete with RAW match uses raw pattern in MATCH."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    stmt = delete(Page).match(("RAW", "(p:Page)")).where(Page.path == "/x")
    cypher = stmt.to_cypher()
    assert "MATCH" in cypher
    assert "(p:Page)" in cypher
    assert "DELETE" in cypher


def test_delete_without_match_from_entities_to_cypher():
    """Delete without match() builds MATCH from entities."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    PageAlias = Page.alias("p")
    stmt = delete(PageAlias).where(Page.path == "/x")
    cypher = stmt.to_cypher()
    assert "MATCH" in cypher
    assert "DELETE" in cypher
    assert "p" in cypher


def test_delete_returns_string_to_cypher():
    """Delete with .returns(str) produces RETURN in Cypher."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    stmt = (
        delete(Page)
        .match(Page.alias("p"))
        .where(Page.path == "/x")
        .returns("p.path")
    )
    cypher = stmt.to_cypher()
    assert "RETURN" in cypher
    assert "p.path" in cypher


def test_delete_returns_function_to_cypher():
    """Delete with .returns(Function) uses to_cypher(alias_map=...)."""
    from graphorm import count

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    stmt = (
        delete(Page)
        .match(Page.alias("p"))
        .where(Page.path == "/x")
        .returns(count(Page.path))
    )
    cypher = stmt.to_cypher()
    assert "RETURN" in cypher


def test_delete_returns_type_with_alias_to_cypher():
    """Delete with .returns(Node alias type) uses _alias in RETURN."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    PageAlias = Page.alias("p")
    stmt = (
        delete(Page)
        .match(PageAlias)
        .where(Page.path == "/x")
        .returns(PageAlias)
    )
    cypher = stmt.to_cypher()
    assert "RETURN" in cypher
    assert "p" in cypher


def test_delete_returns_expression_with_to_cypher_no_name_to_cypher():
    """Delete with .returns(expression needing params/alias_map) in RETURN."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    stmt = (
        delete(Page)
        .match(Page.alias("p"))
        .where(Page.path == "/x")
        .returns(Page.path)
    )
    cypher = stmt.to_cypher()
    assert "RETURN" in cypher


def test_delete_empty_delete_targets_fallback_to_n():
    """Delete with only RAW match (no extractable aliases) falls back to DELETE n."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    stmt = delete().match(("RAW", "(n:Page)")).where("n.path = '/x'")
    cypher = stmt.to_cypher()
    assert "DELETE" in cypher
    assert " n " in cypher or cypher.strip().endswith("n")


def test_delete_tuple_match_src_edge_dst_to_cypher():
    """Delete with tuple (src, edge, dst) match puts node and edge aliases in DELETE."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    class Linked(Edge):
        pass

    PageAlias = Page.alias("a")
    LinkedAlias = Linked.alias("r")
    Page2Alias = Page.alias("b")
    stmt = (
        delete(PageAlias, LinkedAlias)
        .match((PageAlias, LinkedAlias, Page2Alias))
        .where(PageAlias.path == "/x")
    )
    cypher = stmt.to_cypher()
    assert "DELETE" in cypher
    assert "a" in cypher
    assert "r" in cypher
    assert "b" in cypher
