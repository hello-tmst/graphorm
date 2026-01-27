"""
Tests for variable-length paths in MATCH patterns.
"""

from graphorm import (
    Edge,
    Node,
    select,
)


def test_variable_length_path_string_pattern(graph):
    """Test variable-length path using string pattern."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    class Linked(Edge):
        pass

    # Create nodes and edges
    page1 = Page(path="/page1")
    page2 = Page(path="/page2")
    page3 = Page(path="/page3")

    graph.add_node(page1)
    graph.add_node(page2)
    graph.add_node(page3)
    graph.flush()

    graph.add_edge(Linked(page1, page2))
    graph.add_edge(Linked(page2, page3))
    graph.flush()

    # Query with variable-length path (1..3 hops)
    stmt = select().match("(start:Page {path: '/page1'})-[:Linked*1..3]->(end:Page)")

    cypher = stmt.to_cypher()

    assert "MATCH" in cypher
    assert "*1..3" in cypher
    assert "Linked" in cypher


def test_variable_length_path_unbounded(graph):
    """Test variable-length path with unbounded upper limit."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    class Linked(Edge):
        pass

    # Query with unbounded variable-length path
    stmt = select().match("(start:Page)-[:Linked*]->(end:Page)")

    cypher = stmt.to_cypher()

    assert "MATCH" in cypher
    assert "*" in cypher
    assert "Linked" in cypher


def test_variable_length_path_exact_length(graph):
    """Test variable-length path with exact length."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    class Linked(Edge):
        pass

    # Query with exact 2 hops
    stmt = select().match("(start:Page)-[:Linked*2]->(end:Page)")

    cypher = stmt.to_cypher()

    assert "MATCH" in cypher
    assert "*2" in cypher


def test_variable_length_path_mixed_with_regular(graph):
    """Test mixing variable-length paths with regular patterns."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    class Linked(Edge):
        pass

    PageAlias = Page.alias("start")

    # Mix string pattern with regular pattern
    stmt = (
        select()
        .match(PageAlias)
        .match("(start)-[:Linked*1..3]->(end:Page)")
        .returns(PageAlias)
    )

    cypher = stmt.to_cypher()

    assert "MATCH" in cypher
    assert "*1..3" in cypher
    assert "(start:Page)" in cypher or "start" in cypher
