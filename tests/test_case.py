"""
Tests for CASE expressions.
"""

from graphorm import (
    Node,
    case,
    select,
)


def test_case_expression_simple(graph):
    """Test simple CASE expression."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        parsed: bool = False
        error: str = None

    # Create and add nodes
    page1 = Page(path="/page1", parsed=True, error=None)
    page2 = Page(path="/page2", parsed=False, error="404")
    page3 = Page(path="/page3", parsed=False, error=None)

    graph.add_node(page1)
    graph.add_node(page2)
    graph.add_node(page3)
    graph.flush()

    # Query with CASE
    PageAlias = Page.alias("p")
    priority = case(
        (PageAlias.error.is_not_null(), "high"),
        (PageAlias.parsed == False, "medium"),
        else_="low",
    ).label("priority")

    stmt = select().match(PageAlias).returns(PageAlias, priority)

    cypher = stmt.to_cypher()

    assert "CASE" in cypher
    assert "WHEN" in cypher
    assert "THEN" in cypher
    assert "ELSE" in cypher
    assert "END" in cypher
    assert "AS priority" in cypher


def test_case_expression_without_else(graph):
    """Test CASE expression without ELSE."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        parsed: bool = False

    PageAlias = Page.alias("p")
    status = case(
        (PageAlias.parsed == True, "done"), (PageAlias.parsed == False, "pending")
    )

    stmt = select().match(PageAlias).returns(PageAlias, status)

    cypher = stmt.to_cypher()

    assert "CASE" in cypher
    assert "ELSE" not in cypher  # No ELSE clause


def test_case_expression_execution(graph):
    """Test CASE expression in actual query execution."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        parsed: bool = False

    # Create and add nodes
    page1 = Page(path="/page1", parsed=True)
    page2 = Page(path="/page2", parsed=False)

    graph.add_node(page1)
    graph.add_node(page2)
    graph.flush()

    # Query with CASE
    PageAlias = Page.alias("p")
    status = case((PageAlias.parsed == True, "done"), else_="pending").label("status")

    stmt = select().match(PageAlias).returns(PageAlias.path, status)
    result = graph.execute(stmt)

    assert not result.is_empty()
    assert len(result.result_set) == 2
