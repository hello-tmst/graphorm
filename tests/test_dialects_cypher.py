"""
Tests for dialects/cypher stub (CypherQuery).
"""

from graphorm.dialects.cypher import CypherQuery


def test_cypher_query_merge_returns_self():
    """CypherQuery.merge(item) returns the same instance."""
    q = CypherQuery()
    result = q.merge("any_item")
    assert result is q
