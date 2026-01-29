"""
Tests for graphorm.utils: quote_string, format_cypher_value, get_pk_fields, format_pk_cypher_map.
"""

from graphorm.utils import (
    format_cypher_value,
    format_pk_cypher_map,
    get_pk_fields,
    quote_string,
)


def test_quote_string_bytes_decodes():
    """quote_string with bytes decodes and quotes."""
    result = quote_string(b"hello")
    assert result == '"hello"'


def test_quote_string_non_str_returns_unchanged():
    """quote_string with non-str returns value unchanged."""
    result = quote_string(123)
    assert result == 123


def test_quote_string_empty_string():
    """quote_string with empty string returns '""'."""
    result = quote_string("")
    assert result == '""'


def test_format_cypher_value_numeric():
    """format_cypher_value with int/float returns str(value)."""
    assert format_cypher_value(42) == "42"
    assert format_cypher_value(3.14) == "3.14"


def test_get_pk_fields_not_str_or_list_returns_empty():
    """get_pk_fields with __primary_key__ not str or list returns []."""
    class Obj:
        __primary_key__ = ("a", "b")  # tuple

    result = get_pk_fields(Obj())
    assert result == []


def test_format_pk_cypher_map_props_without_get():
    """format_pk_cypher_map with properties that have no .get uses [] access."""
    class PropsNoGet:
        """Object with __getitem__/__contains__ but no .get."""

        def __contains__(self, key):
            return key == "k"

        def __getitem__(self, key):
            return "v" if key == "k" else None

    class Obj:
        __primary_key__ = ["k"]
        properties = PropsNoGet()

    result = format_pk_cypher_map(Obj())
    assert "k" in result
    assert "v" in result


def test_format_pk_cypher_map_props_key_not_present():
    """format_pk_cypher_map when key not in props uses None."""
    class Obj:
        __primary_key__ = ["missing"]
        properties = {}  # missing key

    result = format_pk_cypher_map(Obj())
    assert "missing" in result
    assert "null" in result
