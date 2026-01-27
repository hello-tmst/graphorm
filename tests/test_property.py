"""
Tests for Property descriptor class.
"""

from graphorm import (
    Node,
    Property,
)
from graphorm.expression import (
    BinaryExpression,
    OrderByExpression,
)


def test_property_get_class_access():
    """Test Property.__get__() when accessed as class attribute."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    # Accessing as class attribute should return Property object
    prop = Page.path

    assert isinstance(prop, Property)
    assert prop.name == "path"
    assert prop.node_class == Page


def test_property_get_instance_access(graph):
    """Test Property.__get__() when accessed as instance attribute."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        parsed: bool = False

    # Create and add node
    page = Page(path="/test", parsed=True)
    graph.add_node(page)
    graph.flush()

    # Accessing as instance attribute should return property value
    assert page.path == "/test"
    assert page.parsed is True


def test_property_get_fallback_getitem(graph):
    """Test Property.__get__() fallback to __getitem__."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    page = Page(path="/test")

    # Properties should support __getitem__
    assert page.path == "/test"


def test_property_get_fallback_getattr():
    """Test Property.__get__() fallback to getattr."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    page = Page(path="/test")

    # Should work with getattr fallback
    assert page.path == "/test"


def test_property_set(graph):
    """Test Property.__set__() method."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        parsed: bool = False

    page = Page(path="/test")

    # Setting property should update the value
    page.parsed = True

    assert page.parsed is True
    assert page.properties["parsed"] is True


def test_property_set_alias():
    """Test Property.set_alias() method."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    prop = Page.path
    aliased_prop = prop.set_alias("p")

    assert aliased_prop._alias == "p"
    assert aliased_prop is prop  # Should return self


def test_property_like():
    """Test Property.like() method."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    prop = Page.path
    expr = prop.like(".*test.*")

    assert isinstance(expr, BinaryExpression)
    assert expr.operator == "=~"


def test_property_contains():
    """Test Property.contains() method."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    prop = Page.path
    expr = prop.contains("test")

    assert isinstance(expr, BinaryExpression)
    assert expr.operator == "CONTAINS"


def test_property_starts_with():
    """Test Property.starts_with() method."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    prop = Page.path
    expr = prop.starts_with("/")

    assert isinstance(expr, BinaryExpression)
    assert expr.operator == "STARTS WITH"


def test_property_ends_with():
    """Test Property.ends_with() method."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    prop = Page.path
    expr = prop.ends_with(".html")

    assert isinstance(expr, BinaryExpression)
    assert expr.operator == "ENDS WITH"


def test_property_is_null():
    """Test Property.is_null() method."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        error: str = None

    prop = Page.error
    expr = prop.is_null()

    assert isinstance(expr, BinaryExpression)
    assert expr.operator == "IS NULL"


def test_property_is_not_null():
    """Test Property.is_not_null() method."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        error: str = None

    prop = Page.error
    expr = prop.is_not_null()

    assert isinstance(expr, BinaryExpression)
    assert expr.operator == "IS NOT NULL"


def test_property_asc():
    """Test Property.asc() method."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    prop = Page.path
    order_expr = prop.asc()

    assert isinstance(order_expr, OrderByExpression)
    assert order_expr.direction == "ASC"


def test_property_desc():
    """Test Property.desc() method."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    prop = Page.path
    order_expr = prop.desc()

    assert isinstance(order_expr, OrderByExpression)
    assert order_expr.direction == "DESC"


def test_property_to_cypher_with_alias():
    """Test Property.to_cypher() with explicit alias."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    prop = Page.path
    cypher = prop.to_cypher(alias="p")

    assert cypher == "p.path"


def test_property_to_cypher_with_alias_map():
    """Test Property.to_cypher() with alias_map."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    prop = Page.path
    alias_map = {Page: "p"}
    cypher = prop.to_cypher(alias_map=alias_map)

    assert cypher == "p.path"


def test_property_to_cypher_default_alias():
    """Test Property.to_cypher() with default alias."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    prop = Page.path
    cypher = prop.to_cypher()

    # Should use lowercase class name as default
    assert "page.path" in cypher or "path" in cypher


def test_property_comparison_operators():
    """Test Property comparison operators."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        views: int = 0

    prop = Page.path

    # Test all comparison operators
    assert isinstance(prop == "/test", BinaryExpression)
    assert isinstance(prop != "/test", BinaryExpression)
    assert isinstance(prop < "/test", BinaryExpression)
    assert isinstance(prop <= "/test", BinaryExpression)
    assert isinstance(prop > "/test", BinaryExpression)
    assert isinstance(prop >= "/test", BinaryExpression)

    # Test with numeric property
    views_prop = Page.views
    assert isinstance(views_prop == 10, BinaryExpression)
    assert isinstance(views_prop < 100, BinaryExpression)


def test_property_in_operator():
    """Test Property.in_() method."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    prop = Page.path
    expr = prop.in_(["/home", "/about"])

    assert isinstance(expr, BinaryExpression)
    assert expr.operator == "IN"
    assert expr.right == ["/home", "/about"]


def test_property_not_in_operator():
    """Test Property.not_in() method."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    prop = Page.path
    expr = prop.not_in(["/home", "/about"])

    assert isinstance(expr, BinaryExpression)
    assert expr.operator == "NOT IN"
    assert expr.right == ["/home", "/about"]


def test_property_with_aliased_class():
    """Test Property with aliased Node class."""

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    # Create aliased class
    PageAlias = Page.alias("p")

    # Property should work with aliased class
    prop = PageAlias.path

    assert isinstance(prop, Property)
    cypher = prop.to_cypher()

    # Should use alias from aliased class
    assert "p.path" in cypher or "path" in cypher
