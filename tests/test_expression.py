"""
Tests for expression classes: BinaryExpression, AndExpression, OrExpression, 
OrderByExpression, ArithmeticExpression, Function.
"""
import pytest
from graphorm import Node, Property
from graphorm.expression import (
    BinaryExpression,
    AndExpression,
    OrExpression,
    OrderByExpression,
    ArithmeticExpression,
    Function,
    count,
    sum,
    avg,
    min,
    max,
    indegree,
    outdegree,
)


def test_binary_expression_in_operator():
    """Test BinaryExpression with IN operator."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    prop = Page.path
    expr = prop.in_(["/home", "/about", "/contact"])
    
    assert isinstance(expr, BinaryExpression)
    assert expr.operator == "IN"
    assert expr.right == ["/home", "/about", "/contact"]
    
    params = {}
    cypher = expr.to_cypher(params=params, alias_map={Page: "p"})
    
    assert "IN" in cypher
    assert "param_0" in cypher
    assert params["param_0"] == ["/home", "/about", "/contact"]


def test_binary_expression_not_in_operator():
    """Test BinaryExpression with NOT IN operator."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    prop = Page.path
    expr = prop.not_in(["/home", "/about"])
    
    assert isinstance(expr, BinaryExpression)
    assert expr.operator == "NOT IN"
    
    params = {}
    cypher = expr.to_cypher(params=params, alias_map={Page: "p"})
    
    assert "NOT IN" in cypher


def test_binary_expression_contains_operator():
    """Test BinaryExpression with CONTAINS operator."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    prop = Page.path
    expr = prop.contains("home")
    
    assert isinstance(expr, BinaryExpression)
    assert expr.operator == "CONTAINS"
    
    params = {}
    cypher = expr.to_cypher(params=params, alias_map={Page: "p"})
    
    assert "CONTAINS" in cypher


def test_binary_expression_starts_with_operator():
    """Test BinaryExpression with STARTS WITH operator."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    prop = Page.path
    expr = prop.starts_with("/")
    
    assert isinstance(expr, BinaryExpression)
    assert expr.operator == "STARTS WITH"
    
    params = {}
    cypher = expr.to_cypher(params=params, alias_map={Page: "p"})
    
    assert "STARTS WITH" in cypher


def test_binary_expression_ends_with_operator():
    """Test BinaryExpression with ENDS WITH operator."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    prop = Page.path
    expr = prop.ends_with(".html")
    
    assert isinstance(expr, BinaryExpression)
    assert expr.operator == "ENDS WITH"
    
    params = {}
    cypher = expr.to_cypher(params=params, alias_map={Page: "p"})
    
    assert "ENDS WITH" in cypher


def test_binary_expression_is_null_operator():
    """Test BinaryExpression with IS NULL operator."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        error: str = None
    
    prop = Page.error
    expr = prop.is_null()
    
    assert isinstance(expr, BinaryExpression)
    assert expr.operator == "IS NULL"
    
    params = {}
    cypher = expr.to_cypher(params=params, alias_map={Page: "p"})
    
    assert "IS NULL" in cypher
    assert "$" not in cypher  # IS NULL doesn't use parameters


def test_binary_expression_is_not_null_operator():
    """Test BinaryExpression with IS NOT NULL operator."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        error: str = None
    
    prop = Page.error
    expr = prop.is_not_null()
    
    assert isinstance(expr, BinaryExpression)
    assert expr.operator == "IS NOT NULL"
    
    params = {}
    cypher = expr.to_cypher(params=params, alias_map={Page: "p"})
    
    assert "IS NOT NULL" in cypher


def test_and_expression():
    """Test AndExpression combination."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        parsed: bool = False
    
    expr1 = Page.path == "/home"
    expr2 = Page.parsed == False
    
    and_expr = expr1 & expr2
    
    assert isinstance(and_expr, AndExpression)
    assert and_expr.left == expr1
    assert and_expr.right == expr2
    
    params = {}
    cypher = and_expr.to_cypher(params=params, alias_map={Page: "p"})
    
    assert "AND" in cypher
    assert "(" in cypher  # Should be wrapped in parentheses


def test_or_expression():
    """Test OrExpression combination."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        parsed: bool = False
    
    expr1 = Page.path == "/home"
    expr2 = Page.parsed == True
    
    or_expr = expr1 | expr2
    
    assert isinstance(or_expr, OrExpression)
    assert or_expr.left == expr1
    assert or_expr.right == expr2
    
    params = {}
    cypher = or_expr.to_cypher(params=params, alias_map={Page: "p"})
    
    assert "OR" in cypher


def test_order_by_expression_asc():
    """Test OrderByExpression with ASC."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    order_expr = Page.path.asc()
    
    assert isinstance(order_expr, OrderByExpression)
    assert order_expr.direction == "ASC"
    
    cypher = order_expr.to_cypher(alias_map={Page: "p"})
    
    assert "ASC" in cypher


def test_order_by_expression_desc():
    """Test OrderByExpression with DESC."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    order_expr = Page.path.desc()
    
    assert isinstance(order_expr, OrderByExpression)
    assert order_expr.direction == "DESC"
    
    cypher = order_expr.to_cypher(alias_map={Page: "p"})
    
    assert "DESC" in cypher


def test_arithmetic_expression_add():
    """Test ArithmeticExpression with addition."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    func1 = indegree(Page.alias("p"))
    func2 = outdegree(Page.alias("p"))
    
    arith_expr = func1 + func2
    
    assert isinstance(arith_expr, ArithmeticExpression)
    assert arith_expr.operator == "+"
    
    cypher = arith_expr.to_cypher(alias_map={Page.alias("p"): "p"})
    
    assert "+" in cypher


def test_arithmetic_expression_subtract():
    """Test ArithmeticExpression with subtraction."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    func1 = indegree(Page.alias("p"))
    func2 = outdegree(Page.alias("p"))
    
    arith_expr = func1 - func2
    
    assert isinstance(arith_expr, ArithmeticExpression)
    assert arith_expr.operator == "-"
    
    cypher = arith_expr.to_cypher(alias_map={Page.alias("p"): "p"})
    
    assert "-" in cypher


def test_arithmetic_expression_multiply():
    """Test ArithmeticExpression with multiplication."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    func1 = count(Page.alias("p"))
    arith_expr = func1 * 2
    
    assert isinstance(arith_expr, ArithmeticExpression)
    assert arith_expr.operator == "*"
    
    cypher = arith_expr.to_cypher(alias_map={Page.alias("p"): "p"})
    
    assert "*" in cypher


def test_arithmetic_expression_divide():
    """Test ArithmeticExpression with division."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    func1 = count(Page.alias("p"))
    arith_expr = func1 / 2
    
    assert isinstance(arith_expr, ArithmeticExpression)
    assert arith_expr.operator == "/"
    
    cypher = arith_expr.to_cypher(alias_map={Page.alias("p"): "p"})
    
    assert "/" in cypher


def test_arithmetic_expression_label():
    """Test ArithmeticExpression with label."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    func1 = indegree(Page.alias("p"))
    func2 = outdegree(Page.alias("p"))
    
    arith_expr = (func1 + func2).label("total_degree")
    
    assert hasattr(arith_expr, '_label')
    assert arith_expr._label == "total_degree"
    
    cypher = arith_expr.to_cypher(alias_map={Page.alias("p"): "p"})
    
    assert "AS total_degree" in cypher


def test_arithmetic_expression_comparison():
    """Test ArithmeticExpression with comparison operators."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    func1 = count(Page.alias("p"))
    arith_expr = func1 + 1
    
    # Test comparison
    comp_expr = arith_expr > 5
    
    assert isinstance(comp_expr, BinaryExpression)
    assert comp_expr.operator == ">"


def test_function_count():
    """Test Function count()."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    func = count(Page.alias("p"))
    
    assert isinstance(func, Function)
    assert func.name == "COUNT"
    
    cypher = func.to_cypher(alias_map={Page.alias("p"): "p"})
    
    assert "COUNT" in cypher.upper()


def test_function_sum():
    """Test Function sum()."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        views: int = 0
    
    func = sum(Page.alias("p").views)
    
    assert isinstance(func, Function)
    assert func.name == "SUM"
    
    cypher = func.to_cypher(alias_map={Page.alias("p"): "p"})
    
    assert "SUM" in cypher.upper()


def test_function_avg():
    """Test Function avg()."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        views: int = 0
    
    func = avg(Page.alias("p").views)
    
    assert isinstance(func, Function)
    assert func.name == "AVG"
    
    cypher = func.to_cypher(alias_map={Page.alias("p"): "p"})
    
    assert "AVG" in cypher.upper()


def test_function_min():
    """Test Function min()."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        views: int = 0
    
    func = min(Page.alias("p").views)
    
    assert isinstance(func, Function)
    assert func.name == "MIN"
    
    cypher = func.to_cypher(alias_map={Page.alias("p"): "p"})
    
    assert "MIN" in cypher.upper()


def test_function_max():
    """Test Function max()."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        views: int = 0
    
    func = max(Page.alias("p").views)
    
    assert isinstance(func, Function)
    assert func.name == "MAX"
    
    cypher = func.to_cypher(alias_map={Page.alias("p"): "p"})
    
    assert "MAX" in cypher.upper()


def test_function_indegree():
    """Test Function indegree()."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    func = indegree(Page.alias("p"))
    
    assert isinstance(func, Function)
    assert func.name == "INDEGREE"
    
    cypher = func.to_cypher(alias_map={Page.alias("p"): "p"})
    
    assert "INDEGREE" in cypher.upper()


def test_function_outdegree():
    """Test Function outdegree()."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    func = outdegree(Page.alias("p"))
    
    assert isinstance(func, Function)
    assert func.name == "OUTDEGREE"
    
    cypher = func.to_cypher(alias_map={Page.alias("p"): "p"})
    
    assert "OUTDEGREE" in cypher.upper()


def test_function_label():
    """Test Function with label."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    func = count(Page.alias("p")).label("total")
    
    assert hasattr(func, '_label')
    assert func._label == "total"
    
    cypher = func.to_cypher(alias_map={Page.alias("p"): "p"})
    
    assert "AS total" in cypher


def test_function_arithmetic_operations():
    """Test Function with arithmetic operations."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    func1 = count(Page.alias("p"))
    func2 = count(Page.alias("q"))
    
    # Addition
    add_expr = func1 + func2
    assert isinstance(add_expr, ArithmeticExpression)
    
    # Subtraction
    sub_expr = func1 - func2
    assert isinstance(sub_expr, ArithmeticExpression)
    
    # Multiplication
    mul_expr = func1 * 2
    assert isinstance(mul_expr, ArithmeticExpression)
    
    # Division
    div_expr = func1 / 2
    assert isinstance(div_expr, ArithmeticExpression)


def test_function_comparison_operations():
    """Test Function with comparison operations."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    func = count(Page.alias("p"))
    
    # Test all comparison operators
    assert isinstance(func == 5, BinaryExpression)
    assert isinstance(func != 5, BinaryExpression)
    assert isinstance(func < 5, BinaryExpression)
    assert isinstance(func <= 5, BinaryExpression)
    assert isinstance(func > 5, BinaryExpression)
    assert isinstance(func >= 5, BinaryExpression)


def test_expression_chaining():
    """Test chaining expressions with AND and OR."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        parsed: bool = False
    
    expr1 = Page.path == "/home"
    expr2 = Page.parsed == False
    expr3 = Page.path.contains("test")
    
    # Chain with AND
    chained = expr1 & expr2 & expr3
    
    assert isinstance(chained, AndExpression)
    
    # Chain with OR
    or_chained = expr1 | expr2 | expr3
    
    assert isinstance(or_chained, OrExpression)
