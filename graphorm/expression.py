"""
Expression classes for building Cypher query conditions.

This module provides classes for representing WHERE conditions, ORDER BY clauses,
and combining expressions with AND/OR operators.
"""
from typing import Any, Dict


class BinaryExpression:
    """
    Represents a binary expression (left operator right) for WHERE conditions.
    
    Examples:
        Page.path == "/home"  # BinaryExpression(Page.path, "=", "/home")
        Page.parsed != False   # BinaryExpression(Page.parsed, "!=", False)
    """
    
    def __init__(self, left: Any, operator: str, right: Any):
        """
        Initialize binary expression.
        
        :param left: Left operand (usually a Property)
        :param operator: Operator string ("=", "!=", "<", ">", "IN", etc.)
        :param right: Right operand (value or another expression)
        """
        self.left = left
        self.operator = operator
        self.right = right
    
    def __and__(self, other: "BinaryExpression") -> "AndExpression":
        """
        AND operator: combine two expressions with AND.
        
        :param other: Another BinaryExpression
        :return: AndExpression combining both expressions
        """
        return AndExpression(self, other)
    
    def __or__(self, other: "BinaryExpression") -> "OrExpression":
        """
        OR operator: combine two expressions with OR.
        
        :param other: Another BinaryExpression
        :return: OrExpression combining both expressions
        """
        return OrExpression(self, other)
    
    def to_cypher(self, params: Dict[str, Any] = None, alias_map: Dict[Any, str] = None) -> str:
        """
        Generate Cypher string representation of this expression.
        
        :param params: Dictionary to store parameter values (for parameterized queries)
        :param alias_map: Optional mapping of node classes to aliases
        :return: Cypher string
        """
        if params is None:
            params = {}
        if alias_map is None:
            alias_map = {}
        
        # Get left side Cypher representation
        if hasattr(self.left, 'to_cypher'):
            # If left is a Property, pass alias_map
            if hasattr(self.left, 'to_cypher') and hasattr(self.left, 'node_class'):
                left_str = self.left.to_cypher(alias_map=alias_map)
            else:
                left_str = self.left.to_cypher()
        else:
            left_str = str(self.left)
        
        # Handle special operators
        if self.operator == "IN":
            param_name = self._add_param_to_dict(self.right, params)
            return f"{left_str} IN ${param_name}"
        elif self.operator == "NOT IN":
            param_name = self._add_param_to_dict(self.right, params)
            return f"{left_str} NOT IN ${param_name}"
        elif self.operator == "=~":
            param_name = self._add_param_to_dict(self.right, params)
            return f"{left_str} =~ ${param_name}"
        elif self.operator == "CONTAINS":
            param_name = self._add_param_to_dict(self.right, params)
            return f"{left_str} CONTAINS ${param_name}"
        elif self.operator == "STARTS WITH":
            param_name = self._add_param_to_dict(self.right, params)
            return f"{left_str} STARTS WITH ${param_name}"
        elif self.operator == "ENDS WITH":
            param_name = self._add_param_to_dict(self.right, params)
            return f"{left_str} ENDS WITH ${param_name}"
        elif self.operator == "IS NULL":
            return f"{left_str} IS NULL"
        elif self.operator == "IS NOT NULL":
            return f"{left_str} IS NOT NULL"
        else:
            # Standard operators: =, <>, <, <=, >, >=
            # Note: Cypher uses <> instead of !=
            param_name = self._add_param_to_dict(self.right, params)
            return f"{left_str} {self.operator} ${param_name}"
    
    def _add_param_to_dict(self, value: Any, params: Dict[str, Any]) -> str:
        """
        Add parameter value to params dict and return parameter name.
        
        :param value: Value to add as parameter
        :param params: Dictionary to store parameters
        :return: Parameter name (e.g., "param_0")
        """
        # Generate unique parameter name
        param_num = len(params)
        param_name = f"param_{param_num}"
        params[param_name] = value
        return param_name


class AndExpression:
    """
    Represents AND combination of two expressions.
    
    Example:
        (Page.parsed == False) & (Page.path != "")  # AndExpression
    """
    
    def __init__(self, left: BinaryExpression, right: BinaryExpression):
        """
        Initialize AND expression.
        
        :param left: Left expression
        :param right: Right expression
        """
        self.left = left
        self.right = right
    
    def __and__(self, other: BinaryExpression) -> "AndExpression":
        """Combine with another expression using AND."""
        return AndExpression(self, other)
    
    def __or__(self, other: BinaryExpression) -> "OrExpression":
        """Combine with another expression using OR."""
        return OrExpression(self, other)
    
    def to_cypher(self, params: Dict[str, Any] = None, alias_map: Dict[Any, str] = None) -> str:
        """
        Generate Cypher string representation.
        
        :param params: Dictionary to store parameter values
        :param alias_map: Optional mapping of node classes to aliases
        :return: Cypher string
        """
        if params is None:
            params = {}
        if alias_map is None:
            alias_map = {}
        
        left_str = self.left.to_cypher(params, alias_map) if hasattr(self.left, 'to_cypher') else str(self.left)
        right_str = self.right.to_cypher(params, alias_map) if hasattr(self.right, 'to_cypher') else str(self.right)
        
        return f"({left_str} AND {right_str})"


class OrExpression:
    """
    Represents OR combination of two expressions.
    
    Example:
        (Page.parsed == False) | (Page.path == "/home")  # OrExpression
    """
    
    def __init__(self, left: BinaryExpression, right: BinaryExpression = None):
        """
        Initialize OR expression.
        
        :param left: Left expression
        :param right: Right expression (optional, can be set later)
        """
        self.left = left
        self.right = right
    
    def __or__(self, other: BinaryExpression) -> "OrExpression":
        """Combine with another expression using OR."""
        if self.right is None:
            return OrExpression(self.left, other)
        else:
            # Chain OR expressions
            return OrExpression(self, other)
    
    def __and__(self, other: BinaryExpression) -> "AndExpression":
        """Combine with another expression using AND."""
        return AndExpression(self, other)
    
    def to_cypher(self, params: Dict[str, Any] = None, alias_map: Dict[Any, str] = None) -> str:
        """
        Generate Cypher string representation.
        
        :param params: Dictionary to store parameter values
        :param alias_map: Optional mapping of node classes to aliases
        :return: Cypher string
        """
        if params is None:
            params = {}
        if alias_map is None:
            alias_map = {}
        
        if self.right is None:
            # Single expression
            return self.left.to_cypher(params, alias_map) if hasattr(self.left, 'to_cypher') else str(self.left)
        
        left_str = self.left.to_cypher(params, alias_map) if hasattr(self.left, 'to_cypher') else str(self.left)
        right_str = self.right.to_cypher(params, alias_map) if hasattr(self.right, 'to_cypher') else str(self.right)
        
        return f"({left_str} OR {right_str})"


class OrderByExpression:
    """
    Represents ORDER BY expression for sorting.
    
    Example:
        Page.path.asc()   # OrderByExpression(Page.path, "ASC")
        Page.path.desc()  # OrderByExpression(Page.path, "DESC")
    """
    
    def __init__(self, expression: Any, direction: str = "ASC"):
        """
        Initialize ORDER BY expression.
        
        :param expression: Expression to order by (usually a Property)
        :param direction: Sort direction ("ASC" or "DESC")
        """
        self.expression = expression
        self.direction = direction.upper()
    
    def to_cypher(self) -> str:
        """
        Generate Cypher string representation.
        
        :return: Cypher string
        """
        if hasattr(self.expression, 'to_cypher'):
            expr_str = self.expression.to_cypher()
        else:
            expr_str = str(self.expression)
        
        return f"{expr_str} {self.direction}"


class Function:
    """
    Represents a Cypher function call.
    
    Example:
        func.count(Page)  # Function("count", [Page])
    """
    
    def __init__(self, name: str, *args: Any):
        """
        Initialize function call.
        
        :param name: Function name (e.g., "count", "sum", "avg")
        :param args: Function arguments
        """
        self.name = name.upper()
        self.args = args
    
    def label(self, alias: str) -> "Function":
        """
        Add label (alias) to function result.
        
        :param alias: Alias name
        :return: Function with label
        """
        self._label = alias
        return self
    
    def to_cypher(self) -> str:
        """
        Generate Cypher string representation.
        
        :return: Cypher string
        """
        args_str = ", ".join(
            arg.to_cypher() if hasattr(arg, 'to_cypher') else str(arg)
            for arg in self.args
        )
        result = f"{self.name}({args_str})"
        
        if hasattr(self, '_label'):
            result += f" AS {self._label}"
        
        return result


# Convenience functions for common Cypher functions
def count(*args: Any) -> Function:
    """COUNT function."""
    return Function("count", *args)


def sum(expression: Any) -> Function:
    """SUM function."""
    return Function("sum", expression)


def avg(expression: Any) -> Function:
    """AVG function."""
    return Function("avg", expression)


def min(expression: Any) -> Function:
    """MIN function."""
    return Function("min", expression)


def max(expression: Any) -> Function:
    """MAX function."""
    return Function("max", expression)


def indegree(node: Any) -> Function:
    """indegree function for nodes."""
    return Function("indegree", node)


def outdegree(node: Any) -> Function:
    """outdegree function for nodes."""
    return Function("outdegree", node)
