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
    
    def to_cypher(self, alias_map: Dict[Any, str] = None) -> str:
        """
        Generate Cypher string representation.
        
        :param alias_map: Optional mapping of node classes to aliases
        :return: Cypher string
        """
        if alias_map is None:
            alias_map = {}
        
        if hasattr(self.expression, 'to_cypher'):
            # Pass alias_map if expression supports it
            if hasattr(self.expression, 'name') or hasattr(self.expression, 'left'):
                # Function or ArithmeticExpression
                expr_str = self.expression.to_cypher(alias_map=alias_map)
            else:
                expr_str = self.expression.to_cypher()
        else:
            expr_str = str(self.expression)
        
        return f"{expr_str} {self.direction}"


class ArithmeticExpression:
    """
    Represents an arithmetic expression (left operator right).
    
    Example:
        indegree(Page) + outdegree(Page)  # ArithmeticExpression
    """
    
    def __init__(self, left: Any, operator: str, right: Any):
        """
        Initialize arithmetic expression.
        
        :param left: Left operand (Function, Property, or value)
        :param operator: Arithmetic operator ("+", "-", "*", "/")
        :param right: Right operand (Function, Property, or value)
        """
        self.left = left
        self.operator = operator
        self.right = right
    
    def __add__(self, other: Any) -> "ArithmeticExpression":
        """Addition operator: +"""
        return ArithmeticExpression(self, "+", other)
    
    def __sub__(self, other: Any) -> "ArithmeticExpression":
        """Subtraction operator: -"""
        return ArithmeticExpression(self, "-", other)
    
    def __mul__(self, other: Any) -> "ArithmeticExpression":
        """Multiplication operator: *"""
        return ArithmeticExpression(self, "*", other)
    
    def __truediv__(self, other: Any) -> "ArithmeticExpression":
        """Division operator: /"""
        return ArithmeticExpression(self, "/", other)
    
    def label(self, alias: str) -> "ArithmeticExpression":
        """
        Add label (alias) to arithmetic expression result.
        
        :param alias: Alias name
        :return: ArithmeticExpression with label
        """
        self._label = alias
        return self
    
    def desc(self) -> "OrderByExpression":
        """DESC ordering for arithmetic expression."""
        return OrderByExpression(self, "DESC")
    
    def asc(self) -> "OrderByExpression":
        """ASC ordering for arithmetic expression."""
        return OrderByExpression(self, "ASC")
    
    def __eq__(self, other: Any) -> "BinaryExpression":
        """Equality operator: =="""
        return BinaryExpression(self, "=", other)
    
    def __ne__(self, other: Any) -> "BinaryExpression":
        """Inequality operator: <>"""
        return BinaryExpression(self, "<>", other)
    
    def __lt__(self, other: Any) -> "BinaryExpression":
        """Less than operator: <"""
        return BinaryExpression(self, "<", other)
    
    def __le__(self, other: Any) -> "BinaryExpression":
        """Less than or equal operator: <="""
        return BinaryExpression(self, "<=", other)
    
    def __gt__(self, other: Any) -> "BinaryExpression":
        """Greater than operator: >"""
        return BinaryExpression(self, ">", other)
    
    def __ge__(self, other: Any) -> "BinaryExpression":
        """Greater than or equal operator: >="""
        return BinaryExpression(self, ">=", other)
    
    def to_cypher(self, alias_map: Dict[Any, str] = None) -> str:
        """
        Generate Cypher string representation.
        
        :param alias_map: Optional mapping of node classes to aliases
        :return: Cypher string
        """
        if alias_map is None:
            alias_map = {}
        
        def format_operand(operand: Any) -> str:
            """Format operand for Cypher."""
            if hasattr(operand, 'to_cypher'):
                # Check if it's a Function that needs alias_map
                if hasattr(operand, 'name'):  # It's a Function
                    return operand.to_cypher(alias_map=alias_map)
                # Other expression types
                return operand.to_cypher()
            else:
                return str(operand)
        
        left_str = format_operand(self.left)
        right_str = format_operand(self.right)
        
        result = f"{left_str} {self.operator} {right_str}"
        
        if hasattr(self, '_label'):
            result += f" AS {self._label}"
        
        return result


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
    
    def __add__(self, other: Any) -> ArithmeticExpression:
        """Addition operator: +"""
        return ArithmeticExpression(self, "+", other)
    
    def __sub__(self, other: Any) -> ArithmeticExpression:
        """Subtraction operator: -"""
        return ArithmeticExpression(self, "-", other)
    
    def __mul__(self, other: Any) -> ArithmeticExpression:
        """Multiplication operator: *"""
        return ArithmeticExpression(self, "*", other)
    
    def __truediv__(self, other: Any) -> ArithmeticExpression:
        """Division operator: /"""
        return ArithmeticExpression(self, "/", other)
    
    def __radd__(self, other: Any) -> ArithmeticExpression:
        """Right addition operator: +"""
        return ArithmeticExpression(other, "+", self)
    
    def __rsub__(self, other: Any) -> ArithmeticExpression:
        """Right subtraction operator: -"""
        return ArithmeticExpression(other, "-", self)
    
    def __rmul__(self, other: Any) -> ArithmeticExpression:
        """Right multiplication operator: *"""
        return ArithmeticExpression(other, "*", self)
    
    def __rtruediv__(self, other: Any) -> ArithmeticExpression:
        """Right division operator: /"""
        return ArithmeticExpression(other, "/", self)
    
    def __eq__(self, other: Any) -> "BinaryExpression":
        """Equality operator: =="""
        return BinaryExpression(self, "=", other)
    
    def __ne__(self, other: Any) -> "BinaryExpression":
        """Inequality operator: <>"""
        return BinaryExpression(self, "<>", other)
    
    def __lt__(self, other: Any) -> "BinaryExpression":
        """Less than operator: <"""
        return BinaryExpression(self, "<", other)
    
    def __le__(self, other: Any) -> "BinaryExpression":
        """Less than or equal operator: <="""
        return BinaryExpression(self, "<=", other)
    
    def __gt__(self, other: Any) -> "BinaryExpression":
        """Greater than operator: >"""
        return BinaryExpression(self, ">", other)
    
    def __ge__(self, other: Any) -> "BinaryExpression":
        """Greater than or equal operator: >="""
        return BinaryExpression(self, ">=", other)
    
    def to_cypher(self, alias_map: Dict[Any, str] = None) -> str:
        """
        Generate Cypher string representation.
        
        :param alias_map: Optional mapping of node classes to aliases
        :return: Cypher string
        """
        if alias_map is None:
            alias_map = {}
        
        def format_arg(arg: Any) -> str:
            """Format function argument for Cypher."""
            if hasattr(arg, 'to_cypher'):
                # Property or other expression with to_cypher
                if hasattr(arg, 'to_cypher') and alias_map:
                    return arg.to_cypher(alias_map=alias_map)
                return arg.to_cypher()
            elif isinstance(arg, type):
                # Node class - get alias from map or use default
                if arg in alias_map:
                    return alias_map[arg]
                # Use lowercase class name as default alias
                return arg.__name__.lower()
            else:
                return str(arg)
        
        args_str = ", ".join(format_arg(arg) for arg in self.args)
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
