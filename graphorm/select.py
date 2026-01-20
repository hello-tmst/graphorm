"""
Select statement builder for Cypher queries.

This module provides Select class and select() function for building Cypher queries
in an object-oriented way, similar to SQLAlchemy 2.0.
"""
from typing import Any, Dict, List, TYPE_CHECKING
from .registry import Registry

if TYPE_CHECKING:
    from .node import Node
    from .edge import Edge


class Select:
    """
    Select statement builder for Cypher queries.
    
    Provides fluent interface for building MATCH, WHERE, RETURN, ORDER BY, LIMIT, SKIP clauses.
    """
    
    def __init__(self, *entities: Any):
        """
        Initialize Select statement.
        
        :param entities: Node classes or aliases to select
        """
        self._entities = list(entities)
        self._match_clauses: List[Any] = []
        self._where_clauses: List[Any] = []
        self._return_clauses: List[Any] = []  # Will be auto-populated from entities if not set
        self._order_by_clauses: List[Any] = []
        self._limit_value: int = None
        self._skip_value: int = None
        self._params: Dict[str, Any] = {}
        self._param_counter: int = 0
        self._distinct: bool = False
        self._returns_explicitly_set: bool = False
    
    def match(self, *entities: Any) -> "Select":
        """
        Add MATCH clause (corresponds to MATCH in Cypher).
        
        :param entities: Node classes, aliases, or edge patterns to match
        :return: Self for chaining
        """
        self._match_clauses.extend(entities)
        return self
    
    def optional_match(self, *entities: Any) -> "Select":
        """
        Add OPTIONAL MATCH clause.
        
        :param entities: Node classes, aliases, or edge patterns to optionally match
        :return: Self for chaining
        """
        # Store as tuple with flag to indicate OPTIONAL
        for entity in entities:
            self._match_clauses.append(("OPTIONAL", entity))
        return self
    
    def where(self, *conditions: Any) -> "Select":
        """
        Add WHERE clause (corresponds to WHERE in Cypher).
        
        :param conditions: BinaryExpression objects or other conditions
        :return: Self for chaining
        """
        self._where_clauses.extend(conditions)
        return self
    
    def returns(self, *expressions: Any) -> "Select":
        """
        Add RETURN clause (corresponds to RETURN in Cypher).
        Uses 'returns' instead of 'return' because 'return' is Python keyword.
        
        :param expressions: Expressions to return (Node classes, aliases, properties, functions)
        :return: Self for chaining
        """
        self._returns_explicitly_set = True
        if expressions:
            self._return_clauses = list(expressions)
        elif not self._return_clauses:
            # If no expressions and no return clauses set, use entities
            self._return_clauses = self._entities
        return self
    
    def returns_distinct(self, *expressions: Any) -> "Select":
        """
        Add RETURN DISTINCT clause.
        
        :param expressions: Expressions to return
        :return: Self for chaining
        """
        self._distinct = True
        self._return_clauses = list(expressions) if expressions else self._entities
        return self
    
    def orderby(self, *expressions: Any) -> "Select":
        """
        Add ORDER BY clause (corresponds to ORDER BY in Cypher).
        Uses 'orderby' without underscore for consistency with Cypher syntax.
        
        :param expressions: Expressions to order by (properties with .asc() or .desc())
        :return: Self for chaining
        """
        self._order_by_clauses.extend(expressions)
        return self
    
    def limit(self, count: int) -> "Select":
        """
        Add LIMIT clause (corresponds to LIMIT in Cypher).
        
        :param count: Maximum number of results
        :return: Self for chaining
        """
        self._limit_value = count
        return self
    
    def skip(self, count: int) -> "Select":
        """
        Add SKIP clause (corresponds to SKIP in Cypher).
        
        :param count: Number of results to skip
        :return: Self for chaining
        """
        self._skip_value = count
        return self
    
    def to_cypher(self) -> str:
        """
        Generate Cypher query string from this Select statement.
        
        :return: Cypher query string
        """
        parts: List[str] = []
        
        # Generate MATCH clauses from entities and explicit match() calls
        match_patterns: List[str] = []
        
        # First, add MATCH from entities in select()
        for entity in self._entities:
            pattern = self._entity_to_match_pattern(entity)
            if pattern:
                # Check if this pattern already exists (avoid duplicates)
                if pattern not in match_patterns:
                    match_patterns.append(pattern)
        
        # Then, add explicit MATCH clauses
        optional_patterns: List[str] = []
        for match_item in self._match_clauses:
            if isinstance(match_item, tuple) and match_item[0] == "OPTIONAL":
                # OPTIONAL MATCH
                pattern = self._entity_to_match_pattern(match_item[1])
                if pattern:
                    optional_patterns.append(pattern)
            else:
                pattern = self._entity_to_match_pattern(match_item)
                if pattern:
                    match_patterns.append(pattern)
        
        if match_patterns:
            # Combine all MATCH clauses
            parts.append("MATCH " + ", ".join(match_patterns))
        
        if optional_patterns:
            # Add OPTIONAL MATCH clauses
            for pattern in optional_patterns:
                parts.append(f"OPTIONAL MATCH {pattern}")
        
        # WHERE clause
        if self._where_clauses:
            where_parts: List[str] = []
            # Build alias map for properties
            alias_map: Dict[Any, str] = {}
            for entity in self._entities:
                if isinstance(entity, type):
                    # Check if it's an aliased class
                    if hasattr(entity, '_alias'):
                        # Aliased class - map both the aliased class and base class
                        alias = entity._alias
                        alias_map[entity] = alias
                        if hasattr(entity, '__bases__') and len(entity.__bases__) > 0:
                            base_class = entity.__bases__[0]
                            alias_map[base_class] = alias
                    else:
                        # Regular Node class
                        alias_map[entity] = self._get_alias_for_entity(entity)
                elif hasattr(entity, '_alias'):
                    # Get base class from aliased class instance
                    if hasattr(entity, '__bases__') and len(entity.__bases__) > 0:
                        base_class = entity.__bases__[0]
                        alias_map[base_class] = entity._alias
                    elif isinstance(entity, type):
                        alias_map[entity] = entity._alias
            
            for condition in self._where_clauses:
                if hasattr(condition, 'to_cypher'):
                    where_parts.append(condition.to_cypher(self._params, alias_map))
                else:
                    where_parts.append(str(condition))
            parts.append("WHERE " + " AND ".join(where_parts))
        
        # RETURN clause (required in Cypher)
        if not self._returns_explicitly_set and not self._return_clauses and self._entities:
            # Auto-generate RETURN from entities if not specified
            self._return_clauses = self._entities
        
        if self._return_clauses:
            return_parts: List[str] = []
            # Build alias map for functions
            alias_map: Dict[Any, str] = {}
            for entity in self._entities:
                if isinstance(entity, type):
                    if hasattr(entity, '_alias'):
                        alias_map[entity] = entity._alias
                        if hasattr(entity, '__bases__') and len(entity.__bases__) > 0:
                            base_class = entity.__bases__[0]
                            alias_map[base_class] = entity._alias
                    else:
                        alias_map[entity] = self._get_alias_for_entity(entity)
            
            for expr in self._return_clauses:
                if hasattr(expr, 'to_cypher'):
                    # Pass alias_map to functions
                    if hasattr(expr, 'name'):  # It's a Function
                        return_parts.append(expr.to_cypher(alias_map=alias_map))
                    else:
                        return_parts.append(expr.to_cypher())
                elif isinstance(expr, type):
                    # Node class or aliased class - get alias
                    if hasattr(expr, '_alias'):
                        # Aliased class
                        return_parts.append(expr._alias)
                    else:
                        # Regular Node class - get default alias
                        alias = self._get_alias_for_entity(expr)
                        return_parts.append(alias)
                elif hasattr(expr, '_alias'):
                    # Aliased node class instance
                    return_parts.append(expr._alias)
                else:
                    return_parts.append(str(expr))
            
            distinct_str = "DISTINCT " if self._distinct else ""
            parts.append(f"RETURN {distinct_str}" + ", ".join(return_parts))
        
        # ORDER BY clause (must come after RETURN)
        if self._order_by_clauses:
            order_parts: List[str] = []
            # Build alias map for order by expressions
            alias_map: Dict[Any, str] = {}
            for entity in self._entities:
                if isinstance(entity, type):
                    if hasattr(entity, '_alias'):
                        alias_map[entity] = entity._alias
                        if hasattr(entity, '__bases__') and len(entity.__bases__) > 0:
                            base_class = entity.__bases__[0]
                            alias_map[base_class] = entity._alias
                    else:
                        alias_map[entity] = self._get_alias_for_entity(entity)
            
            for expr in self._order_by_clauses:
                if hasattr(expr, 'to_cypher'):
                    # Pass alias_map to OrderByExpression
                    order_parts.append(expr.to_cypher(alias_map=alias_map))
                else:
                    order_parts.append(str(expr))
            parts.append("ORDER BY " + ", ".join(order_parts))
        
        # SKIP (must come before LIMIT in Cypher)
        if self._skip_value is not None:
            parts.append(f"SKIP {self._skip_value}")
        
        # LIMIT
        if self._limit_value is not None:
            parts.append(f"LIMIT {self._limit_value}")
        
        return " ".join(parts)
    
    def get_params(self) -> Dict[str, Any]:
        """
        Get parameter dictionary for parameterized query.
        
        :return: Dictionary of parameter names to values
        """
        return self._params
    
    def _entity_to_match_pattern(self, entity: Any) -> str:
        """
        Convert entity (Node class, alias, etc.) to MATCH pattern.
        
        :param entity: Node class, alias, or other entity
        :return: MATCH pattern string or None
        """
        if isinstance(entity, type):
            # Check if it's an aliased class first (has _alias attribute)
            if hasattr(entity, '_alias'):
                alias = entity._alias
                # Get the actual node class from the aliased class
                if hasattr(entity, '__bases__') and len(entity.__bases__) > 0:
                    base_class = entity.__bases__[0]
                    label = self._get_label_from_class(base_class)
                else:
                    label = entity.__name__
                return f"({alias}:{label})"
            # Regular Node class
            elif hasattr(entity, '__labels__'):
                alias = self._get_alias_for_entity(entity)
                label = self._get_label_from_class(entity)
                return f"({alias}:{label})"
        elif isinstance(entity, str):
            # String pattern (for complex patterns)
            return entity
        
        return None
    
    def _get_alias_for_entity(self, entity: Any) -> str:
        """
        Get default alias for entity.
        
        :param entity: Node class or alias
        :return: Alias string
        """
        if isinstance(entity, type):
            # Check if it's an aliased class first
            if hasattr(entity, '_alias'):
                return entity._alias
            # Use lowercase class name
            return entity.__name__.lower()
        elif hasattr(entity, '_alias'):
            # Aliased class instance
            return entity._alias
        else:
            return "n"
    
    def _get_label_from_class(self, node_class: type) -> str:
        """
        Get label from Node class.
        
        :param node_class: Node class
        :return: Label string
        """
        if hasattr(node_class, "__label__"):
            return node_class.__label__
        else:
            return node_class.__name__


def select(*entities: Any) -> Select:
    """
    Create a SELECT statement.
    
    :param entities: Node classes or aliases to select
    :return: Select instance
    """
    return Select(*entities)


def aliased(node_class: type["Node"], name: str = None) -> type:
    """
    Create an alias for a Node class.
    
    :param node_class: Node class to create alias for
    :param name: Alias name (defaults to lowercase class name)
    :return: Aliased Node class
    """
    if name is None:
        name = node_class.__name__.lower()
    
    # Create subclass with alias as class attribute
    class AliasedNode(node_class):
        _alias = name
    
    # Set alias as class attribute (in case it wasn't set in class definition)
    if not hasattr(AliasedNode, '_alias'):
        AliasedNode._alias = name
    
    # Copy class attributes
    AliasedNode.__name__ = f"Aliased{node_class.__name__}"
    AliasedNode.__qualname__ = f"Aliased{node_class.__qualname__}"
    
    # Ensure __labels__ is copied from parent
    if hasattr(node_class, '__labels__'):
        AliasedNode.__labels__ = node_class.__labels__
    
    return AliasedNode
