"""
Delete statement builder for Cypher queries.

This module provides Delete class and delete() function for building DELETE/DETACH DELETE queries
in an object-oriented way, similar to SQLAlchemy 2.0.
"""
from typing import Any, Dict, TYPE_CHECKING, Union, Optional
from .select import Statement

if TYPE_CHECKING:
    from .node import Node
    from .edge import Edge
    from .query_result import QueryResult


class Delete(Statement):
    """
    Delete statement builder for Cypher queries.
    
    Provides fluent interface for building MATCH, WHERE, DELETE/DETACH DELETE clauses.
    """
    
    def __init__(self, *entities: Any):
        """
        Initialize Delete statement.
        
        :param entities: Node classes, aliases, or edge classes to delete
        """
        super().__init__()
        self._entities = list(entities)
        self._detach: bool = False
        self._return_clauses: list[Any] = []
    
    def detach(self) -> "Delete":
        """Use DETACH DELETE instead of DELETE."""
        self._detach = True
        return self
    
    def returns(self, *expressions: Any) -> "Delete":
        """Add RETURN clause (optional for DELETE)."""
        self._return_clauses = list(expressions)
        return self
    
    def to_cypher(self) -> str:
        """Generate DELETE Cypher query."""
        parts: list[str] = []
        
        # Generate MATCH clauses
        match_patterns: list[str] = []
        optional_patterns: list[str] = []
        
        for match_item in self._match_clauses:
            if isinstance(match_item, tuple) and match_item[0] == "OPTIONAL":
                pattern = self._entity_to_match_pattern(match_item[1])
                if pattern:
                    optional_patterns.append(pattern)
            elif isinstance(match_item, tuple) and match_item[0] == "RAW":
                match_patterns.append(match_item[1])
            else:
                pattern = self._entity_to_match_pattern(match_item)
                if pattern:
                    match_patterns.append(pattern)
        
        # Fallback: if no match() was called, try to generate from entities
        if not match_patterns and not optional_patterns and self._entities:
            for entity in self._entities:
                pattern = self._entity_to_match_pattern(entity)
                if pattern and pattern not in match_patterns:
                    match_patterns.append(pattern)
        
        if match_patterns:
            parts.append("MATCH " + ", ".join(match_patterns))
        
        if optional_patterns:
            for pattern in optional_patterns:
                parts.append(f"OPTIONAL MATCH {pattern}")
        
        # WITH clause (if present)
        with_clause = self._build_with_clause()
        if with_clause:
            parts.append(with_clause)
            alias_map = self._alias_map.copy()
        else:
            # Build alias map for WHERE
            alias_map: Dict[Any, str] = {}
            for match_item in self._match_clauses:
                if isinstance(match_item, tuple) and match_item[0] == "OPTIONAL":
                    match_item = match_item[1]
                elif isinstance(match_item, tuple) and match_item[0] == "RAW":
                    continue
                
                if isinstance(match_item, tuple) and len(match_item) == 3:
                    src, edge, dst = match_item
                    self._add_to_alias_map(src, alias_map)
                    self._add_to_alias_map(dst, alias_map)
                else:
                    self._add_to_alias_map(match_item, alias_map)
            
            if not alias_map:
                for entity in self._entities:
                    self._add_to_alias_map(entity, alias_map)
            
            self._alias_map.update(alias_map)
        
        # WHERE clause
        where_clause = self._build_where_clause(alias_map)
        if where_clause:
            parts.append(where_clause)
        
        # DELETE clause
        delete_targets: list[str] = []
        
        # Extract aliases from match patterns first (they take precedence)
        for match_item in self._match_clauses:
            if isinstance(match_item, tuple) and match_item[0] in ("OPTIONAL", "RAW"):
                continue
            
            if isinstance(match_item, tuple) and len(match_item) == 3:
                # Relationship pattern: extract src, edge, dst
                src, edge, dst = match_item
                if isinstance(src, type):
                    alias = self._get_alias_for_entity(src)
                    if alias not in delete_targets:
                        delete_targets.append(alias)
                elif hasattr(src, '_alias'):
                    if src._alias not in delete_targets:
                        delete_targets.append(src._alias)
                if isinstance(edge, type):
                    alias = self._get_alias_for_entity(edge)
                    if alias not in delete_targets:
                        delete_targets.append(alias)
                elif hasattr(edge, '_alias'):
                    if edge._alias not in delete_targets:
                        delete_targets.append(edge._alias)
                if isinstance(dst, type):
                    alias = self._get_alias_for_entity(dst)
                    if alias not in delete_targets:
                        delete_targets.append(alias)
                elif hasattr(dst, '_alias'):
                    if dst._alias not in delete_targets:
                        delete_targets.append(dst._alias)
            elif isinstance(match_item, type):
                alias = self._get_alias_for_entity(match_item)
                if alias not in delete_targets:
                    delete_targets.append(alias)
            elif hasattr(match_item, '_alias'):
                if match_item._alias not in delete_targets:
                    delete_targets.append(match_item._alias)
        
        # If no match patterns, use entities
        if not delete_targets and self._entities:
            for entity in self._entities:
                if isinstance(entity, type):
                    alias = self._get_alias_for_entity(entity)
                    if alias not in delete_targets:
                        delete_targets.append(alias)
                elif hasattr(entity, '_alias'):
                    if entity._alias not in delete_targets:
                        delete_targets.append(entity._alias)
                else:
                    alias_str = str(entity)
                    if alias_str not in delete_targets:
                        delete_targets.append(alias_str)
        
        if not delete_targets:
            # Last resort: use wildcard or default
            delete_targets = ["n"]
        
        delete_keyword = "DETACH DELETE" if self._detach else "DELETE"
        parts.append(f"{delete_keyword} {', '.join(delete_targets)}")
        
        # RETURN clause (optional)
        if self._return_clauses:
            return_parts: list[str] = []
            for expr in self._return_clauses:
                if isinstance(expr, str):
                    return_parts.append(expr)
                elif hasattr(expr, 'to_cypher'):
                    if hasattr(expr, 'name'):  # Function
                        return_parts.append(expr.to_cypher(alias_map=alias_map))
                    else:
                        # CaseExpression and other expressions need params
                        return_parts.append(expr.to_cypher(params=self._params, alias_map=alias_map))
                elif isinstance(expr, type):
                    if hasattr(expr, '_alias'):
                        return_parts.append(expr._alias)
                    else:
                        return_parts.append(self._get_alias_for_entity(expr))
                elif hasattr(expr, '_alias'):
                    return_parts.append(expr._alias)
                else:
                    return_parts.append(str(expr))
            
            parts.append("RETURN " + ", ".join(return_parts))
        
        return " ".join(parts)


def delete(*entities: Any) -> Delete:
    """
    Create a DELETE statement.
    
    :param entities: Node classes, aliases, or edge classes to delete
    :return: Delete instance
    """
    return Delete(*entities)
