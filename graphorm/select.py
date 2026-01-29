"""
Select statement builder for Cypher queries.

This module provides Select class and select() function for building Cypher queries
in an object-oriented way, similar to SQLAlchemy 2.0.
"""

from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    TypeVar,
    Union,
)

from .variable_length import VariableLength

if TYPE_CHECKING:
    from .edge import Edge
    from .expression import (
        BinaryExpression,
        OrderByExpression,
    )
    from .node import Node

T = TypeVar("T", bound="Node")
E = TypeVar("E", bound="Edge")


class Statement:
    """Base class for all Cypher statements (Select, Delete, etc.)."""

    def __init__(self):
        self._match_clauses: list[Any] = []
        self._where_clauses: list[Any] = []
        self._where_after_with: list[Any] = []  # WHERE conditions after WITH
        self._match_clauses_after_with: list[Any] = []  # MATCH patterns after WITH
        self._where_after_match_after_with: list[Any] = []  # WHERE after that MATCH
        self._with_clauses: list[Any] = []
        self._params: dict[str, Any] = {}
        self._param_counter: int = 0
        self._alias_map: dict[Any, str] = {}
        self._with_called: bool = False  # Track if with_() has been called

    def match(self, *patterns: Any) -> "Statement":
        """Add MATCH clause."""
        target = (
            self._match_clauses_after_with
            if self._with_called
            else self._match_clauses
        )
        for pattern in patterns:
            if isinstance(pattern, str):
                # Raw Cypher pattern (supports variable-length: *1..3, *)
                target.append(("RAW", pattern))
            else:
                target.append(pattern)
        return self

    def optional_match(self, *entities: Any) -> "Statement":
        """Add OPTIONAL MATCH clause."""
        for entity in entities:
            self._match_clauses.append(("OPTIONAL", entity))
        return self

    def where(self, *conditions: Any) -> "Statement":
        """Add WHERE clause."""
        # If with_() has been called and match-after-with exists, add to WHERE after that MATCH
        if self._with_called and len(self._match_clauses_after_with) > 0:
            self._where_after_match_after_with.extend(conditions)
        elif self._with_called:
            self._where_after_with.extend(conditions)
        else:
            self._where_clauses.extend(conditions)
        return self

    def with_(self, *expressions: Any) -> "Statement":
        """Add WITH clause."""
        self._with_clauses.extend(expressions)
        self._with_called = True  # Mark that WITH has been called
        return self

    def get_params(self) -> dict[str, Any]:
        """Get parameter dictionary for parameterized query."""
        return self._params

    def _add_param(self, value: Any) -> str:
        """Add parameter and return parameter name."""
        param_name = f"param_{self._param_counter}"
        self._param_counter += 1
        self._params[param_name] = value
        return param_name

    def _build_where_clause(
        self, alias_map: dict[Any, str] = None, where_clauses: list[Any] = None
    ) -> str:
        """Build WHERE clause string."""
        if where_clauses is None:
            where_clauses = self._where_clauses

        if not where_clauses:
            return ""

        if alias_map is None:
            alias_map = self._alias_map.copy()
        if not alias_map:
            alias_map = self._build_alias_map_from_match_clauses(include_edges=False)

        where_parts: list[str] = []
        for condition in where_clauses:
            if hasattr(condition, "to_cypher"):
                where_parts.append(condition.to_cypher(self._params, alias_map))
            else:
                where_parts.append(str(condition))

        return "WHERE " + " AND ".join(where_parts)

    def _build_with_clause(self, alias_map: dict[Any, str] = None) -> str:
        """Build WITH clause string."""
        if not self._with_clauses:
            return ""

        if alias_map is None:
            alias_map = self._alias_map.copy()

        with_parts: list[str] = []
        alias_map_for_with: dict[Any, str] = {}

        for expr in self._with_clauses:
            if isinstance(expr, tuple) and len(expr) == 2:
                # (expression, alias) tuple
                expr_obj, alias_name = expr
                expr_str = self._format_expression(expr_obj, alias_map)
                with_parts.append(f"{expr_str} AS {alias_name}")
                # Update alias map
                if hasattr(expr_obj, "node_class"):
                    alias_map_for_with[expr_obj.node_class] = alias_name
            elif hasattr(expr, "to_cypher"):
                # Expression with label
                # Check if it's a Function (doesn't accept params)
                if hasattr(expr, "name"):  # Function
                    expr_str = expr.to_cypher(alias_map=alias_map)
                    # Function.to_cypher() already includes "AS label" if it has _label
                    # So we don't need to add it again
                    with_parts.append(expr_str)
                else:
                    expr_str = expr.to_cypher(params=self._params, alias_map=alias_map)
                    if hasattr(expr, "_label") and expr._label:
                        # Check if AS is already in expr_str (from to_cypher)
                        if " AS " not in expr_str:
                            with_parts.append(f"{expr_str} AS {expr._label}")
                        else:
                            with_parts.append(expr_str)
                        # Update alias map if it's a property
                        if hasattr(expr, "node_class"):
                            alias_map_for_with[expr.node_class] = expr._label
                    else:
                        with_parts.append(expr_str)
            elif isinstance(expr, type):
                # Node/Edge class - use its alias
                if hasattr(expr, "_alias"):
                    with_parts.append(expr._alias)
                else:
                    alias = self._get_alias_for_entity(expr)
                    with_parts.append(alias)
            else:
                with_parts.append(str(expr))

        # Update alias_map for subsequent clauses
        alias_map.update(alias_map_for_with)
        self._alias_map.update(alias_map_for_with)

        return "WITH " + ", ".join(with_parts)

    def _format_expression(self, expr: Any, alias_map: dict[Any, str] = None) -> str:
        """Format expression for use in WITH/RETURN."""
        if alias_map is None:
            alias_map = {}

        if hasattr(expr, "to_cypher"):
            if hasattr(expr, "name"):  # Function
                return expr.to_cypher(alias_map=alias_map)
            else:
                return expr.to_cypher(params=self._params, alias_map=alias_map)
        elif isinstance(expr, type):
            if hasattr(expr, "_alias"):
                return expr._alias
            else:
                return self._get_alias_for_entity(expr)
        else:
            return str(expr)

    def _get_alias_for_entity(self, entity: Any) -> str:
        """Get default alias for entity."""
        if isinstance(entity, type):
            if hasattr(entity, "_alias"):
                return entity._alias
            return entity.__name__.lower()
        elif hasattr(entity, "_alias"):
            return entity._alias
        else:
            return "n"

    def _add_to_alias_map(self, entity: Any, alias_map: dict[Any, str]) -> None:
        """Add entity to alias map if it's a Node/Edge class."""
        if isinstance(entity, type):
            if hasattr(entity, "_alias"):
                alias = entity._alias
                alias_map[entity] = alias
                if hasattr(entity, "__bases__") and len(entity.__bases__) > 0:
                    base_class = entity.__bases__[0]
                    alias_map[base_class] = alias
            elif hasattr(entity, "__labels__") or hasattr(entity, "__relation__"):
                alias_map[entity] = self._get_alias_for_entity(entity)

    def _build_alias_map_from_match_clauses(
        self,
        *,
        include_edges: bool = False,
        match_clauses: list[Any] | None = None,
    ) -> dict:
        """Build alias map by iterating match clauses. OPTIONAL/RAW normalized or skipped."""
        clauses = (
            match_clauses if match_clauses is not None else self._match_clauses
        )
        alias_map: dict[Any, str] = {}
        for match_item in clauses:
            if isinstance(match_item, tuple) and match_item[0] == "OPTIONAL":
                match_item = match_item[1]
            elif isinstance(match_item, tuple) and match_item[0] == "RAW":
                continue
            if isinstance(match_item, tuple) and len(match_item) == 3:
                src, edge, dst = match_item
                self._add_to_alias_map(src, alias_map)
                self._add_to_alias_map(dst, alias_map)
                if include_edges and not isinstance(edge, VariableLength):
                    self._add_to_alias_map(edge, alias_map)
            else:
                self._add_to_alias_map(match_item, alias_map)
        return alias_map

    def _get_label_from_class(self, node_class: type) -> str:
        """Get label from Node class."""
        if hasattr(node_class, "__label__"):
            return node_class.__label__
        else:
            return node_class.__name__

    def _get_relation_from_class(self, edge_class: type) -> str:
        """Get relation name from Edge class."""
        if hasattr(edge_class, "__relation_name__"):
            return edge_class.__relation_name__
        elif hasattr(edge_class, "__relation__"):
            return edge_class.__relation__
        else:
            return edge_class.__name__

    def _entity_to_match_pattern(self, entity: Any) -> str:
        """Convert entity to MATCH pattern."""
        # Handle tuple patterns (relationship patterns)
        if isinstance(entity, tuple) and len(entity) == 3:
            src, edge, dst = entity
            src_pattern = self._entity_to_match_pattern(src)
            edge_pattern = self._edge_to_match_pattern(edge)
            dst_pattern = self._entity_to_match_pattern(dst)

            if src_pattern and edge_pattern and dst_pattern:
                # Add direction -> for edges (outgoing relationship)
                return f"{src_pattern}-{edge_pattern}->{dst_pattern}"
            return None

        # Handle Node classes and aliases
        if isinstance(entity, type):
            if hasattr(entity, "_alias"):
                alias = entity._alias
                if hasattr(entity, "__bases__") and len(entity.__bases__) > 0:
                    base_class = entity.__bases__[0]
                    label = self._get_label_from_class(base_class)
                else:
                    label = entity.__name__
                return f"({alias}:{label})"
            elif hasattr(entity, "__labels__"):
                alias = self._get_alias_for_entity(entity)
                label = self._get_label_from_class(entity)
                return f"({alias}:{label})"
        elif isinstance(entity, str):
            return entity

        return None

    def _variable_length_edge_to_pattern(self, v: VariableLength) -> str:
        """Convert VariableLength descriptor to Cypher edge pattern [:REL*...]."""
        relation = self._get_relation_from_class(v.edge_class)
        min_h, max_h = v.min_hops, v.max_hops
        if min_h is None and max_h is None:
            suffix = "*"
        elif min_h is not None and max_h is not None and min_h == max_h:
            suffix = f"*{min_h}"
        elif min_h is not None and max_h is not None:
            suffix = f"*{min_h}..{max_h}"
        elif min_h is not None:
            suffix = f"*{min_h}.."
        else:
            suffix = "*"
        return f"[:{relation}{suffix}]"

    def _edge_to_match_pattern(self, edge: Any) -> str:
        """Convert edge to MATCH pattern."""
        if isinstance(edge, VariableLength):
            return self._variable_length_edge_to_pattern(edge)
        if isinstance(edge, type):
            if hasattr(edge, "_alias"):
                alias = edge._alias
                if hasattr(edge, "__bases__") and len(edge.__bases__) > 0:
                    base_class = edge.__bases__[0]
                    relation = self._get_relation_from_class(base_class)
                else:
                    relation = edge.__name__
                return f"[{alias}:{relation}]"
            elif hasattr(edge, "__relation__"):
                alias = self._get_alias_for_entity(edge)
                relation = edge.__relation__
                return f"[{alias}:{relation}]"

        return None


class Select(Statement, Generic[T]):
    """
    Select statement builder for Cypher queries.

    Provides fluent interface for building MATCH, WHERE, RETURN, ORDER BY, LIMIT, SKIP clauses.
    """

    def __init__(self, *entities: Any):
        """
        Initialize Select statement.

        :param entities: Node classes or aliases (deprecated - use match() instead)
        """
        super().__init__()
        self._entities = list(
            entities
        )  # Kept for backward compatibility, but match() is now required
        self._return_clauses: list[Any] = []
        self._order_by_clauses: list[Any] = []
        self._limit_value: int = None
        self._skip_value: int = None
        self._remove_clauses: list[Any] = []
        self._distinct: bool = False
        self._returns_explicitly_set: bool = False

    def match(self, *patterns: Any) -> "Select":
        """
        Add MATCH clause (corresponds to MATCH in Cypher).

        Patterns can be:
        - Node class: match(Page.alias("a"))
        - Tuple (relationship pattern): match((Page.alias("a"), Linked.alias("r"), Page.alias("b")))
        - String: match("(a:Page)-[r:Linked]->(b:Page)") or variable-length: match("(a)-[:Linked*1..3]->(b)")

        :param patterns: Node classes, aliases, relationship tuples, or string patterns
        :return: Self for chaining
        """
        super().match(*patterns)
        return self

    def optional_match(self, *entities: Any) -> "Select":
        """
        Add OPTIONAL MATCH clause.

        :param entities: Node classes, aliases, or edge patterns to optionally match
        :return: Self for chaining
        """
        super().optional_match(*entities)
        return self

    def where(self, *conditions: Union["BinaryExpression", Any]) -> "Select":
        """
        Add WHERE clause (corresponds to WHERE in Cypher).

        :param conditions: BinaryExpression objects or other conditions
        :return: Self for chaining
        """
        super().where(*conditions)
        return self

    def with_(self, *expressions: Any) -> "Select":
        """
        Add WITH clause (corresponds to WITH in Cypher).

        :param expressions: Expressions to pass through (Node classes, aliases, properties, functions)
        :return: Self for chaining
        """
        super().with_(*expressions)
        return self

    def remove(self, *expressions: Any) -> "Select":
        """
        Add REMOVE clause (corresponds to REMOVE in Cypher).

        :param expressions: Property expressions to remove (e.g., Page.error.remove())
        :return: Self for chaining
        """
        self._remove_clauses.extend(expressions)
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

    def orderby(self, *expressions: Union["OrderByExpression", Any]) -> "Select":
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
        parts: list[str] = []

        # Generate MATCH clauses - match() is now required
        match_patterns: list[str] = []
        optional_patterns: list[str] = []

        # Process match clauses
        for match_item in self._match_clauses:
            if isinstance(match_item, tuple) and match_item[0] == "OPTIONAL":
                # OPTIONAL MATCH
                pattern = self._entity_to_match_pattern(match_item[1])
                if pattern:
                    optional_patterns.append(pattern)
            elif isinstance(match_item, tuple) and match_item[0] == "RAW":
                # Raw string pattern (supports variable-length paths)
                match_patterns.append(match_item[1])
            else:
                pattern = self._entity_to_match_pattern(match_item)
                if pattern:
                    match_patterns.append(pattern)

        # Fallback: if no match() was called, try to generate from entities (backward compatibility)
        if not match_patterns and not optional_patterns and self._entities:
            for entity in self._entities:
                pattern = self._entity_to_match_pattern(entity)
                if pattern and pattern not in match_patterns:
                    match_patterns.append(pattern)

        if match_patterns:
            # Combine all MATCH clauses
            parts.append("MATCH " + ", ".join(match_patterns))

        if optional_patterns:
            # Add OPTIONAL MATCH clauses
            for pattern in optional_patterns:
                parts.append(f"OPTIONAL MATCH {pattern}")

        # Build initial alias map from match patterns
        alias_map = self._build_alias_map_from_match_clauses(include_edges=False)
        if not alias_map:
            for entity in self._entities:
                self._add_to_alias_map(entity, alias_map)
        self._alias_map.update(alias_map)

        # WHERE clause before WITH (if any)
        where_before_with = self._build_where_clause(alias_map, self._where_clauses)
        if where_before_with:
            parts.append(where_before_with)

        # WITH clause (must come after MATCH and WHERE before WITH)
        with_clause = self._build_with_clause(alias_map)
        if with_clause:
            parts.append(with_clause)
            # Use updated alias_map from _build_with_clause
            alias_map = self._alias_map.copy()

        # WHERE clause after WITH (if any)
        where_after_with = self._build_where_clause(alias_map, self._where_after_with)
        if where_after_with:
            parts.append(where_after_with)

        # MATCH and WHERE after WITH (patterns added via match() after with_())
        if self._match_clauses_after_with:
            match_patterns_after: list[str] = []
            optional_patterns_after: list[str] = []
            for match_item in self._match_clauses_after_with:
                if isinstance(match_item, tuple) and match_item[0] == "OPTIONAL":
                    pattern = self._entity_to_match_pattern(match_item[1])
                    if pattern:
                        optional_patterns_after.append(pattern)
                elif isinstance(match_item, tuple) and match_item[0] == "RAW":
                    match_patterns_after.append(match_item[1])
                else:
                    pattern = self._entity_to_match_pattern(match_item)
                    if pattern:
                        match_patterns_after.append(pattern)
            if match_patterns_after:
                parts.append("MATCH " + ", ".join(match_patterns_after))
            for p in optional_patterns_after:
                parts.append("OPTIONAL MATCH " + p)
            alias_map_from_after = self._build_alias_map_from_match_clauses(
                include_edges=False,
                match_clauses=self._match_clauses_after_with,
            )
            alias_map.update(alias_map_from_after)
            self._alias_map.update(alias_map_from_after)
            if self._where_after_match_after_with:
                where_after_match = self._build_where_clause(
                    alias_map, self._where_after_match_after_with
                )
                if where_after_match:
                    parts.append(where_after_match)

        # REMOVE clause (must come after WHERE, before RETURN)
        if self._remove_clauses:
            remove_parts: list[str] = []
            for expr in self._remove_clauses:
                if hasattr(expr, "to_cypher"):
                    remove_parts.append(expr.to_cypher(alias_map=alias_map))
                else:
                    remove_parts.append(str(expr))
            parts.append("REMOVE " + ", ".join(remove_parts))

        # RETURN clause (required in Cypher)
        if not self._returns_explicitly_set and not self._return_clauses:
            # Auto-generate RETURN from match patterns or entities
            if self._match_clauses:
                # Extract entities from match patterns
                auto_return = []
                for match_item in self._match_clauses:
                    if isinstance(match_item, tuple) and match_item[0] == "OPTIONAL":
                        match_item = match_item[1]

                    if isinstance(match_item, tuple) and len(match_item) == 3:
                        # Relationship pattern: extract src and dst (skip edge)
                        src, edge, dst = match_item
                        auto_return.append(src)
                        auto_return.append(dst)
                    elif isinstance(match_item, type) or isinstance(match_item, str):
                        # Single node or string pattern
                        if isinstance(match_item, type):
                            auto_return.append(match_item)
                self._return_clauses = auto_return if auto_return else self._entities
            else:
                # Fallback to entities
                self._return_clauses = self._entities

        # Always include RETURN clause (required in Cypher)
        if not self._return_clauses:
            # If still empty, return all matched entities
            all_entities = []
            for match_item in self._match_clauses:
                if isinstance(match_item, tuple) and match_item[0] == "OPTIONAL":
                    match_item = match_item[1]

                if isinstance(match_item, tuple) and len(match_item) == 3:
                    src, edge, dst = match_item
                    if isinstance(src, type):
                        all_entities.append(src)
                    if isinstance(dst, type):
                        all_entities.append(dst)
                elif isinstance(match_item, type):
                    all_entities.append(match_item)

            if all_entities:
                self._return_clauses = all_entities
            else:
                # Last resort: return a wildcard
                self._return_clauses = ["*"]

        if self._return_clauses:
            return_parts: list[str] = []
            # Use alias_map from flow when WITH or match-after-with was used
            if not (self._with_called or self._match_clauses_after_with):
                alias_map = self._build_alias_map_from_match_clauses(
                    include_edges=True
                )
                if not alias_map:
                    for entity in self._entities:
                        self._add_to_alias_map(entity, alias_map)
            for expr in self._return_clauses:
                if isinstance(expr, str):
                    # String expression (e.g., "*" or raw Cypher)
                    return_parts.append(expr)
                elif hasattr(expr, "to_cypher"):
                    # Pass alias_map to functions
                    if hasattr(expr, "name"):  # It's a Function
                        # If Function has label and was in WITH, use only the label in RETURN
                        if (
                            hasattr(expr, "_label")
                            and expr._label
                            and self._with_called
                        ):
                            # After WITH, use only the alias
                            return_parts.append(expr._label)
                        else:
                            return_parts.append(expr.to_cypher(alias_map=alias_map))
                    elif hasattr(expr, "left"):  # It's an ArithmeticExpression
                        # If ArithmeticExpression has label and was in WITH, use only the label in RETURN
                        if (
                            hasattr(expr, "_label")
                            and expr._label
                            and self._with_called
                        ):
                            # After WITH, use only the alias
                            return_parts.append(expr._label)
                        else:
                            # ArithmeticExpression doesn't take params
                            return_parts.append(expr.to_cypher(alias_map=alias_map))
                    else:
                        # CaseExpression and other expressions need params
                        return_parts.append(
                            expr.to_cypher(params=self._params, alias_map=alias_map)
                        )
                elif isinstance(expr, type):
                    # Node class or aliased class - get alias
                    if hasattr(expr, "_alias"):
                        # Aliased class
                        return_parts.append(expr._alias)
                    else:
                        # Regular Node class - get default alias
                        alias = self._get_alias_for_entity(expr)
                        return_parts.append(alias)
                elif hasattr(expr, "_alias"):
                    # Aliased node class instance
                    return_parts.append(expr._alias)
                else:
                    return_parts.append(str(expr))

            distinct_str = "DISTINCT " if self._distinct else ""
            parts.append(f"RETURN {distinct_str}" + ", ".join(return_parts))

        # ORDER BY clause (must come after RETURN)
        if self._order_by_clauses:
            order_parts: list[str] = []
            # Use alias_map from flow when WITH or match-after-with was used
            if not (self._with_called or self._match_clauses_after_with):
                alias_map = self._build_alias_map_from_match_clauses(
                    include_edges=True
                )
                if not alias_map:
                    for entity in self._entities:
                        self._add_to_alias_map(entity, alias_map)
            for expr in self._order_by_clauses:
                if hasattr(expr, "to_cypher"):
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

    def _entity_to_match_pattern(self, entity: Any) -> str:
        """
        Convert entity (Node class, alias, tuple pattern, etc.) to MATCH pattern.

        Supports:
        - Node class: Page.alias("a") → (a:Page)
        - Tuple pattern: (Page.alias("a"), Linked.alias("r"), Page.alias("b")) → (a:Page)-[r:Linked]->(b:Page)
        - String pattern: "(a:Page)-[r:Linked]->(b:Page)" → as-is

        :param entity: Node class, alias, tuple pattern, or string
        :return: MATCH pattern string or None
        """
        # Handle tuple patterns (relationship patterns)
        if isinstance(entity, tuple) and len(entity) == 3:
            src, edge, dst = entity
            src_pattern = self._entity_to_match_pattern(src)
            edge_pattern = self._edge_to_match_pattern(edge)
            dst_pattern = self._entity_to_match_pattern(dst)

            if src_pattern and edge_pattern and dst_pattern:
                # Combine patterns: (src)-[edge]->(dst)
                # src_pattern and dst_pattern already have parentheses
                # Add direction -> for edges (outgoing relationship)
                return f"{src_pattern}-{edge_pattern}->{dst_pattern}"
            return None

        # Handle Node classes and aliases
        if isinstance(entity, type):
            # Check if it's an aliased class first (has _alias attribute)
            if hasattr(entity, "_alias"):
                alias = entity._alias
                # Get the actual node class from the aliased class
                if hasattr(entity, "__bases__") and len(entity.__bases__) > 0:
                    base_class = entity.__bases__[0]
                    label = self._get_label_from_class(base_class)
                else:
                    label = entity.__name__
                return f"({alias}:{label})"
            # Regular Node class
            elif hasattr(entity, "__labels__"):
                alias = self._get_alias_for_entity(entity)
                label = self._get_label_from_class(entity)
                return f"({alias}:{label})"
        elif isinstance(entity, str):
            # String pattern (for complex patterns)
            return entity

        return None


def select(*entities: Any) -> Select[Any]:
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
    if not hasattr(AliasedNode, "_alias"):
        AliasedNode._alias = name

    # Copy class attributes
    AliasedNode.__name__ = f"Aliased{node_class.__name__}"
    AliasedNode.__qualname__ = f"Aliased{node_class.__qualname__}"

    # Ensure __labels__ is copied from parent
    if hasattr(node_class, "__labels__"):
        AliasedNode.__labels__ = node_class.__labels__

    return AliasedNode
