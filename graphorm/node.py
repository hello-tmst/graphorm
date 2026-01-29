from __future__ import annotations

import json
from logging import getLogger
from typing import Any

from .common import Common
from .exceptions import QueryExecutionError
from .registry import Registry
from .utils import (
    format_cypher_value,
    format_pk_cypher_map,
    get_pk_fields,
    random_string,
)

logger = getLogger(__file__)


class Node(Common):
    __slots__ = {
        "__graph__",
        "__relations__",
        "__alias__",
        "__primary_key__",
        "__labels__",
    }

    def __new__(cls, _id: int = None, **kwargs) -> Common:
        """
        Create new instance of Node.

        :param _id:
        :param kwargs:
        """
        obj = super().__new__(cls, **kwargs)

        setattr(obj, "__id__", _id)
        setattr(obj, "__alias__", random_string())
        return obj

    def __init_subclass__(cls) -> None:
        # Check for explicit label, otherwise use class name as-is
        if hasattr(cls, "__label__"):
            label = cls.__label__
            if not label or not isinstance(label, str):
                raise ValueError(
                    f"__label__ must be a non-empty string for class {cls.__name__}"
                )
        else:
            label = cls.__name__

        setattr(cls, "__labels__", {label})
        Registry.add_node_label(cls)

        # Create Property descriptors for all annotated properties
        from .property import Property

        if hasattr(cls, "__annotations__"):
            for prop_name in cls.__annotations__:
                # Skip internal attributes
                if not prop_name.startswith("__"):
                    # Create Property descriptor
                    setattr(cls, prop_name, Property(cls, prop_name))

    @classmethod
    def _alias_classmethod(cls, name: str) -> type:
        """
        Create an aliased version of this Node class for use in queries.

        :param name: Alias name for the node in queries
        :return: Aliased Node class with _alias attribute set
        """

        # Create a simple subclass with alias attribute
        # __init_subclass__ will be called automatically, creating Property descriptors
        class AliasedNode(cls):
            _alias = name

        # Ensure __labels__ is copied
        if hasattr(cls, "__labels__"):
            AliasedNode.__labels__ = cls.__labels__

        # Set alias as class attribute
        AliasedNode._alias = name
        AliasedNode.__name__ = f"Aliased{cls.__name__}"
        AliasedNode.__qualname__ = f"Aliased{cls.__qualname__}"

        # Property descriptors are automatically created in __init_subclass__
        # with node_class=AliasedNode, so they should work correctly
        # The Property.__get__ method will use the owner class (AliasedNode) and its _alias

        return AliasedNode

    @classmethod
    def create_index(cls, property_name: str, graph: "Graph") -> "QueryResult | None":
        """
        Create an index on a property for this Node class (idempotent).

        :param property_name: Name of the property to index
        :param graph: Graph instance to create the index on
        :return: QueryResult on success, None if index already exists
        """
        # Get label for this node class
        if hasattr(cls, "__labels__"):
            label = list(cls.__labels__)[0]
        elif hasattr(cls, "__label__"):
            label = cls.__label__
        else:
            label = cls.__name__

        # Step 1: check via list_indexes()
        for idx in graph.list_indexes():
            if idx["label"] == label and property_name in idx.get("properties", []):
                logger.debug(
                    "Index on %s.%s already exists (via list_indexes)", label, property_name
                )
                return None

        # Step 2: create with race-condition protection
        query = f"CREATE INDEX ON :{label}({property_name})"
        try:
            result = graph.query(query)
            logger.debug("Created index on %s.%s", label, property_name)
            return result
        except QueryExecutionError as e:
            msg = str(e).lower()
            if "already indexed" in msg or "already exists" in msg:
                logger.debug(
                    "Index on %s.%s already exists (race condition)", label, property_name
                )
                return None
            raise

    def set_alias(self, alias: str) -> None:
        """
        Set Node alias.

        :param alias:
        :return:
        """
        setattr(self, "__alias__", alias)

    @property
    def labels(self) -> set[str]:
        return self.__labels__

    @property
    def graph(self):
        return self.__graph__

    @property
    def relations(self):
        return self.__relations__

    # Properties are now managed by PropertiesManager in Common class
    # The properties property is inherited from Common and returns
    # only user-defined properties, excluding internal attributes

    def __str_pk__(self) -> str:
        """
        Generate primary key of Node instance.

        :return:
        """
        res = "("
        res += ":".join([self.alias, *self.labels])
        pk_map = format_pk_cypher_map(self)
        if pk_map:
            res += pk_map
        res += ")"
        if not res or not res.startswith("(") or not res.endswith(")"):
            raise ValueError(
                f"Cannot form valid node pattern for {self}. "
                f"Labels: {self.labels}, PK: {get_pk_fields(self)}, Alias: {self.alias}"
            )
        return res

    def __str__(self) -> str:
        """
        Generate Node instance insertion constraint.

        :return:
        """

        res = self.__str_pk__()
        res += " SET "
        if self.properties:
            # Include all properties, including False values
            # Filter only None values, but include False, 0, empty strings, etc.
            set_clauses = []
            for k, v in sorted(self.properties.items()):
                # Include value if it's not None (this includes False, 0, empty strings, etc.)
                # False is not None, so it will be included
                if v is not None:
                    set_clauses.append(f"{self.alias}.{k}={format_cypher_value(v)}")
            if set_clauses:
                res += ", ".join(set_clauses)
        return res

    def merge(self) -> str:
        """
        Generate MERGE query for the node.

        Uses ON CREATE SET and ON MATCH SET to ensure properties are set
        both when creating new nodes and when updating existing ones.

        :return: MERGE query string
        """
        pk_pattern = self.__str_pk__()

        # Build SET clauses for all properties (excluding primary key properties)
        set_clauses = []
        pk_set = set(get_pk_fields(self))

        for k, v in sorted(self.properties.items()):
            # Skip primary key properties in SET (they're already in MERGE pattern)
            if k not in pk_set:
                if v is not None:
                    set_clauses.append(f"{self.alias}.{k}={format_cypher_value(v)}")

        if set_clauses:
            set_clause = ", ".join(set_clauses)
            # Use ON CREATE SET and ON MATCH SET to ensure properties are set in both cases
            return f"MERGE {pk_pattern} ON CREATE SET {set_clause} ON MATCH SET {set_clause}"
        else:
            # If no properties to set (only primary key), just MERGE
            return f"MERGE {pk_pattern}"

    def __eq__(self, other: Any):
        # Quick positive check, if both IDs are set.
        if self.id is not None and other.id is not None and self.id != other.id:
            return False

        # Label should match.
        if set(self.labels) ^ set(other.labels):
            return False

        # Quick check for number of properties.
        if len(self.properties) != len(other.properties):
            return False

        # Compare properties.
        if self.properties != other.properties:
            return False

        return True

    def __hash__(self) -> int:
        return hash((frozenset(self.labels), json.dumps(self.properties)))
