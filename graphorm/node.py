import json
from typing import Any
from logging import getLogger
from stringcase import camelcase

from .registry import Registry
from .common import Common
from .utils import quote_string, random_string, format_cypher_value

logger = getLogger(__file__)


class Node(Common):
    __slots__ = {"__graph__", "__relations__", "__alias__", "__primary_key__", "__labels__"}

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
        setattr(cls, "__labels__", {camelcase(cls.__name__)})

        Registry.add_node_label(cls)

    def set_alias(self, alias: str) -> None:
        """
        Set Node alias.

        :param alias:
        :return:
        """
        setattr(self, "__alias__", alias)

    @property
    def alias(self) -> str:
        return self.__alias__

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
        if isinstance(self.__primary_key__, str):
            pk = self.__primary_key__
            res += "{" + f"{pk}:{format_cypher_value(self.properties[pk])}" + "}"
        elif isinstance(self.__primary_key__, list):
            props = ",".join(f"{pk}:{format_cypher_value(self.properties[pk])}" for pk in self.__primary_key__)
            res += "{" + props + "}"
        res += ")"
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
        pk_set = set(self.__primary_key__ if isinstance(self.__primary_key__, list) else [self.__primary_key__])
        
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
