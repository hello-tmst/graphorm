import json
from logging import getLogger

from .registry import Registry
from .utils import quote_string, random_string, format_cypher_value
from .node import Node
from .common import Common

logger = getLogger(__file__)


class Edge(Common):
    __slots__ = {"__alias__", "__relation__", "src_node", "dst_node"}

    def __new__(cls, src_node: Node, dst_node: Node, *, _id: int = None, **kwargs) -> Common:
        obj = super().__new__(cls, **kwargs)

        if src_node is None or dst_node is None:
            raise ValueError("Both src_node & dst_node must be provided")

        setattr(obj, "__id__", _id)
        setattr(obj, "__alias__", random_string())
        setattr(obj, "src_node", src_node)
        setattr(obj, "dst_node", dst_node)
        return obj

    def __init_subclass__(cls) -> None:
        # Check for explicit relation name, otherwise use class name as-is
        if hasattr(cls, "__relation_name__"):
            relation = cls.__relation_name__
            if not relation or not isinstance(relation, str):
                raise ValueError(f"__relation_name__ must be a non-empty string for class {cls.__name__}")
        else:
            relation = cls.__name__
        
        setattr(cls, "__relation__", relation)
        Registry.add_edge_relation(cls)
        
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
        Create an aliased version of this Edge class for use in queries.
        
        :param name: Alias name for the edge in queries
        :return: Aliased Edge class with _alias attribute set
        """
        # Create a simple subclass with alias attribute
        # __init_subclass__ will be called automatically, creating Property descriptors
        class AliasedEdge(cls):
            _alias = name
        
        # Ensure __relation__ is copied
        if hasattr(cls, '__relation__'):
            AliasedEdge.__relation__ = cls.__relation__
        
        # Set alias as class attribute
        AliasedEdge._alias = name
        AliasedEdge.__name__ = f"Aliased{cls.__name__}"
        AliasedEdge.__qualname__ = f"Aliased{cls.__qualname__}"
        
        # Property descriptors are automatically created in __init_subclass__
        # with node_class=AliasedEdge, so they should work correctly
        # The Property.__get__ method will use the owner class (AliasedEdge) and its _alias
        
        return AliasedEdge
    
    class _AliasDescriptor:
        """Descriptor that handles both classmethod and instance property for alias."""
        def __get__(self, obj, owner):
            if obj is None:
                # Accessed on class - return the classmethod
                return owner._alias_classmethod
            else:
                # Accessed on instance - return the instance's alias
                return obj.__alias__
    
    alias = _AliasDescriptor()

    def set_alias(self, alias: str) -> None:
        """
        Set Node alias.

        :param alias:
        :return:
        """
        setattr(self, "__alias__", alias)

    @property
    def relation(self) -> str:
        return self.__relation__

    def __str_pk__(self) -> str:
        """
        Generate primary key of Node instance.

        :return:
        """
        res = "["
        res += f"{self.alias}:{self.relation}"
        res += "]"
        return res

    def __str__(self):
        # Source node.
        if isinstance(self.src_node, Node):
            res = f"({str(self.src_node.alias)})"
        else:
            res = "()"

        # Edge
        res += "-["
        res += f"{self.alias}:{self.relation}"
        if self.properties:
            props = ",".join(
                f"{k}:{quote_string(v)}" for k, v in sorted(self.properties.items()) if v is not None
            )
            res += "{" + props + "}"
        res += "]->"

        # Dest node.
        if isinstance(self.dst_node, Node):
            res += f"({str(self.dst_node.alias)})"
        else:
            res += "()"

        return res

    # Properties are now managed by PropertiesManager in Common class
    # The properties property is inherited from Common and returns
    # only user-defined properties, excluding internal attributes

    def merge(self):
        # When nodes and edges are committed separately, we need to MATCH nodes by primary key
        # instead of using alias (which only works in the same query)
        if isinstance(self.src_node, Node) and isinstance(self.dst_node, Node):
            # Build MATCH patterns for nodes using primary keys
            src_labels = ":".join(self.src_node.labels)
            dst_labels = ":".join(self.dst_node.labels)
            
            # Build source node pattern
            if isinstance(self.src_node.__primary_key__, str):
                pk = self.src_node.__primary_key__
                src_pattern = f"{{ {pk}: {format_cypher_value(self.src_node.properties[pk])} }}"
            elif isinstance(self.src_node.__primary_key__, list):
                props = ",".join(
                    f"{pk}:{format_cypher_value(self.src_node.properties[pk])}" 
                    for pk in self.src_node.__primary_key__
                )
                src_pattern = f"{{ {props} }}"
            else:
                src_pattern = ""
            
            # Build destination node pattern
            if isinstance(self.dst_node.__primary_key__, str):
                pk = self.dst_node.__primary_key__
                dst_pattern = f"{{ {pk}: {format_cypher_value(self.dst_node.properties[pk])} }}"
            elif isinstance(self.dst_node.__primary_key__, list):
                props = ",".join(
                    f"{pk}:{format_cypher_value(self.dst_node.properties[pk])}" 
                    for pk in self.dst_node.__primary_key__
                )
                dst_pattern = f"{{ {props} }}"
            else:
                dst_pattern = ""
            
            # Build edge pattern
            edge_pattern = f"-[{self.alias}:{self.relation}"
            if self.properties:
                props = ",".join(
                    f"{k}:{quote_string(v)}" for k, v in sorted(self.properties.items()) if v is not None
                )
                edge_pattern += "{" + props + "}"
            edge_pattern += "]->"
            
            # MATCH nodes by primary key, then MERGE edge (to avoid duplicates)
            src_var = "src"
            dst_var = "dst"
            if src_pattern and dst_pattern:
                return f"MATCH ({src_var}:{src_labels} {src_pattern}), ({dst_var}:{dst_labels} {dst_pattern}) MERGE ({src_var}){edge_pattern}({dst_var})"
            else:
                # Fallback to original if no primary key
                return "MERGE " + str(self)
        return "MERGE " + str(self)

    def __eq__(self, rhs):
        # Quick positive check, if both IDs are set.
        if self.id is not None and rhs.id is not None and self.id == rhs.id:
            return True

        # Source and destination nodes should match.
        if self.src_node != rhs.src_node:
            return False

        if self.dst_node != rhs.dst_node:
            return False

        # Relation should match.
        if self.relation != rhs.relation:
            return False

        # Quick check for number of properties.
        if len(self.properties) != len(rhs.properties):
            return False

        # Compare properties.
        if self.properties != rhs.properties:
            return False

        return True

    def __hash__(self) -> int:
        return hash((self.relation, self.src_node, self.dst_node, json.dumps(self.properties)))
