"""
Relationship descriptor for lazy loading of related nodes.

This module provides Relationship class that acts as a descriptor for node relationships,
allowing lazy loading of related nodes when accessed.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, List, Union, TypeVar, Generic

if TYPE_CHECKING:
    from .node import Node
    from .edge import Edge

N = TypeVar('N', bound="Node")


class Relationship(Generic[N]):
    """
    Relationship descriptor for lazy loading of related nodes.
    
    Acts as a descriptor that loads related nodes when accessed on a node instance.
    """
    
    def __init__(self, edge_class: Union[type[Edge], str], 
                 direction: str = "outgoing",
                 related_node_type: type[N] | None = None):
        """
        Initialize relationship descriptor.
        
        :param edge_class: Edge class that represents the relationship, or relation name as string
        :param direction: Direction of relationship ("outgoing", "incoming", or "both")
        :param related_node_type: Optional type of related nodes for type safety
        """
        self.edge_class = edge_class
        self.direction = direction
        self.related_node_type = related_node_type
        edge_name = edge_class.__name__ if isinstance(edge_class, type) else edge_class
        self._cache_attr = f"_cached_{edge_name}_{direction}"
    
    def __get__(self, instance: Node, owner: type) -> List[N]:
        """
        Descriptor protocol: load related nodes when accessed.
        
        :param instance: The node instance
        :param owner: The class that owns this descriptor
        :return: List of related nodes
        """
        if instance is None:
            # Accessed as class attribute - return Relationship object
            return self
        
        # Check cache first
        if hasattr(instance, self._cache_attr):
            return getattr(instance, self._cache_attr)
        
        # Load related nodes
        related = self._load_related(instance)
        
        # Cache result
        setattr(instance, self._cache_attr, related)
        
        return related
    
    def _load_related(self, instance: Node) -> List[N]:
        """
        Load related nodes via MATCH query.
        
        :param instance: Node instance to load relationships for
        :return: List of related nodes
        """
        if not hasattr(instance, "__graph__") or instance.__graph__ is None:
            return []
        
        graph = instance.__graph__
        
        # Get edge relation name
        if isinstance(self.edge_class, str):
            relation = self.edge_class
        elif hasattr(self.edge_class, "__relation_name__"):
            relation = self.edge_class.__relation_name__
        elif hasattr(self.edge_class, "__relation__"):
            relation = self.edge_class.__relation__
        else:
            relation = self.edge_class.__name__
        
        # Get node label
        if hasattr(instance, "__labels__"):
            label = list(instance.__labels__)[0]
        else:
            label = instance.__class__.__name__
        
        # Build WHERE clause using primary key (more reliable than id)
        where_clause = ""
        params = {}
        
        if hasattr(instance, "__primary_key__") and instance.__primary_key__:
            pk = instance.__primary_key__
            if isinstance(pk, str):
                pk_fields = [pk]
            else:
                pk_fields = pk
            
            # Build WHERE clause with primary key
            where_parts = []
            for i, pk_field in enumerate(pk_fields):
                param_name = f"pk_{i}"
                where_parts.append(f"n.{pk_field} = ${param_name}")
                # Get property value - properties is a @property that returns a dict
                props = instance.properties
                if hasattr(props, 'get'):
                    params[param_name] = props.get(pk_field)
                elif isinstance(props, dict):
                    params[param_name] = props.get(pk_field)
                else:
                    # Fallback to PropertiesManager
                    if hasattr(instance, '_properties_manager'):
                        params[param_name] = instance._properties_manager.get(pk_field)
                    else:
                        params[param_name] = getattr(instance, pk_field, None)
            
            if where_parts:
                where_clause = "WHERE " + " AND ".join(where_parts)
        elif instance.id is not None:
            # Fallback to id if primary key not available
            where_clause = "WHERE id(n) = $node_id"
            params = {"node_id": instance.id}
        
        # Build query based on direction
        if self.direction == "outgoing":
            # (instance)-[r:Relation]->(related)
            query = f"MATCH (n:{label})-[r:{relation}]->(related) {where_clause} RETURN related"
        elif self.direction == "incoming":
            # (related)-[r:Relation]->(instance) - find nodes that link TO the instance
            query = f"MATCH (related)-[r:{relation}]->(n:{label}) {where_clause} RETURN related"
        else:  # both
            # (instance)-[r:Relation]-(related)
            query = f"MATCH (n:{label})-[r:{relation}]-(related) {where_clause} RETURN related"
        
        # Execute query
        result = graph.query(query, params=params)
        
        # Extract nodes from result
        related_nodes = []
        if not result.is_empty():
            for row in result.result_set:
                if row and len(row) > 0:
                    related_nodes.append(row[0])
        
        return related_nodes
    
    def clear_cache(self, instance: Node) -> None:
        """
        Clear cached related nodes for an instance.
        
        :param instance: Node instance to clear cache for
        """
        if hasattr(instance, self._cache_attr):
            delattr(instance, self._cache_attr)
