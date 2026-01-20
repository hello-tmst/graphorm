"""
Property descriptor for Node and Edge classes.

This module provides Property class that acts as a descriptor for node/edge properties,
allowing them to be used in queries with operators similar to SQLAlchemy 2.0.
"""
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .node import Node
    from .edge import Edge
    from .expression import BinaryExpression, OrderByExpression


class Property:
    """
    Property descriptor for Node and Edge classes.
    
    Acts as a descriptor that returns Property objects when accessed as class attributes,
    and property values when accessed as instance attributes.
    """
    
    def __init__(self, node_class: type["Node"], name: str, alias: str = None):
        """
        Initialize Property descriptor.
        
        :param node_class: The Node or Edge class this property belongs to
        :param name: The name of the property
        :param alias: Optional alias for the node/edge in queries
        """
        self.node_class = node_class
        self.name = name
        self._alias = alias
    
    def __get__(self, instance: Any, owner: type) -> Any:
        """
        Descriptor protocol: return Property object when accessed as class attribute,
        property value when accessed as instance attribute.
        
        :param instance: The instance (None if accessed as class attribute)
        :param owner: The class that owns this descriptor
        :return: Property object or property value
        """
        if instance is None:
            # Accessed as class attribute - return Property object with owner class
            # Create new Property instance bound to the owner class
            prop = Property.__new__(Property)
            prop.node_class = owner
            prop.name = self.name
            prop._alias = self._alias
            return prop
        else:
            # Accessed as instance attribute - return property value
            return instance.properties.get(self.name)
    
    def __set__(self, instance: Any, value: Any) -> None:
        """
        Descriptor protocol: set property value on instance.
        
        :param instance: The instance to set the property on
        :param value: The value to set
        """
        if instance is not None:
            instance.update({self.name: value})
    
    def set_alias(self, alias: str) -> "Property":
        """Set alias for this property in queries."""
        self._alias = alias
        return self
    
    def __eq__(self, other: Any) -> "BinaryExpression":
        """Equality operator: =="""
        from .expression import BinaryExpression
        return BinaryExpression(self, "=", other)
    
    def __ne__(self, other: Any) -> "BinaryExpression":
        """Inequality operator: <> (Cypher uses <> instead of !=)"""
        from .expression import BinaryExpression
        return BinaryExpression(self, "<>", other)
    
    def __lt__(self, other: Any) -> "BinaryExpression":
        """Less than operator: <"""
        from .expression import BinaryExpression
        return BinaryExpression(self, "<", other)
    
    def __le__(self, other: Any) -> "BinaryExpression":
        """Less than or equal operator: <="""
        from .expression import BinaryExpression
        return BinaryExpression(self, "<=", other)
    
    def __gt__(self, other: Any) -> "BinaryExpression":
        """Greater than operator: >"""
        from .expression import BinaryExpression
        return BinaryExpression(self, ">", other)
    
    def __ge__(self, other: Any) -> "BinaryExpression":
        """Greater than or equal operator: >="""
        from .expression import BinaryExpression
        return BinaryExpression(self, ">=", other)
    
    def in_(self, values: list[Any]) -> "BinaryExpression":
        """
        IN operator: property IN [values]
        
        :param values: List of values to check against
        :return: BinaryExpression with IN operator
        """
        from .expression import BinaryExpression
        return BinaryExpression(self, "IN", values)
    
    def not_in(self, values: list[Any]) -> "BinaryExpression":
        """
        NOT IN operator: property NOT IN [values]
        
        :param values: List of values to check against
        :return: BinaryExpression with NOT IN operator
        """
        from .expression import BinaryExpression
        return BinaryExpression(self, "NOT IN", values)
    
    def like(self, pattern: str) -> "BinaryExpression":
        """
        LIKE operator (regex match): property =~ pattern
        Note: RedisGraph/FalkorDB may not support =~, consider using contains() or starts_with() instead.
        
        :param pattern: Regex pattern to match
        :return: BinaryExpression with =~ operator
        """
        from .expression import BinaryExpression
        # Note: RedisGraph doesn't support =~, but we'll keep it for compatibility
        # Users should use contains() or starts_with() for RedisGraph
        return BinaryExpression(self, "=~", pattern)
    
    def contains(self, value: Any) -> "BinaryExpression":
        """
        CONTAINS operator: property CONTAINS value
        
        :param value: Value to check if property contains
        :return: BinaryExpression with CONTAINS operator
        """
        from .expression import BinaryExpression
        return BinaryExpression(self, "CONTAINS", value)
    
    def starts_with(self, value: str) -> "BinaryExpression":
        """
        STARTS WITH operator: property STARTS WITH value
        
        :param value: Value to check if property starts with
        :return: BinaryExpression with STARTS WITH operator
        """
        from .expression import BinaryExpression
        return BinaryExpression(self, "STARTS WITH", value)
    
    def ends_with(self, value: str) -> "BinaryExpression":
        """
        ENDS WITH operator: property ENDS WITH value
        
        :param value: Value to check if property ends with
        :return: BinaryExpression with ENDS WITH operator
        """
        from .expression import BinaryExpression
        return BinaryExpression(self, "ENDS WITH", value)
    
    def is_null(self) -> "BinaryExpression":
        """
        IS NULL operator: property IS NULL
        
        :return: BinaryExpression with IS NULL operator
        """
        from .expression import BinaryExpression
        return BinaryExpression(self, "IS NULL", None)
    
    def is_not_null(self) -> "BinaryExpression":
        """
        IS NOT NULL operator: property IS NOT NULL
        
        :return: BinaryExpression with IS NOT NULL operator
        """
        from .expression import BinaryExpression
        return BinaryExpression(self, "IS NOT NULL", None)
    
    def asc(self) -> "OrderByExpression":
        """
        ASC ordering: property ASC
        
        :return: OrderByExpression for ascending order
        """
        from .expression import OrderByExpression
        return OrderByExpression(self, "ASC")
    
    def desc(self) -> "OrderByExpression":
        """
        DESC ordering: property DESC
        
        :return: OrderByExpression for descending order
        """
        from .expression import OrderByExpression
        return OrderByExpression(self, "DESC")
    
    def to_cypher(self, alias: str = None, alias_map: dict[Any, str] = None) -> str:
        """
        Generate Cypher representation of this property.
        
        :param alias: Optional alias to use (overrides self._alias and alias_map)
        :param alias_map: Optional mapping of node classes to aliases
        :return: Cypher string representation
        """
        if alias:
            node_alias = alias
        elif alias_map and self.node_class and self.node_class in alias_map:
            # Use alias from map
            node_alias = alias_map[self.node_class]
        elif self._alias:
            node_alias = self._alias
        else:
            node_alias = self._get_default_alias()
        return f"{node_alias}.{self.name}"
    
    def _get_default_alias(self) -> str:
        """
        Get default alias for the node class.
        
        :return: Default alias string
        """
        # Use lowercase class name as default alias
        if self.node_class:
            # Handle aliased classes
            if hasattr(self.node_class, '_alias'):
                return self.node_class._alias
            return self.node_class.__name__.lower()
        return "n"
