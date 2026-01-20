"""
Properties management system for graphorm.

This module provides an isolated, testable system for managing properties
of Node and Edge instances. It separates user-defined properties from
internal attributes and provides validation capabilities.
"""
from typing import Any, Dict, Optional, Callable, Set
from abc import ABC, abstractmethod


class PropertiesValidator(ABC):
    """Abstract base class for property validators."""
    
    @abstractmethod
    def validate(self, key: str, value: Any) -> Any:
        """
        Validate and optionally transform a property value.
        
        :param key: Property key
        :param value: Property value to validate
        :return: Validated (and possibly transformed) value
        :raises ValueError: If validation fails
        """
        pass


class DefaultPropertiesValidator(PropertiesValidator):
    """Default validator that performs basic type checking based on annotations."""
    
    def __init__(self, annotations: Dict[str, type]):
        """
        Initialize validator with type annotations.
        
        :param annotations: Dictionary of property names to their types
        """
        self.annotations = annotations
    
    def validate(self, key: str, value: Any) -> Any:
        """
        Validate property value against type annotation.
        
        :param key: Property key
        :param value: Property value
        :return: Validated value
        """
        if key in self.annotations:
            expected_type = self.annotations[key]
            # Handle Optional types (Union[Type, None])
            if hasattr(expected_type, '__origin__'):
                if expected_type.__origin__ is type(None).__class__:
                    # This is Optional, get the actual type
                    if hasattr(expected_type, '__args__'):
                        args = expected_type.__args__
                        if len(args) == 2 and type(None) in args:
                            # Extract the non-None type
                            actual_type = next(t for t in args if t is not type(None))
                            expected_type = actual_type
            
            # Allow None values for any type (flexible validation)
            if value is None:
                return value
            
            # Type checking (lenient - allows subclasses)
            if not isinstance(value, expected_type):
                # Try to convert if possible
                try:
                    if expected_type == bool and isinstance(value, (int, str)):
                        # Convert int/str to bool
                        if isinstance(value, int):
                            return bool(value)
                        if isinstance(value, str):
                            return value.lower() in ('true', '1', 'yes', 'on')
                    elif expected_type in (int, float) and isinstance(value, (int, float, str)):
                        return expected_type(value)
                    elif expected_type == str:
                        return str(value)
                except (ValueError, TypeError):
                    pass
                
                # If conversion fails, still allow but log warning
                # This maintains backward compatibility
                return value
        
        return value


class PropertiesManager:
    """
    Isolated manager for properties.
    
    This class provides a clean interface for managing properties,
    separating them from internal attributes and providing validation.
    """
    
    def __init__(
        self,
        initial_data: Optional[Dict[str, Any]] = None,
        validator: Optional[PropertiesValidator] = None,
        internal_keys: Optional[Set[str]] = None
    ):
        """
        Initialize PropertiesManager.
        
        :param initial_data: Initial properties dictionary
        :param validator: Validator instance (optional)
        :param internal_keys: Set of keys that should be excluded from properties
        """
        self._properties: Dict[str, Any] = {}
        self._validator = validator
        self._internal_keys = internal_keys or set()
        
        if initial_data:
            self.update(initial_data)
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a property value.
        
        :param key: Property key
        :param default: Default value if key not found
        :return: Property value or default
        """
        return self._properties.get(key, default)
    
    def set(self, key: str, value: Any) -> None:
        """
        Set a property value with validation.
        
        :param key: Property key
        :param value: Property value
        """
        # Skip internal keys
        if key in self._internal_keys:
            return
        
        # Validate if validator is set
        if self._validator:
            value = self._validator.validate(key, value)
        
        self._properties[key] = value
    
    def update(self, data: Dict[str, Any]) -> None:
        """
        Update multiple properties at once.
        
        :param data: Dictionary of properties to update
        """
        for key, value in data.items():
            self.set(key, value)
    
    def delete(self, key: str) -> None:
        """
        Delete a property.
        
        :param key: Property key to delete
        """
        self._properties.pop(key, None)
    
    def clear(self) -> None:
        """Clear all properties."""
        self._properties.clear()
    
    def keys(self) -> set:
        """Get all property keys."""
        return set(self._properties.keys())
    
    def items(self) -> Dict[str, Any]:
        """
        Get all properties as a dictionary.
        
        :return: Dictionary of properties
        """
        return dict(self._properties)
    
    def __contains__(self, key: str) -> bool:
        """Check if property exists."""
        return key in self._properties
    
    def __getitem__(self, key: str) -> Any:
        """Get property by key."""
        return self._properties[key]
    
    def __setitem__(self, key: str, value: Any) -> None:
        """Set property by key."""
        self.set(key, value)
    
    def __delitem__(self, key: str) -> None:
        """Delete property by key."""
        self.delete(key)
    
    def __len__(self) -> int:
        """Get number of properties."""
        return len(self._properties)
    
    def __iter__(self):
        """Iterate over property keys."""
        return iter(self._properties)
    
    def __eq__(self, other) -> bool:
        """Compare properties with another PropertiesManager or dict."""
        if isinstance(other, PropertiesManager):
            return self._properties == other._properties
        elif isinstance(other, dict):
            return self._properties == other
        return False
    
    def __repr__(self) -> str:
        """String representation."""
        return f"PropertiesManager({self._properties})"
    
    def copy(self) -> 'PropertiesManager':
        """
        Create a copy of this PropertiesManager.
        
        :return: New PropertiesManager instance with copied properties
        """
        new_manager = PropertiesManager(
            initial_data=self._properties.copy(),
            validator=self._validator,
            internal_keys=self._internal_keys.copy() if self._internal_keys else None
        )
        return new_manager
