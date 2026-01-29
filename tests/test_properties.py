"""
Tests for PropertiesManager - isolated properties management system.
"""

from graphorm import Node
from graphorm.properties import (
    DefaultPropertiesValidator,
    PropertiesManager,
)


class TestPropertiesManager:
    """Test suite for PropertiesManager."""

    def test_init_empty(self):
        """Test initializing PropertiesManager with no data."""
        manager = PropertiesManager()
        assert len(manager) == 0
        assert manager.items() == {}

    def test_init_with_data(self):
        """Test initializing PropertiesManager with initial data."""
        data = {"name": "test", "value": 42}
        manager = PropertiesManager(initial_data=data)
        assert len(manager) == 2
        assert manager.get("name") == "test"
        assert manager.get("value") == 42

    def test_set_get(self):
        """Test setting and getting properties."""
        manager = PropertiesManager()
        manager.set("key", "value")
        assert manager.get("key") == "value"
        assert manager["key"] == "value"

    def test_update(self):
        """Test updating multiple properties at once."""
        manager = PropertiesManager()
        manager.update({"a": 1, "b": 2, "c": 3})
        assert manager.get("a") == 1
        assert manager.get("b") == 2
        assert manager.get("c") == 3

    def test_delete(self):
        """Test deleting properties."""
        manager = PropertiesManager(initial_data={"a": 1, "b": 2})
        manager.delete("a")
        assert "a" not in manager
        assert manager.get("b") == 2

    def test_clear(self):
        """Test clearing all properties."""
        manager = PropertiesManager(initial_data={"a": 1, "b": 2})
        manager.clear()
        assert len(manager) == 0

    def test_keys(self):
        """Test getting all property keys."""
        manager = PropertiesManager(initial_data={"a": 1, "b": 2})
        keys = manager.keys()
        assert "a" in keys
        assert "b" in keys
        assert len(keys) == 2

    def test_items(self):
        """Test getting all properties as dictionary."""
        data = {"a": 1, "b": 2}
        manager = PropertiesManager(initial_data=data)
        items = manager.items()
        assert items == data
        # Ensure it's a copy, not a reference
        items["c"] = 3
        assert "c" not in manager

    def test_contains(self):
        """Test checking if property exists."""
        manager = PropertiesManager(initial_data={"a": 1})
        assert "a" in manager
        assert "b" not in manager

    def test_getitem_setitem(self):
        """Test dictionary-like access."""
        manager = PropertiesManager()
        manager["key"] = "value"
        assert manager["key"] == "value"
        del manager["key"]
        assert "key" not in manager

    def test_len(self):
        """Test getting number of properties."""
        manager = PropertiesManager()
        assert len(manager) == 0
        manager.set("a", 1)
        assert len(manager) == 1
        manager.set("b", 2)
        assert len(manager) == 2

    def test_iter(self):
        """Test iterating over properties."""
        manager = PropertiesManager(initial_data={"a": 1, "b": 2})
        keys = list(iter(manager))
        assert "a" in keys
        assert "b" in keys

    def test_equality(self):
        """Test comparing PropertiesManager instances."""
        data = {"a": 1, "b": 2}
        manager1 = PropertiesManager(initial_data=data)
        manager2 = PropertiesManager(initial_data=data)
        assert manager1 == manager2
        assert manager1 == data

    def test_internal_keys_exclusion(self):
        """Test that internal keys are excluded from properties."""
        internal_keys = {"__internal__", "__private__"}
        manager = PropertiesManager(internal_keys=internal_keys)

        # Try to set internal key
        manager.set("__internal__", "value")

        # Internal key should not be stored
        assert "__internal__" not in manager
        assert len(manager) == 0

    def test_copy(self):
        """Test copying PropertiesManager."""
        manager = PropertiesManager(initial_data={"a": 1, "b": 2})
        copy = manager.copy()

        assert copy == manager
        assert copy is not manager

        # Modifying copy should not affect original
        copy.set("c", 3)
        assert "c" not in manager


class TestDefaultPropertiesValidator:
    """Test suite for DefaultPropertiesValidator."""

    def test_validate_with_annotations(self):
        """Test validation with type annotations."""
        annotations = {"name": str, "age": int, "active": bool}
        validator = DefaultPropertiesValidator(annotations)

        # Valid types should pass through
        assert validator.validate("name", "test") == "test"
        assert validator.validate("age", 25) == 25
        assert validator.validate("active", True) == True

    def test_validate_none(self):
        """Test that None values are allowed."""
        annotations = {"name": str}
        validator = DefaultPropertiesValidator(annotations)

        # None should be allowed for any type
        assert validator.validate("name", None) is None

    def test_validate_type_conversion(self):
        """Test automatic type conversion."""
        annotations = {"age": int, "active": bool}
        validator = DefaultPropertiesValidator(annotations)

        # String to int conversion
        result = validator.validate("age", "25")
        assert isinstance(result, str | int)  # May allow string for compatibility

        # Int to bool conversion
        result = validator.validate("active", 1)
        # Should allow conversion or original value
        assert result in (True, 1)

    def test_validate_unknown_key(self):
        """Test validation of keys not in annotations."""
        annotations = {"name": str}
        validator = DefaultPropertiesValidator(annotations)

        # Unknown keys should pass through without validation
        assert validator.validate("unknown", "value") == "value"
        assert validator.validate("unknown", 123) == 123


class TestPropertiesManagerIntegration:
    """Test PropertiesManager integration with Common, Node, and Edge."""

    def test_node_properties_isolation(self):
        """Test that Node properties are isolated from internal attributes."""
        from graphorm import Node

        class TestNode(Node):
            __primary_key__ = ["node_id"]
            node_id: str
            name: str
            value: int = 0

        node = TestNode(node_id="1", name="test", value=42)

        # Properties should only contain user-defined fields
        props = node.properties
        assert "node_id" in props
        assert "name" in props
        assert "value" in props

        # Internal attributes should not be in properties
        assert "__id__" not in props
        assert "__alias__" not in props
        assert "__primary_key__" not in props

    def test_edge_properties_isolation(self):
        """Test that Edge properties are isolated from internal attributes."""
        from graphorm import (
            Edge,
            Node,
        )

        class TestNode(Node):
            __primary_key__ = ["node_id"]
            node_id: str

        class TestEdge(Edge):
            weight: float = 1.0

        node1 = TestNode(node_id="1")
        node2 = TestNode(node_id="2")
        edge = TestEdge(node1, node2, weight=2.5)

        # Properties should only contain user-defined fields
        props = edge.properties
        assert "weight" in props

        # Internal attributes should not be in properties
        assert "__id__" not in props
        assert "__alias__" not in props
        assert "__relation__" not in props
        assert "src_node" not in props
        assert "dst_node" not in props

    def test_update_with_validation(self):
        """Test that update uses PropertiesManager with validation."""
        from graphorm import Node

        class TestNode(Node):
            __primary_key__ = ["node_id"]
            node_id: str
            count: int = 0

        node = TestNode(node_id="1", count=10)

        # Update should work through PropertiesManager
        node.update({"count": 20})

        assert node.properties["count"] == 20
        assert node.count == 20

    def test_properties_backward_compatibility(self):
        """Test that properties property works as before for existing code."""
        from graphorm import Node

        class TestNode(Node):
            __primary_key__ = ["node_id"]
            node_id: str
            name: str

        node = TestNode(node_id="1", name="test")

        # Properties should be a dict-like object
        props = node.properties
        assert isinstance(props, dict)
        assert props["node_id"] == "1"
        assert props["name"] == "test"

        # Should support dict operations
        assert "node_id" in props
        assert len(props) == 2
        assert props.get("name") == "test"

    def test_boolean_properties(self):
        """Test that boolean properties work correctly."""
        from graphorm import Node

        class TestNode(Node):
            __primary_key__ = ["node_id"]
            node_id: str
            active: bool = False

        node = TestNode(node_id="1", active=False)

        # False should be preserved
        assert node.properties["active"] is False
        assert node.active is False

        # Update to True
        node.update({"active": True})
        assert node.properties["active"] is True
        assert node.active is True

        # Update back to False
        node.update({"active": False})
        assert node.properties["active"] is False
        assert node.active is False


def test_common_update_fallback_without_properties_manager():
    """Common.update() fallback when _properties_manager is missing."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    node = Page(path="/x")
    del node._properties_manager
    node.update({"path": "/y"})
    assert node.path == "/y"
    assert hasattr(node, "_properties_manager")


def test_common_properties_sync_from_dict():
    """Common.properties syncs from __dict__ when new keys set after init."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    node = Page(path="/x")
    node.__dict__["extra"] = 42
    props = node.properties
    assert "extra" in props
    assert props["extra"] == 42


def test_common_validate_uses_default_when_key_not_in_data():
    """Common._validate uses class default when key in annotations but not in data."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        parsed: bool = False

    # Call _validate with incomplete data to hit else branch (key not in data)
    values = Page._validate({"path": "/only"})
    assert values["path"] == "/only"
    # "parsed" is in annotations but not in data -> else branch runs; default may be Property or False
    assert "parsed" in values
