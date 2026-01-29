"""
Tests for graphorm.registry: get_node and get_edge KeyError.
"""

import pytest

from graphorm import Edge, Node
from graphorm.registry import Registry


def test_registry_get_node_raises_key_error_for_unknown_label():
    """Registry.get_node with unknown label raises KeyError with message."""
    with pytest.raises(KeyError, match="Node with label .* not found"):
        Registry.get_node("NonExistentLabel")


def test_registry_get_edge_raises_key_error_for_unknown_relation():
    """Registry.get_edge with unknown relation raises KeyError with message."""
    with pytest.raises(KeyError, match="Edge with relation .* not found"):
        Registry.get_edge("NonExistentRelation")
