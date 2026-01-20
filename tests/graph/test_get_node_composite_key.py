"""Tests for composite primary keys in get_node() and update_node()."""


def test_get_node_with_composite_key(graph):
    """Test that get_node() works correctly with composite primary keys."""
    from graphorm import Node

    class User(Node):
        __primary_key__ = ["tenant_id", "user_id"]
        tenant_id: str
        user_id: int
        name: str = ""

    # Create and add user
    user = User(tenant_id="acme", user_id=123, name="John Doe")
    graph.add_node(user)
    graph.flush()

    # Retrieve user by composite key
    retrieved = graph.get_node(User(tenant_id="acme", user_id=123))
    
    assert retrieved is not None
    assert retrieved.properties["tenant_id"] == "acme"
    assert retrieved.properties["user_id"] == 123
    assert retrieved.properties["name"] == "John Doe"


def test_get_node_with_composite_key_not_found(graph):
    """Test that get_node() returns None when node with composite key is not found."""
    from graphorm import Node

    class User(Node):
        __primary_key__ = ["tenant_id", "user_id"]
        tenant_id: str
        user_id: int

    # Try to retrieve non-existent user
    retrieved = graph.get_node(User(tenant_id="acme", user_id=999))
    
    assert retrieved is None


def test_update_node_with_composite_key(graph):
    """Test that update_node() works correctly with composite primary keys."""
    from graphorm import Node

    class User(Node):
        __primary_key__ = ["tenant_id", "user_id"]
        tenant_id: str
        user_id: int
        name: str = ""
        email: str = ""

    # Create and add user
    user = User(tenant_id="acme", user_id=123, name="John Doe", email="john@example.com")
    graph.add_node(user)
    graph.flush()

    # Update user properties
    graph.update_node(user, {"name": "Jane Doe", "email": "jane@example.com"})

    # Retrieve and verify update
    retrieved = graph.get_node(User(tenant_id="acme", user_id=123))
    
    assert retrieved is not None
    assert retrieved.properties["tenant_id"] == "acme"
    assert retrieved.properties["user_id"] == 123
    assert retrieved.properties["name"] == "Jane Doe"
    assert retrieved.properties["email"] == "jane@example.com"


def test_update_node_with_composite_key_partial_update(graph):
    """Test that update_node() works with partial updates on composite keys."""
    from graphorm import Node

    class User(Node):
        __primary_key__ = ["tenant_id", "user_id"]
        tenant_id: str
        user_id: int
        name: str = ""
        email: str = ""

    # Create and add user
    user = User(tenant_id="acme", user_id=123, name="John Doe", email="john@example.com")
    graph.add_node(user)
    graph.flush()

    # Update only name
    graph.update_node(user, {"name": "Jane Doe"})

    # Retrieve and verify partial update
    retrieved = graph.get_node(User(tenant_id="acme", user_id=123))
    
    assert retrieved is not None
    assert retrieved.properties["name"] == "Jane Doe"
    assert retrieved.properties["email"] == "john@example.com"  # Unchanged


def test_composite_key_cypher_generation(graph):
    """Test that Cypher generation for composite keys is correct."""
    from graphorm import Node

    class User(Node):
        __primary_key__ = ["tenant_id", "user_id"]
        tenant_id: str
        user_id: int

    user = User(tenant_id="acme", user_id=123)
    
    # Check that __str_pk__ generates correct Cypher pattern
    pk_pattern = user.__str_pk__()
    
    # Should generate something like: (alias:User{tenant_id:'acme',user_id:123})
    assert "User" in pk_pattern
    assert "tenant_id" in pk_pattern
    assert "user_id" in pk_pattern
    assert "acme" in pk_pattern or "'acme'" in pk_pattern
    assert "123" in pk_pattern
