"""Tests for bulk operations using UNWIND."""


def test_bulk_upsert_simple(graph):
    """Test bulk upsert with simple primary key."""
    from graphorm import Node

    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        parsed: bool = False
        title: str = ""

    # Bulk upsert nodes
    data = [
        {"path": "/a", "parsed": True, "title": "Page A"},
        {"path": "/b", "parsed": False, "title": "Page B"},
        {"path": "/c", "parsed": True, "title": "Page C"},
    ]

    result = graph.bulk_upsert(Page, data)

    assert result is not None

    # Verify nodes were created
    for item in data:
        node = graph.get_node(Page(path=item["path"]))
        assert node is not None
        assert node.properties["path"] == item["path"]
        assert node.properties["parsed"] == item["parsed"]
        assert node.properties["title"] == item["title"]


def test_bulk_upsert_composite_key(graph):
    """Test bulk upsert with composite primary key."""
    from graphorm import Node

    class User(Node):
        __primary_key__ = ["tenant_id", "user_id"]
        tenant_id: str
        user_id: int
        name: str = ""

    # Bulk upsert nodes
    data = [
        {"tenant_id": "acme", "user_id": 1, "name": "Alice"},
        {"tenant_id": "acme", "user_id": 2, "name": "Bob"},
        {"tenant_id": "corp", "user_id": 1, "name": "Charlie"},
    ]

    result = graph.bulk_upsert(User, data)

    assert result is not None

    # Verify nodes were created
    for item in data:
        node = graph.get_node(
            User(tenant_id=item["tenant_id"], user_id=item["user_id"])
        )
        assert node is not None
        assert node.properties["name"] == item["name"]


def test_bulk_upsert_large_batch(graph):
    """Test bulk upsert with large batch."""
    from graphorm import Node

    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        parsed: bool = False

    # Create large batch
    data = [{"path": f"/page{i}", "parsed": i % 2 == 0} for i in range(100)]

    result = graph.bulk_upsert(Page, data, batch_size=50)

    assert result is not None

    # Verify some nodes were created
    node = graph.get_node(Page(path="/page0"))
    assert node is not None
    assert node.properties["parsed"] is True


def test_bulk_upsert_update_existing(graph):
    """Test that bulk upsert updates existing nodes."""
    from graphorm import Node

    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        title: str = ""

    # Create initial node
    page = Page(path="/home", title="Old Title")
    graph.add_node(page)
    graph.flush()

    # Bulk upsert with same path but different title
    data = [{"path": "/home", "title": "New Title"}]
    result = graph.bulk_upsert(Page, data)

    assert result is not None

    # Verify node was updated
    node = graph.get_node(Page(path="/home"))
    assert node is not None
    assert node.properties["title"] == "New Title"
