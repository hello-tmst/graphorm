"""
Tests for boolean field handling and updates in graphorm.
These tests verify that boolean values are correctly formatted in Cypher queries
and that updates to boolean fields are properly persisted in the graph.
"""


def test_create_node_with_boolean_false(graph):
    """Test creating a node with boolean field set to False."""
    from graphorm import Node

    class Page(Node):
        __primary_key__ = ["path"]

        path: str
        parsed: bool = False

    page = Page(path="/test", parsed=False)
    graph.add_node(page)
    graph.flush()

    # Retrieve the node from graph
    retrieved = graph.get_node(Page(path="/test"))
    assert retrieved is not None
    assert retrieved.path == "/test"
    assert retrieved.parsed is False


def test_create_node_with_boolean_true(graph):
    """Test creating a node with boolean field set to True."""
    from graphorm import Node

    class Page(Node):
        __primary_key__ = ["path"]

        path: str
        parsed: bool = False

    page = Page(path="/test", parsed=True)
    graph.add_node(page)
    graph.flush()

    # Retrieve the node from graph
    retrieved = graph.get_node(Page(path="/test"))
    assert retrieved is not None
    assert retrieved.path == "/test"
    assert retrieved.parsed is True


def test_update_boolean_from_false_to_true(graph):
    """Test updating a boolean field from False to True."""
    from graphorm import Node

    class Page(Node):
        __primary_key__ = ["path"]

        path: str
        parsed: bool = False

    # Create node with parsed=False
    page = Page(path="/test", parsed=False)
    graph.add_node(page)
    graph.flush()

    # Verify initial state
    retrieved = graph.get_node(Page(path="/test"))
    assert retrieved is not None
    assert retrieved.parsed is False

    # Update to True
    graph.update_node(page, {"parsed": True})
    graph.flush()

    # Verify update
    retrieved = graph.get_node(Page(path="/test"))
    assert retrieved is not None
    assert retrieved.parsed is True


def test_update_boolean_from_true_to_false(graph):
    """Test updating a boolean field from True to False."""
    from graphorm import Node

    class Page(Node):
        __primary_key__ = ["path"]

        path: str
        parsed: bool = False

    # Create node with parsed=True
    page = Page(path="/test", parsed=True)
    graph.add_node(page)
    graph.flush()

    # Verify initial state
    retrieved = graph.get_node(Page(path="/test"))
    assert retrieved is not None
    assert retrieved.parsed is True

    # Update to False
    graph.update_node(page, {"parsed": False})
    graph.flush()

    # Verify update
    retrieved = graph.get_node(Page(path="/test"))
    assert retrieved is not None
    assert retrieved.parsed is False


def test_query_nodes_by_boolean_false(graph):
    """Test querying nodes where boolean field is False."""
    from graphorm import Node

    class Page(Node):
        __primary_key__ = ["path"]

        path: str
        parsed: bool = False

    # Create multiple nodes with different parsed values
    page1 = Page(path="/page1", parsed=False)
    page2 = Page(path="/page2", parsed=True)
    page3 = Page(path="/page3", parsed=False)

    graph.add_node(page1)
    graph.add_node(page2)
    graph.add_node(page3)
    graph.flush()

    # Query for unparsed pages
    query = """
    MATCH (p:Page)
    WHERE p.parsed = false
    RETURN p
    """
    result = graph.query(query)

    assert len(result.result_set) == 2
    paths = {node.path for node, in result.result_set}
    assert "/page1" in paths
    assert "/page3" in paths
    assert "/page2" not in paths


def test_query_nodes_by_boolean_true(graph):
    """Test querying nodes where boolean field is True."""
    from graphorm import Node

    class Page(Node):
        __primary_key__ = ["path"]

        path: str
        parsed: bool = False

    # Create multiple nodes with different parsed values
    page1 = Page(path="/page1", parsed=False)
    page2 = Page(path="/page2", parsed=True)
    page3 = Page(path="/page3", parsed=True)

    graph.add_node(page1)
    graph.add_node(page2)
    graph.add_node(page3)
    graph.flush()

    # Query for parsed pages
    query = """
    MATCH (p:Page)
    WHERE p.parsed = true
    RETURN p
    """
    result = graph.query(query)

    assert len(result.result_set) == 2
    paths = {node.path for node, in result.result_set}
    assert "/page2" in paths
    assert "/page3" in paths
    assert "/page1" not in paths


def test_update_boolean_multiple_times(graph):
    """Test updating boolean field multiple times."""
    from graphorm import Node

    class Page(Node):
        __primary_key__ = ["path"]

        path: str
        parsed: bool = False

    page = Page(path="/test", parsed=False)
    graph.add_node(page)
    graph.flush()

    # First update: False -> True
    graph.update_node(page, {"parsed": True})
    graph.flush()
    retrieved = graph.get_node(Page(path="/test"))
    assert retrieved.parsed is True

    # Second update: True -> False
    graph.update_node(page, {"parsed": False})
    graph.flush()
    retrieved = graph.get_node(Page(path="/test"))
    assert retrieved.parsed is False

    # Third update: False -> True
    graph.update_node(page, {"parsed": True})
    graph.flush()
    retrieved = graph.get_node(Page(path="/test"))
    assert retrieved.parsed is True


def test_node_with_multiple_boolean_fields(graph):
    """Test node with multiple boolean fields."""
    from graphorm import Node

    class Page(Node):
        __primary_key__ = ["path"]

        path: str
        parsed: bool = False
        visited: bool = False
        indexed: bool = False

    page = Page(path="/test", parsed=False, visited=False, indexed=False)
    graph.add_node(page)
    graph.flush()

    # Update all boolean fields
    graph.update_node(page, {"parsed": True, "visited": True, "indexed": True})
    graph.flush()

    retrieved = graph.get_node(Page(path="/test"))
    assert retrieved.parsed is True
    assert retrieved.visited is True
    assert retrieved.indexed is True

    # Update some fields back to False
    graph.update_node(page, {"parsed": False, "indexed": False})
    graph.flush()

    retrieved = graph.get_node(Page(path="/test"))
    assert retrieved.parsed is False
    assert retrieved.visited is True  # Should remain True
    assert retrieved.indexed is False


def test_boolean_field_in_query_after_update(graph):
    """Test that boolean field updates are reflected in subsequent queries."""
    from graphorm import Node

    class Page(Node):
        __primary_key__ = ["path"]

        path: str
        parsed: bool = False

    # Create unparsed page
    page = Page(path="/test", parsed=False)
    graph.add_node(page)
    graph.flush()

    # Query should find it as unparsed
    query_unparsed = """
    MATCH (p:Page)
    WHERE p.parsed = false AND p.path = '/test'
    RETURN p
    """
    result = graph.query(query_unparsed)
    assert len(result.result_set) == 1

    # Update to parsed
    graph.update_node(page, {"parsed": True})
    graph.flush()

    # Query should NOT find it as unparsed anymore
    result = graph.query(query_unparsed)
    assert len(result.result_set) == 0

    # Query should find it as parsed
    query_parsed = """
    MATCH (p:Page)
    WHERE p.parsed = true AND p.path = '/test'
    RETURN p
    """
    result = graph.query(query_parsed)
    assert len(result.result_set) == 1
    assert result.result_set[0][0].path == "/test"
    assert result.result_set[0][0].parsed is True


def test_update_node_without_flush_immediately_visible(graph):
    """Test that update_node changes are immediately visible without explicit flush.
    
    Note: update_node executes a direct Cypher query, so changes should be
    persisted immediately. However, we still flush to ensure consistency.
    """
    from graphorm import Node

    class Page(Node):
        __primary_key__ = ["path"]

        path: str
        parsed: bool = False

    page = Page(path="/test", parsed=False)
    graph.add_node(page)
    graph.flush()

    # Update without explicit flush (update_node executes direct query)
    graph.update_node(page, {"parsed": True})
    # Note: update_node executes a direct query, so it should be persisted
    # But we flush to be safe
    graph.flush()

    # Verify update is visible
    retrieved = graph.get_node(Page(path="/test"))
    assert retrieved is not None
    assert retrieved.parsed is True


def test_boolean_field_persistence_across_graph_operations(graph):
    """Test that boolean field values persist across multiple graph operations."""
    from graphorm import Node, Edge

    class Page(Node):
        __primary_key__ = ["path"]

        path: str
        parsed: bool = False

    class Link(Edge):
        pass

    # Create pages
    page1 = Page(path="/page1", parsed=False)
    page2 = Page(path="/page2", parsed=False)
    
    graph.add_node(page1)
    graph.add_node(page2)
    graph.flush()

    # Update page1 to parsed
    graph.update_node(page1, {"parsed": True})
    graph.flush()

    # Add edge between pages
    graph.add_edge(Link(page1, page2))
    graph.flush()

    # Verify boolean values are still correct after edge addition
    retrieved1 = graph.get_node(Page(path="/page1"))
    retrieved2 = graph.get_node(Page(path="/page2"))
    
    assert retrieved1.parsed is True
    assert retrieved2.parsed is False


def test_update_node_retrieved_from_graph(graph):
    """Test updating a node that was retrieved from the graph (not created locally).
    
    This is important because nodes retrieved from graph may have different
    aliases than locally created nodes.
    """
    from graphorm import Node

    class Page(Node):
        __primary_key__ = ["path"]

        path: str
        parsed: bool = False

    # Create and flush node
    page = Page(path="/test", parsed=False)
    graph.add_node(page)
    graph.flush()

    # Retrieve node from graph (this creates a new node instance)
    retrieved_page = graph.get_node(Page(path="/test"))
    assert retrieved_page is not None
    assert retrieved_page.parsed is False

    # Update the retrieved node
    graph.update_node(retrieved_page, {"parsed": True})
    graph.flush()

    # Verify update persisted
    updated_page = graph.get_node(Page(path="/test"))
    assert updated_page is not None
    assert updated_page.parsed is True


def test_update_node_via_query_result(graph):
    """Test updating a node that was obtained from a query result."""
    from graphorm import Node

    class Page(Node):
        __primary_key__ = ["path"]

        path: str
        parsed: bool = False

    # Create multiple pages
    page1 = Page(path="/page1", parsed=False)
    page2 = Page(path="/page2", parsed=False)
    
    graph.add_node(page1)
    graph.add_node(page2)
    graph.flush()

    # Query for pages
    query = """
    MATCH (p:Page)
    WHERE p.path = '/page1'
    RETURN p
    """
    result = graph.query(query)
    assert len(result.result_set) == 1
    
    # Get node from query result
    queried_page = result.result_set[0][0]
    assert queried_page.path == "/page1"
    assert queried_page.parsed is False

    # Update the queried node
    graph.update_node(queried_page, {"parsed": True})
    graph.flush()

    # Verify update persisted
    updated_page = graph.get_node(Page(path="/page1"))
    assert updated_page is not None
    assert updated_page.parsed is True
