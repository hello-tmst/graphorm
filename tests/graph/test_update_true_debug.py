"""
Debug test to check why True values are not updated.
"""


def test_debug_update_true(graph):
    """Debug test to check True update."""
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
    print(f"Initial state - parsed: {retrieved.parsed}, type: {type(retrieved.parsed)}")
    assert retrieved is not None
    assert retrieved.parsed is False

    # Check what update_node will generate
    from graphorm.utils import format_cypher_value
    print(f"format_cypher_value(True): {format_cypher_value(True)}")
    print(f"format_cypher_value(False): {format_cypher_value(False)}")
    
    # Check node properties and primary key
    print(f"Node properties: {page.properties}")
    print(f"Node __str_pk__(): {page.__str_pk__()}")
    print(f"Retrieved properties: {retrieved.properties}")
    print(f"Retrieved __str_pk__(): {retrieved.__str_pk__()}")
    
    # Try to update with True
    print("\nUpdating with True...")
    # Check what query will be generated
    from graphorm.utils import format_cypher_value
    pk_pattern = retrieved.__str_pk__()
    set_clause = f"{retrieved.alias}.parsed={format_cypher_value(True)}"
    query = f"MATCH {pk_pattern} SET {set_clause} RETURN {retrieved.alias}"
    print(f"Generated query: {query}")
    
    graph.update_node(retrieved, {"parsed": True})
    
    # Check result
    result = graph.query(f"MATCH {retrieved.__str_pk__()} RETURN {retrieved.alias}")
    if len(result.result_set) > 0:
        updated_node = result.result_set[0][0]
        print(f"Node after update query - parsed: {updated_node.parsed}")
    else:
        print("Node not found after update!")
    
    graph.flush()

    # Verify update
    updated = graph.get_node(Page(path="/test"))
    print(f"After update - parsed: {updated.parsed}, type: {type(updated.parsed)}")
    assert updated is not None
    assert updated.parsed is True, f"Expected True, got {updated.parsed} (type: {type(updated.parsed)})"
