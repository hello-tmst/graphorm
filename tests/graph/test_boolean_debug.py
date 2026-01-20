"""
Debug test to check if boolean False values are included in node properties.
"""


def test_debug_boolean_false_in_properties(graph):
    """Debug test to check if False is included in properties."""
    from graphorm import Node

    class Page(Node):
        __primary_key__ = ["path"]

        path: str
        parsed: bool = False

    # Create node with parsed=False
    page = Page(path="/test", parsed=False)
    
    # Check properties
    print(f"Page properties: {page.properties}")
    print(f"Page parsed value: {page.parsed}")
    print(f"Page parsed type: {type(page.parsed)}")
    print(f"Page parsed in properties: {'parsed' in page.properties}")
    if 'parsed' in page.properties:
        print(f"Page properties['parsed']: {page.properties['parsed']}")
        print(f"Page properties['parsed'] type: {type(page.properties['parsed'])}")
        print(f"Page properties['parsed'] is False: {page.properties['parsed'] is False}")
        print(f"Page properties['parsed'] is not None: {page.properties['parsed'] is not None}")
    
    # Check what __str__ generates
    node_str = str(page)
    print(f"Node __str__: {node_str}")
    
    # Check what merge() generates
    merge_str = page.merge()
    print(f"Node merge(): {merge_str}")
    
    graph.add_node(page)
    graph.flush()
    
    # Retrieve and check
    retrieved = graph.get_node(Page(path="/test"))
    if retrieved:
        print(f"Retrieved properties: {retrieved.properties}")
        print(f"Retrieved parsed: {retrieved.parsed}")
        print(f"Retrieved parsed type: {type(retrieved.parsed)}")
    else:
        print("Node not found in graph!")
