"""
Debug test to check if boolean values are saved correctly when creating nodes.
"""


def test_debug_boolean_save(graph):
    """Debug test to check if boolean values are saved."""
    from graphorm import Node

    class Page(Node):
        __primary_key__ = ["path"]

        path: str
        parsed: bool = False

    # Create node with parsed=False
    page1 = Page(path="/page1", parsed=False)
    print(f"Page1 properties: {page1.properties}")
    print(f"Page1 merge(): {page1.merge()}")
    
    # Create node with parsed=True
    page2 = Page(path="/page2", parsed=True)
    print(f"Page2 properties: {page2.properties}")
    print(f"Page2 merge(): {page2.merge()}")
    
    graph.add_node(page1)
    graph.add_node(page2)
    graph.flush()
    
    # Query all pages
    query_all = "MATCH (p:page) RETURN p"
    result_all = graph.query(query_all)
    print(f"\nAll pages after creation:")
    for node, in result_all.result_set:
        print(f"  {node.path}: parsed={node.parsed} (type: {type(node.parsed)})")
    
    # Query for false
    query_false = "MATCH (p:page) WHERE p.parsed = false RETURN p"
    result_false = graph.query(query_false)
    print(f"\nPages with parsed=false: {len(result_false.result_set)}")
    for node, in result_false.result_set:
        print(f"  {node.path}: parsed={node.parsed}")
    
    # Query for true
    query_true = "MATCH (p:page) WHERE p.parsed = true RETURN p"
    result_true = graph.query(query_true)
    print(f"\nPages with parsed=true: {len(result_true.result_set)}")
    for node, in result_true.result_set:
        print(f"  {node.path}: parsed={node.parsed}")
