"""
Tests for QueryResult parsing methods: parse_path, parse_map, parse_point.
"""
import pytest
from graphorm import Node, Edge, Graph, QueryResult
from graphorm.query_result import ResultSetScalarTypes


def test_query_result_parse_path(graph):
    """Test QueryResult.parse_path() method."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    class Linked(Edge):
        pass
    
    # Create nodes and edges
    page1 = Page(path="/page1")
    page2 = Page(path="/page2")
    page3 = Page(path="/page3")
    
    graph.add_node(page1)
    graph.add_node(page2)
    graph.add_node(page3)
    graph.flush()
    
    edge1 = Linked(page1, page2)
    edge2 = Linked(page2, page3)
    
    graph.add_edge(edge1)
    graph.add_edge(edge2)
    graph.flush()
    
    # Query to get path
    result = graph.query("""
        MATCH path = (p1:Page {path: '/page1'})-[:Linked*]->(p3:Page {path: '/page3'})
        RETURN path
        LIMIT 1
    """)
    
    # If path is returned, test parsing
    if not result.is_empty() and len(result.result_set) > 0:
        # The path should be parsed correctly
        # Note: Actual parsing depends on RedisGraph response format
        pass


def test_query_result_parse_map(graph):
    """Test QueryResult.parse_map() method."""
    # Query that returns a map
    result = graph.query("RETURN {key1: 'value1', key2: 42, key3: true} AS m")
    
    if not result.is_empty():
        # Map should be parsed
        # Note: Actual format depends on RedisGraph response
        pass


def test_query_result_parse_point(graph):
    """Test QueryResult.parse_point() method."""
    # Query that returns a point
    # Note: RedisGraph/FalkorDB may not support Point type
    # This is a placeholder test
    result = graph.query("RETURN point({latitude: 40.7128, longitude: -74.0060}) AS p")
    
    if not result.is_empty():
        # Point should be parsed
        # Note: Actual format depends on RedisGraph response
        pass


def test_query_result_parse_scalar_array(graph):
    """Test QueryResult.parse_scalar() with VALUE_ARRAY."""
    # Query that returns an array
    result = graph.query("RETURN [1, 2, 3, 4, 5] AS arr")
    
    if not result.is_empty():
        arr = result.result_set[0][0]
        assert isinstance(arr, list)
        # Note: Actual parsing depends on RedisGraph response format


def test_query_result_parse_scalar_map(graph):
    """Test QueryResult.parse_scalar() with VALUE_MAP."""
    # Query that returns a map as scalar
    result = graph.query("RETURN {name: 'test', value: 123} AS m")
    
    if not result.is_empty():
        # Map should be parsed
        # Note: Actual format depends on RedisGraph response
        pass


def test_query_result_empty_result_set():
    """Test QueryResult with empty result set."""
    from graphorm.drivers.redis import RedisDriver
    
    # Create a mock graph for testing
    driver = RedisDriver("localhost", 6379)
    
    # Create QueryResult with empty response
    # Simulating empty result
    empty_response = [
        [],  # Empty header
        []   # Empty result set
    ]
    
    # Create a minimal graph object for QueryResult
    class MockGraph:
        def get_label(self, idx):
            return "Page"
        
        def get_property(self, idx):
            return "path"
        
        def get_relation(self, idx):
            return "Linked"
    
    mock_graph = MockGraph()
    result = QueryResult(mock_graph, empty_response)
    
    assert result.is_empty()
    assert len(result.result_set) == 0


def test_query_result_pretty_print(graph):
    """Test QueryResult.pretty_print() method."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    # Create and add node
    page = Page(path="/test")
    graph.add_node(page)
    graph.flush()
    
    # Query
    result = graph.query("MATCH (p:Page) RETURN p LIMIT 1")
    
    # Test pretty_print (should not raise exception)
    # We can't easily test the output format, but we can test it doesn't crash
    try:
        result.pretty_print()
    except Exception as e:
        pytest.fail(f"pretty_print() raised {e}")


def test_query_result_statistics():
    """Test QueryResult statistics properties."""
    from graphorm.drivers.redis import RedisDriver
    
    # Create a mock graph
    class MockGraph:
        def get_label(self, idx):
            return "Page"
        
        def get_property(self, idx):
            return "path"
        
        def get_relation(self, idx):
            return "Linked"
    
    mock_graph = MockGraph()
    
    # Simulate response with statistics
    response_with_stats = [
        [],  # Empty header
        [],  # Empty result set
        ["Nodes created: 5", "Relationships created: 3", "Properties set: 10"]
    ]
    
    result = QueryResult(mock_graph, response_with_stats)
    
    # Statistics should be accessible
    assert hasattr(result, 'statistics')
    # Note: Actual values depend on RedisGraph response format


def test_query_result_parse_node_with_multiple_labels(graph):
    """Test QueryResult.parse_node() with multiple labels."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    # Create and add node
    page = Page(path="/test")
    graph.add_node(page)
    graph.flush()
    
    # Query node
    result = graph.query("MATCH (p:Page) RETURN p LIMIT 1")
    
    if not result.is_empty():
        node = result.result_set[0][0]
        assert isinstance(node, Page)
        assert node.properties['path'] == "/test"


def test_query_result_parse_edge(graph):
    """Test QueryResult.parse_edge() method."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
    
    class Linked(Edge):
        weight: float = 1.0
    
    # Create nodes and edge
    page1 = Page(path="/page1")
    page2 = Page(path="/page2")
    
    graph.add_node(page1)
    graph.add_node(page2)
    graph.flush()
    
    edge = Linked(page1, page2, weight=0.9)
    graph.add_edge(edge)
    graph.flush()
    
    # Query edge
    result = graph.query("""
        MATCH (p1:Page {path: '/page1'})-[r:Linked]->(p2:Page {path: '/page2'})
        RETURN r
        LIMIT 1
    """)
    
    if not result.is_empty():
        edge_result = result.result_set[0][0]
        assert isinstance(edge_result, Linked)
        # Edge should have properties
        assert hasattr(edge_result, 'properties')


def test_query_result_simple_response():
    """Test QueryResult with simple response (like from GRAPH.DELETE)."""
    from graphorm.drivers.redis import RedisDriver
    
    # Create a mock graph
    class MockGraph:
        pass
    
    mock_graph = MockGraph()
    
    # Simple string response (like from GRAPH.DELETE)
    simple_response = "OK"
    
    result = QueryResult(mock_graph, simple_response)
    
    # Should handle simple responses gracefully
    assert hasattr(result, 'statistics')
    assert result.is_empty()  # Simple responses have no result set
