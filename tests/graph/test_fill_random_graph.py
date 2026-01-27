def test_fill_random_graph(graph):
    from graphorm import (
        Edge,
        Node,
    )

    class TestNode(Node):
        __primary_key__ = ["code"]

        code: str

    class TestEdge(Edge):
        pass

    for i in range(1, 2):
        graph.add_node(i_node := TestNode(code=f"i_{i}"))
        for j in range(1, 11):
            graph.add_node(j_node := TestNode(code=f"j_{i}_{j}"))
            graph.add_edge(TestEdge(i_node, j_node))
            for l in range(1, 11):
                graph.add_node(l_node := TestNode(code=f"l_{i}_{j}_{l}"))
                graph.add_edge(TestEdge(j_node, l_node))
                graph.flush()
