def test_update_graph(graph):
    from graphorm import Node, Edge

    class Page(Node):
        __primary_key__ = ["path", "parsed"]

        path: str
        parsed: bool

    class Linked(Edge):
        pass

    graph.add_node(
        page_node_0 := Page(
            path="0",
            parsed=False
        )
    )

    graph.flush()

    graph.update_node(page_node_0, {"parsed": True})

    graph.add_node(
        page_node_1 := Page(
            path="1",
            parsed=False
        )
    )

    graph.add_edge(Linked(page_node_0, page_node_1))

    graph.flush()
