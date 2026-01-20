def test_update_graph(graph):
    from graphorm import Node, Edge

    class Page(Node):
        __primary_key__ = ["path"]

        path: str
        parsed: bool = False

    class Linked(Edge):
        pass

    graph.add_node(
        page_node_0 := Page(
            path="0",
            parsed=False
        )
    )

    graph.flush()

    # Verify initial state
    retrieved = graph.get_node(Page(path="0"))
    assert retrieved is not None
    assert retrieved.parsed is False

    # Update boolean field
    graph.update_node(page_node_0, {"parsed": True})
    graph.flush()

    # Verify update was persisted
    retrieved = graph.get_node(Page(path="0"))
    assert retrieved is not None
    assert retrieved.parsed is True

    graph.add_node(
        page_node_1 := Page(
            path="1",
            parsed=False
        )
    )

    graph.add_edge(Linked(page_node_0, page_node_1))

    graph.flush()
