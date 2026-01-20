def test_fill_graph(graph):
    from graphorm import Node, Edge

    class Page(Node):
        __primary_key__ = ["path", "parsed"]

        path: str
        parsed: bool

    class Website(Node):
        __primary_key__ = ["domain"]

        domain: str
        parsed: bool

    class Linked(Edge):
        pass

    graph.add_node(
        page_node_0 := Page(
            path="0",
            parsed=False
        )
    )

    graph.add_node(
        page_node_1 := Page(
            path="1",
            parsed=False
        )
    )

    graph.add_edge(Linked(page_node_0, page_node_1))

    graph.add_node(
        website_node_0 := Website(
            domain="0",
            parsed=False,
        )
    )

    graph.add_edge(Linked(page_node_0, website_node_0))

    graph.flush()

    assert graph.add_node(page_node_1) == 0

    result = graph.query(
        "MATCH (p:Page), (w:Website) RETURN p, w"
    )

    # for packs in result.result_set:
    #     for item in packs:
    #         logging.warning(item)
