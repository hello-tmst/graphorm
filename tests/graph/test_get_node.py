import logging


def test_get_node(graph):
    from graphorm import Node

    class TestNode(Node):
        __primary_key__ = ["code"]

        code: str

    graph.add_node(node := TestNode(code="test"))

    graph.flush()

    node = graph.get_node(node)

    logging.info(node)