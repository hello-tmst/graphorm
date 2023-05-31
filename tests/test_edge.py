import logging


def test_edge():
    from graphorm.edge import Edge
    from graphorm.node import Node

    class Page(Node):
        __primary_key__ = ["path", "parsed"]

        path: str
        parsed: bool

    class Linked(Edge):
        pass

    page0 = Page(path="0")
    page1 = Page(path="1")

    edge = Linked(page0, page1, _id=1)

    # logging.info(edge)
