import logging


def test_edge():
    from graphorm.edge import CommonEdge
    from graphorm.node import CommonNode

    class Page(CommonNode):
        __primary_key__ = ["path", "parsed"]

        path: str
        parsed: bool

    class Linked(CommonEdge):
        pass

    page0 = Page(path="0")
    page1 = Page(path="1")

    edge = Linked(page0, page1)

    # logging.info(edge)
