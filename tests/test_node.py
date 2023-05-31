import logging


def test_node():
    from graphorm.node import Node

    class Page(Node):
        __primary_key__ = ["path", "parsed"]

        path: str
        parsed: bool

    class Website(Node):
        __primary_key__ = ["domain"]

        domain: str
        parsed: bool = False

    page = Page(path="123", parsed=True)

    # logging.info(page)

    website = Website(domain="google.com")

    # logging.info(website)
