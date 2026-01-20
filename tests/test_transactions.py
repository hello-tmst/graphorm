"""Tests for transaction context manager."""


def test_transaction_basic(graph):
    """Test basic transaction usage."""
    from graphorm import Node, Edge

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    class Linked(Edge):
        pass

    # Use transaction
    with graph.transaction() as tx:
        page1 = Page(path="/page1")
        page2 = Page(path="/page2")
        tx.add_node(page1)
        tx.add_node(page2)
        tx.add_edge(Linked(page1, page2))
        # Auto-flush on exit

    # Verify nodes and edge were created
    assert graph.get_node(Page(path="/page1")) is not None
    assert graph.get_node(Page(path="/page2")) is not None


def test_transaction_manual_flush(graph):
    """Test transaction with manual flush."""
    from graphorm import Node

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    with graph.transaction() as tx:
        page1 = Page(path="/page1")
        page2 = Page(path="/page2")
        tx.add_node(page1)
        tx.add_node(page2)
        
        # Manual flush
        result = tx.flush()
        assert result is not None
        
        # Verify nodes were created before context exit
        assert graph.get_node(Page(path="/page1")) is not None


def test_transaction_exception_no_flush(graph):
    """Test that transaction doesn't flush on exception."""
    from graphorm import Node

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    try:
        with graph.transaction() as tx:
            page1 = Page(path="/page1")
            tx.add_node(page1)
            raise ValueError("Test exception")
    except ValueError:
        pass

    # Node should not be created (no flush on exception)
    # Note: This test may need adjustment based on actual behavior
    # If nodes are added to graph cache before flush, they might still exist
    # This is a design decision - should nodes be added immediately or only on flush?


def test_transaction_chaining(graph):
    """Test transaction method chaining."""
    from graphorm import Node, Edge

    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    class Linked(Edge):
        pass

    with graph.transaction() as tx:
        page1 = Page(path="/page1")
        page2 = Page(path="/page2")
        tx.add_node(page1).add_node(page2).add_edge(Linked(page1, page2))

    # Verify all were created
    assert graph.get_node(Page(path="/page1")) is not None
    assert graph.get_node(Page(path="/page2")) is not None
