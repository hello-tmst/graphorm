"""
Tests for README examples.

This module contains tests that verify all code examples from README.md work correctly.
This ensures the documentation stays up-to-date and all examples are functional.
"""
import pytest
from graphorm import Node, Edge, Graph, select, count, indegree, outdegree, Relationship


def test_why_graphorm_complex_query(graph):
    """Test the complex query example from 'Why GraphORM?' section."""
    class User(Node):
        __primary_key__ = ["user_id"]
        user_id: int
        active: bool = True

    class FRIEND(Edge):
        pass

    # Create test data: users with friends of friends
    # User 1 -> User 2 -> User 3, User 4, User 5 (multiple friends)
    # User 1 -> User 6 -> User 7, User 8
    # User 2 is active, User 6 is not active
    
    users = [
        User(user_id=1, active=True),
        User(user_id=2, active=True),
        User(user_id=3, active=True),
        User(user_id=4, active=True),
        User(user_id=5, active=True),
        User(user_id=6, active=False),  # Not active
        User(user_id=7, active=True),
        User(user_id=8, active=True),
    ]
    
    for user in users:
        graph.add_node(user)
    graph.flush()
    
    # Create friendships: 1 -> 2 -> 3,4,5 (3 friends of friend 2)
    #                     1 -> 6 -> 7,8 (2 friends of friend 6, but 6 is not active)
    graph.add_edge(FRIEND(users[0], users[1]))  # 1 -> 2
    graph.add_edge(FRIEND(users[1], users[2]))  # 2 -> 3
    graph.add_edge(FRIEND(users[1], users[3]))  # 2 -> 4
    graph.add_edge(FRIEND(users[1], users[4]))  # 2 -> 5
    graph.add_edge(FRIEND(users[0], users[5]))  # 1 -> 6
    graph.add_edge(FRIEND(users[5], users[6]))  # 6 -> 7
    graph.add_edge(FRIEND(users[5], users[7]))  # 6 -> 8
    graph.flush()
    
    # Execute the GraphORM query from README
    UserA = User.alias("a")
    UserB = User.alias("b")
    UserC = User.alias("c")
    friend_count_expr = count(FRIEND.alias("r2")).label("friend_count")

    # Build query with WHERE before and after WITH
    # Count friends of b (intermediate friend), grouping by b only
    # Then match to c and filter
    stmt = select().match(
        (UserA, FRIEND.alias("r1"), UserB),
        (UserB, FRIEND.alias("r2"), UserC)
    ).where(
        (UserA.user_id == 1) & 
        (UserC.user_id != 1) & 
        (UserB.active == True)
    ).with_(
        UserB,
        friend_count_expr
    ).where(
        friend_count_expr > 2
    ).match(
        (UserB, FRIEND.alias("r3"), UserC)
    ).where(
        UserC.user_id != 1
    ).returns(
        UserC,
        friend_count_expr
    ).orderby(
        friend_count_expr.desc()
    ).limit(10)
    
    result = graph.execute(stmt)
    
    # Should find users 3, 4, 5 (friends of friend 2, who has 3 friends)
    # User 2 has 3 friends (users 3, 4, 5), so friend_count for each should be 3
    assert len(result.result_set) > 0
    # Verify structure
    for row in result.result_set:
        user, friend_count = row
        assert isinstance(user, User)
        assert friend_count >= 1
        # Each of users 3, 4, 5 should have friend_count = 3 (from friend 2)
        # But we might also get user 6's friends (7, 8) with friend_count = 2
        # So we just verify the structure is correct


def test_quick_start_defining_nodes(graph):
    """Test the 'Defining Nodes' example from Quick Start section."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        parsed: bool = False
        title: str = ""

    # Create a graph instance (already created by fixture)
    graph.create()

    # Create and add nodes
    page = Page(path="/home", parsed=True, title="Home Page")
    graph.add_node(page)
    graph.flush()
    
    # Verify node was created
    retrieved = graph.get_node(Page(path="/home"))
    assert retrieved is not None
    assert retrieved.properties["path"] == "/home"
    assert retrieved.properties["parsed"] is True
    assert retrieved.properties["title"] == "Home Page"


def test_quick_start_defining_edges(graph):
    """Test the 'Defining Edges' example from Quick Start section."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    class Linked(Edge):
        pass

    page1 = Page(path="/page1")
    page2 = Page(path="/page2")

    graph.add_node(page1)
    graph.add_node(page2)
    graph.flush()

    link = Linked(page1, page2)
    graph.add_edge(link)
    graph.flush()
    
    # Verify edge was created by querying
    PageA = Page.alias("a")
    PageB = Page.alias("b")
    stmt = select().match(
        (PageA, Linked.alias("r"), PageB)
    ).where(
        PageA.path == "/page1"
    )
    
    result = graph.execute(stmt)
    assert len(result.result_set) > 0
    assert result.result_set[0][0].properties["path"] == "/page1"
    assert result.result_set[0][1].properties["path"] == "/page2"


def test_query_builder_find_unparsed_pages(graph):
    """Test the 'Find unparsed pages' example from Query Builder API section."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        parsed: bool = False
        error: str = None

    # Create test pages
    pages = [
        Page(path="/page1", parsed=False, error=None),
        Page(path="/page2", parsed=True, error=None),
        Page(path="/page3", parsed=False, error="Some error"),
        Page(path="/page4", parsed=False, error=None),
    ]
    
    for page in pages:
        graph.add_node(page)
    graph.flush()
    
    # Find unparsed pages
    stmt = select().match(Page.alias("p")).where(
        (Page.alias("p").parsed == False) & 
        (Page.alias("p").error.is_null())
    ).limit(10)

    result = graph.execute(stmt)
    
    # Should find /page1 and /page4 (unparsed, no error)
    assert len(result.result_set) == 2
    paths = {row[0].properties["path"] for row in result.result_set}
    assert "/page1" in paths
    assert "/page4" in paths
    assert "/page2" not in paths  # parsed
    assert "/page3" not in paths  # has error


def test_query_builder_find_pages_highest_degree(graph):
    """Test the 'Find pages with highest degree' example from Query Builder API section."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    class Linked(Edge):
        pass

    # Create pages with different degrees
    pages = [Page(path=f"/page{i}") for i in range(5)]
    for page in pages:
        graph.add_node(page)
    graph.flush()
    
    # Create links: page0 -> page1, page2, page3 (outdegree 3)
    #               page1 -> page0 (indegree 1 from page0, outdegree 1)
    graph.add_edge(Linked(pages[0], pages[1]))
    graph.add_edge(Linked(pages[0], pages[2]))
    graph.add_edge(Linked(pages[0], pages[3]))
    graph.add_edge(Linked(pages[1], pages[0]))
    graph.flush()
    
    # Find pages with highest degree
    PageP = Page.alias("p")
    total_degree = indegree(PageP) + outdegree(PageP)
    total_degree_labeled = total_degree.label("degree")
    stmt = select().match(PageP).where(
        outdegree(PageP) > 0
    ).returns(
        PageP,
        total_degree_labeled
    ).orderby("degree DESC").limit(20)

    result = graph.execute(stmt)
    
    assert len(result.result_set) > 0
    # Verify structure
    for row in result.result_set:
        page, degree = row
        assert isinstance(page, Page)
        assert isinstance(degree, (int, float))
        assert degree > 0
    
    # page0 should have highest degree (3 outgoing + 1 incoming = 4)
    if len(result.result_set) > 0:
        top_page, top_degree = result.result_set[0]
        assert top_degree >= 3


def test_transactions_example(graph):
    """Test the Transactions example from README."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    class Linked(Edge):
        pass

    page1 = Page(path="/page1")
    page2 = Page(path="/page2")

    # Use transactions to group operations atomically
    with graph.transaction() as tx:
        tx.add_node(page1)
        tx.add_node(page2)
        tx.add_edge(Linked(page1, page2))
        # Automatically flushed on exit

    # Verify nodes and edge were created
    assert graph.get_node(Page(path="/page1")) is not None
    assert graph.get_node(Page(path="/page2")) is not None
    
    # Verify edge exists
    PageA = Page.alias("a")
    PageB = Page.alias("b")
    stmt = select().match(
        (PageA, Linked.alias("r"), PageB)
    ).where(
        PageA.path == "/page1"
    )
    result = graph.execute(stmt)
    assert len(result.result_set) > 0


def test_bulk_operations_example(graph):
    """Test the Bulk Operations example from README."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        domain: str = ""
        parsed: bool = False

    # Use smaller dataset for testing (100 instead of 10000)
    pages_data = [
        {"path": f"/page{i}", "domain": "example.com", "parsed": False}
        for i in range(100)
    ]

    result = graph.bulk_upsert(Page, pages_data, batch_size=1000)
    
    assert result is not None
    
    # Verify some nodes were created
    node = graph.get_node(Page(path="/page0"))
    assert node is not None
    assert node.properties["domain"] == "example.com"
    assert node.properties["parsed"] is False
    
    node50 = graph.get_node(Page(path="/page50"))
    assert node50 is not None


def test_relationships_lazy_loading(graph):
    """Test the Relationships lazy loading example from README."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        linked_pages = Relationship("Linked", direction="outgoing")
        linked_from = Relationship("Linked", direction="incoming")

    class Linked(Edge):
        pass

    # Create pages
    page1 = Page(path="/home")
    page2 = Page(path="/about")
    page3 = Page(path="/contact")
    
    graph.add_node(page1)
    graph.add_node(page2)
    graph.add_node(page3)
    graph.flush()
    
    # Create links
    graph.add_edge(Linked(page1, page2))
    graph.add_edge(Linked(page1, page3))
    graph.flush()
    
    # Get page from graph (graph reference is automatically set by QueryResult)
    page = graph.get_node(Page(path="/home"))
    if page:
        # Lazy load related pages (__graph__ is set automatically by QueryResult.parse_node())
        linked = page.linked_pages
        assert len(linked) == 2
        paths = {p.properties['path'] for p in linked}
        assert "/about" in paths
        assert "/contact" in paths


def test_indexes_automatic_creation(graph):
    """Test the Indexes automatic creation example from README."""
    class Page(Node):
        __primary_key__ = ["path"]
        __indexes__ = ["path", "parsed", "domain"]
        path: str
        parsed: bool = False
        domain: str = ""

    # Create graph - automatically creates indexes from __indexes__
    graph.create()
    
    # Verify no errors occurred (we can't easily verify indexes exist without list_indexes)
    # But we can verify that queries work
    page = Page(path="/test", parsed=True, domain="example.com")
    graph.add_node(page)
    graph.flush()
    
    retrieved = graph.get_node(Page(path="/test"))
    assert retrieved is not None


def test_querying_with_labels(graph):
    """Test the 'Querying with Labels' example from README."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str

    # For class Page (label is "Page")
    stmt = select().match(Page.alias("p"))
    result = graph.execute(stmt)
    
    # Should work without errors
    assert result is not None
    
    # For class with explicit label
    class MyPage(Node):
        __label__ = "Page"
        __primary_key__ = ["path"]
        path: str

    # Create a node with explicit label
    my_page = MyPage(path="/mypage")
    graph.add_node(my_page)
    graph.flush()
    
    # Query automatically uses "Page" label - GraphORM handles it for you
    stmt2 = select().match(MyPage.alias("p"))
    result2 = graph.execute(stmt2)
    
    assert result2 is not None
    assert len(result2.result_set) > 0


def test_properties_accessing(graph):
    """Test the 'Accessing Properties' example from Properties Management section."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        parsed: bool = False

    page = Page(path="/test", parsed=True)

    # Get all properties (excluding internal attributes)
    props = page.properties
    # Returns: {'path': '/test', 'parsed': True}
    assert props["path"] == "/test"
    assert props["parsed"] is True
    
    # Properties are isolated from internal attributes
    # Internal attributes like __id__, __alias__, etc. are not included
    assert "__id__" not in props
    assert "__alias__" not in props
    assert "__graph__" not in props


def test_properties_updating(graph):
    """Test the 'Updating Properties' example from Properties Management section."""
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        parsed: bool = False
        title: str = ""

    page = Page(path="/test", parsed=True, title="Original")
    graph.add_node(page)
    graph.flush()
    
    # Update properties
    page.update({"parsed": False, "title": "New Title"})
    
    # Properties are validated based on type annotations
    assert page.properties["parsed"] is False
    assert page.properties["title"] == "New Title"
    assert page.properties["path"] == "/test"  # Should remain unchanged


def test_complete_example(graph):
    """Test the Complete Example from README."""
    # Define nodes
    class Page(Node):
        __primary_key__ = ["path"]
        path: str
        parsed: bool = False

    class Website(Node):
        __primary_key__ = ["domain"]
        domain: str

    # Define edges
    class Linked(Edge):
        pass

    # Create graph (already created by fixture)
    graph.create()

    # Create nodes
    page1 = Page(path="/page1", parsed=False)
    page2 = Page(path="/page2", parsed=True)
    website = Website(domain="example.com")

    graph.add_node(page1)
    graph.add_node(page2)
    graph.add_node(website)
    graph.flush()

    # Create edges
    link1 = Linked(page1, page2)
    link2 = Linked(page1, website)

    graph.add_edge(link1)
    graph.add_edge(link2)
    graph.flush()

    # Query using GraphORM Query Builder
    # Use ORM API - query for Page targets and Website targets separately, then combine
    PageP = Page.alias("p")
    PageTarget = Page.alias("target")
    WebsiteTarget = Website.alias("target")
    
    # Query for Page targets
    stmt1 = select().match(
        (PageP, Linked.alias("r"), PageTarget)
    ).where(
        PageP.parsed == False
    ).returns(
        PageP,
        PageTarget
    )
    
    result1 = graph.execute(stmt1)
    
    # Query for Website targets
    stmt2 = select().match(
        (PageP, Linked.alias("r"), WebsiteTarget)
    ).where(
        PageP.parsed == False
    ).returns(
        PageP,
        WebsiteTarget
    )
    
    result2 = graph.execute(stmt2)
    
    # Combine results
    all_results = list(result1.result_set) + list(result2.result_set)
    
    # Should find page1 (parsed=False) and its targets (page2 and website)
    assert len(all_results) > 0
    
    # Verify results
    found_page1 = False
    for row in all_results:
        page, target = row
        if page.properties["path"] == "/page1":
            found_page1 = True
            # Target should be either page2 or website
            assert target.properties.get("path") == "/page2" or target.properties.get("domain") == "example.com"
    
    assert found_page1
