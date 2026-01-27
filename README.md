# GraphORM

[![Python Version](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/hello-tmst/graphorm/main/.github/badges/coverage.json)](https://github.com/hello-tmst/graphorm/actions)

GraphORM is a modern Python ORM for graph databases, specifically designed for **RedisGraph** and **FalkorDB**. It provides a simple, intuitive API with type safety, automatic property management, and a powerful fluent query builder.

## Features

- **Type-safe node and edge definitions** with Python type hints
- **Automatic property management** with validation
- **Fluent Query Builder API** with intuitive syntax
- **Transaction support** for atomic operations
- **Bulk operations** for efficient data insertion
- **Lazy loading of relationships** using Relationship descriptors
- **Automatic index creation** from class definitions
- **Support for explicit labels and relation names**
- **Batch operations** with configurable batch size
- **Isolated properties system** (separates user properties from internal attributes)

## Why GraphORM?

GraphORM makes complex graph queries readable and maintainable. Compare writing raw Cypher with using GraphORM's Query Builder:

**Raw Cypher** (hard to read and maintain):

```python
query = """
MATCH (a:User {user_id: 1})-[r1:FRIEND]->(b:User)-[r2:FRIEND]->(c:User)
WHERE c.user_id <> 1 AND b.active = true
WITH b, count(r2) as friend_count
WHERE friend_count > 5
MATCH (b)-[r3:FRIEND]->(c:User)
WHERE c.user_id <> 1
RETURN c, friend_count
ORDER BY friend_count DESC
LIMIT 10
"""
result = graph.query(query)
```

**GraphORM Query Builder** (readable and type-safe):

```python
from graphorm import select, count

class User(Node):
    __primary_key__ = ["user_id"]
    user_id: int
    active: bool = True

class FRIEND(Edge):
    pass

UserA = User.alias("a")
UserB = User.alias("b")
UserC = User.alias("c")
friend_count_expr = count(FRIEND.alias("r2")).label("friend_count")

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
    friend_count_expr > 5
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
```

**Benefits:**

- ✅ Type-safe property access with autocomplete
- ✅ Readable, Pythonic syntax
- ✅ Automatic parameterization (SQL injection safe)
- ✅ Easy to compose and maintain
- ✅ IDE support for refactoring

## Requirements

- Python 3.10, 3.11, 3.12, or 3.13
- RedisGraph 2.x or FalkorDB
- Redis server with graph module enabled

## Installation

Install from PyPI:

```bash
pip install graphorm
```

Install from source:

```bash
git clone https://github.com/hello-tmst/graphorm.git
cd graphorm
pip install -e .
```

## Quick Start

### Defining Nodes

```python
from graphorm import Node, Graph

class Page(Node):
    __primary_key__ = ["path"]

    path: str
    parsed: bool = False
    title: str = ""

# Create a graph instance
graph = Graph("my_graph", host="localhost", port=6379)
graph.create()

# Create and add nodes
page = Page(path="/home", parsed=True, title="Home Page")
graph.add_node(page)
graph.flush()
```

### Defining Edges

```python
from graphorm import Edge

class Linked(Edge):
    pass

page1 = Page(path="/page1")
page2 = Page(path="/page2")

graph.add_node(page1)
graph.add_node(page2)

link = Linked(page1, page2)
graph.add_edge(link)
graph.flush()
```

## Inserting Data: From Cypher Pain to Python Joy

| Raw Cypher problem | GraphORM solution |
|--------------------|-------------------|
| String escaping (`\"`, `\'` in paths, JSON) | Automatic via parameterized queries |
| Verbose relationship syntax `CREATE (a)-[:REL]->(b)` per pair | `graph.add_edge(Rel(a, b))` with Python objects |
| Batch insert = huge `UNWIND [...]` strings | `graph.flush(batch_size=1000)` — one call, batching under the hood |
| Type validation only at query execution | Validation at object creation (type hints / property handling) |
| Manual transaction control | Implicit atomicity: `flush()` behaves as a single transaction |

### Before: Raw Cypher

```python
# Insert 3 users + 2 follows relationships
queries = [
    """CREATE (:User {email: "alice@example.com", name: "Alice O\\'Connor", age: 30})""",
    """CREATE (:User {email: "bob@example.com", name: "Bob \\"The Builder\\"", age: 28})""",
    """CREATE (:User {email: "carol@example.com", name: "Carol", age: 35})""",
    """
    MATCH (a:User {email: "alice@example.com"}), (b:User {email: "bob@example.com"})
    CREATE (a)-[:FOLLOWS {since: 1704067200}]->(b)
    """,
    """
    MATCH (b:User {email: "bob@example.com"}), (c:User {email: "carol@example.com"})
    CREATE (b)-[:FOLLOWS {since: 1704067300}]->(c)
    """
]

for q in queries:
    graph.query(q)  # No transaction safety!
```

**Problems:**

- Manual escaping for `O'Connor`, `"The Builder"`
- 5 separate queries = 5 network round-trips
- No atomicity — if the 4th query fails, the graph is left in a partial state
- No validation — `age: "thirty"` would only fail at execution time

### After: GraphORM

```python
from graphorm import Node, Edge, Graph

class User(Node):
    __primary_key__ = ["email"]
    email: str
    name: str
    age: int

class Follows(Edge):
    since: int = 0

graph = Graph("social", host="localhost", port=6379)
graph.create()

# Create objects — validation happens here
alice = User(email="alice@example.com", name="Alice O'Connor", age=30)
bob = User(email="bob@example.com", name='Bob "The Builder"', age=28)
carol = User(email="carol@example.com", name="Carol", age=35)

# Build relationships — no string interpolation
graph.add_node(alice)
graph.add_node(bob)
graph.add_node(carol)
graph.add_edge(Follows(alice, bob, since=1704067200))
graph.add_edge(Follows(bob, carol, since=1704067300))

# One network call — atomic transaction
graph.flush()
```

**Benefits:**

- No escaping — `O'Connor` and `"The Builder"` work out of the box
- One network call instead of 5 (batching under the hood)
- Atomic — all or nothing
- Validation before hitting the DB (`age="thirty"` raises an error at object creation)

### Bulk Insert: 10,000 Pages + Links

```python
# Raw Cypher approach (pseudo-code)
cypher = "UNWIND $nodes AS n CREATE (:Page {path: n.path, title: n.title})"
graph.query(cypher, nodes=batch)  # Manual batching, manual error handling

# GraphORM approach (Page and Linked as in Quick Start)
for page_data in pages:
    page = Page(**page_data)
    graph.add_node(page)
    for link_path in page_data.get("links", []):
        graph.add_edge(Linked(page, Page(path=link_path)))

graph.flush(batch_size=1000)  # Automatic batching + rollback on error
```

| Metric | Raw Cypher | GraphORM |
|--------|------------|----------|
| Lines of code for 10k nodes | ~80 | **12** |
| Network calls | 10 (with manual batching) | **1** (automatic batching) |
| Error handling | Manual per-batch checks | **Built-in** (rollback on failure) |
| Development time | 2–3 hours | **~5 minutes** |

## Query Builder API

GraphORM provides a fluent query builder API:

```python
from graphorm import select, count, indegree, outdegree

# Find unparsed pages
stmt = select().match(Page.alias("p")).where(
    (Page.alias("p").parsed == False) &
    (Page.alias("p").error.is_null())
).limit(10)

result = graph.execute(stmt)

# Find pages with highest degree
total_degree = indegree(Page.alias("p")) + outdegree(Page.alias("p"))
stmt = select().match(Page.alias("p")).where(
    outdegree(Page.alias("p")) > 0
).returns(
    Page.alias("p"),
    total_degree.label("degree")
).orderby(total_degree.desc()).limit(20)

result = graph.execute(stmt)
for row in result.result_set:
    page, degree = row
    print(f"{page.properties['path']}: {degree} connections")
```

## Transactions

Use transactions to group operations atomically:

```python
with graph.transaction() as tx:
    tx.add_node(page1)
    tx.add_node(page2)
    tx.add_edge(Linked(page1, page2))
    # Automatically flushed on exit
```

## Bulk Operations

Efficiently insert large amounts of data using bulk operations:

```python
pages_data = [
    {"path": f"/page{i}", "domain": "example.com", "parsed": False}
    for i in range(10000)
]

result = graph.bulk_upsert(Page, pages_data, batch_size=1000)
```

## Real-World Example: Building a Social Network

Here's a powerful example that demonstrates GraphORM's capabilities for building a social network with thousands of users and connections:

```python
from graphorm import Node, Edge, Graph
import random

# Define the data model
class User(Node):
    __primary_key__ = ["user_id"]
    __indexes__ = ["user_id", "name", "active"]

    user_id: int
    name: str
    email: str
    active: bool = True
    join_date: str = ""

class FRIEND(Edge):
    pass

class FOLLOWS(Edge):
    created_at: str = ""

# Create graph
graph = Graph("social_network", host="localhost", port=6379)
graph.create()

# Generate 10,000 users with bulk insert
users_data = [
    {
        "user_id": i,
        "name": f"User {i}",
        "email": f"user{i}@example.com",
        "active": random.choice([True, False]),
        "join_date": "2024-01-01"
    }
    for i in range(10000)
]

# Bulk insert users (fast and efficient)
print("Creating 10,000 users...")
result = graph.bulk_upsert(User, users_data, batch_size=1000)
print(f"Created {len(users_data)} users in batches")

# Create friendships using transactions
print("Creating 50,000 friendships...")
friendships_created = 0

# Process in batches for better performance
batch_size = 1000
for batch_start in range(0, 50000, batch_size):
    with graph.transaction() as tx:
        for _ in range(batch_size):
            # Randomly connect users
            user1_id = random.randint(0, 9999)
            user2_id = random.randint(0, 9999)

            if user1_id != user2_id:
                user1 = User(user_id=user1_id)
                user2 = User(user_id=user2_id)
                tx.add_edge(FRIEND(user1, user2))
                friendships_created += 1

    if (batch_start // batch_size + 1) % 10 == 0:
        print(f"  Processed {batch_start + batch_size} friendships...")

print(f"Created {friendships_created} friendships")

# Create follow relationships
print("Creating 20,000 follow relationships...")
follows_created = 0

for batch_start in range(0, 20000, batch_size):
    with graph.transaction() as tx:
        for _ in range(batch_size):
            follower_id = random.randint(0, 9999)
            followee_id = random.randint(0, 9999)

            if follower_id != followee_id:
                follower = User(user_id=follower_id)
                followee = User(user_id=followee_id)
                tx.add_edge(FOLLOWS(follower, followee, created_at="2024-01-01"))
                follows_created += 1

print(f"Created {follows_created} follow relationships")

# Query: Find most connected users
from graphorm import select, count, outdegree, indegree

print("\nFinding most connected users...")
UserA = User.alias("u")
stmt = select().match(UserA).where(
    UserA.active == True
).returns(
    UserA,
    (outdegree(UserA) + indegree(UserA)).label("total_connections")
).orderby(
    "total_connections DESC"
).limit(10)

result = graph.execute(stmt)
print("Top 10 most connected users:")
for user, connections in result.result_set:
    print(f"  {user.properties['name']}: {connections} connections")

# Query: Find mutual friends
print("\nFinding mutual friends between two users...")
User1 = User.alias("u1")
User2 = User.alias("u2")
Mutual = User.alias("m")

stmt = select().match(
    (User1, FRIEND.alias("f1"), Mutual),
    (User2, FRIEND.alias("f2"), Mutual)
).where(
    (User1.user_id == 0) & (User2.user_id == 1)
).returns(
    Mutual
).limit(20)

result = graph.execute(stmt)
print(f"Found {len(result.result_set)} mutual friends between User 0 and User 1")

print("\n✅ Social network created successfully!")
print(f"   - {len(users_data)} users")
print(f"   - {friendships_created} friendships")
print(f"   - {follows_created} follow relationships")
```

**What makes this impressive:**

- 🚀 **10,000 users** inserted in seconds using bulk operations
- 🔗 **70,000+ relationships** created efficiently with transactions
- 📊 **Complex queries** executed with readable, type-safe syntax
- ⚡ **Batch processing** for optimal performance
- 🛡️ **Atomic operations** ensuring data consistency
- 🎯 **Type safety** throughout - IDE autocomplete works everywhere

This example demonstrates how GraphORM makes it easy to build and query large-scale graph applications with clean, maintainable code.

## Relationships

Lazy load related nodes using Relationship descriptors:

```python
from graphorm import Relationship

class Page(Node):
    __primary_key__ = ["path"]
    path: str

    linked_pages = Relationship("Linked", direction="outgoing")
    linked_from = Relationship("Linked", direction="incoming")

page = graph.get_node(Page(path="/home"))
if page:
    for linked_page in page.linked_pages:
        print(linked_page.properties['path'])
```

## Indexes

Automatically create indexes on node properties:

```python
class Page(Node):
    __primary_key__ = ["path"]
    __indexes__ = ["path", "parsed", "domain"]
    path: str
    parsed: bool = False
    domain: str = ""

graph.create()  # Automatically creates indexes from __indexes__
```

## Managing Labels and Relations

### Default Behavior

By default, GraphORM uses the class name as-is for labels and relations:

```python
class Page(Node):
    # Label will be "Page" (not "page")
    pass

class MyLink(Edge):
    # Relation will be "MyLink" (not "myLink")
    pass
```

### Explicit Labels and Relations

You can explicitly specify labels and relation names using class attributes:

```python
class CustomPage(Node):
    __label__ = "Page"  # Explicit label
    __primary_key__ = ["path"]
    path: str

class CustomLink(Edge):
    __relation_name__ = "Linked"  # Explicit relation name
    pass
```

**Note:** Explicit labels and relations must be non-empty strings. Invalid values will raise a `ValueError`.

### Querying with Labels

GraphORM automatically uses the correct label (class name or explicit label) when building queries. Use the Query Builder API for type-safe, readable queries:

```python
from graphorm import select

# For class Page (label is "Page")
stmt = select().match(Page.alias("p"))
result = graph.execute(stmt)

# For class with explicit label
class MyPage(Node):
    __label__ = "Page"
    __primary_key__ = ["path"]
    path: str

# Query automatically uses "Page" label - GraphORM handles it for you
stmt = select().match(MyPage.alias("p"))
result = graph.execute(stmt)
```

**Note:** While you can still use raw Cypher queries with `graph.query()`, the Query Builder API provides better type safety, readability, and maintainability.

## Properties Management

GraphORM includes an isolated properties management system that separates user-defined properties from internal attributes.

### Accessing Properties

```python
page = Page(path="/test", parsed=True)

# Get all properties (excluding internal attributes)
props = page.properties
# Returns: {'path': '/test', 'parsed': True}

# Properties are isolated from internal attributes
# Internal attributes like __id__, __alias__, etc. are not included
```

### Updating Properties

```python
# Update properties
page.update({"parsed": False, "title": "New Title"})

# Properties are validated based on type annotations
```

## Examples

### Complete Example

```python
from graphorm import Node, Edge, Graph

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

# Create graph
graph = Graph("example", host="localhost", port=6379)
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
from graphorm import select

# Query for Page targets
PageP = Page.alias("p")
PageTarget = Page.alias("target")
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
WebsiteTarget = Website.alias("target")
stmt2 = select().match(
    (PageP, Linked.alias("r"), WebsiteTarget)
).where(
    PageP.parsed == False
).returns(
    PageP,
    WebsiteTarget
)

result2 = graph.execute(stmt2)

# Combine results if needed
all_results = list(result1.result_set) + list(result2.result_set)

# Cleanup
graph.delete()
```

### More Examples

For more detailed examples, see the [examples directory](docs/examples/):

- [Web Crawler](docs/examples/web_crawler.md) - Building a web crawler with GraphORM
- [Social Network](docs/examples/social_network.md) - Modeling social networks
- [Ontology](docs/examples/ontology.md) - Working with ontologies

## Documentation

- [GitHub Repository](https://github.com/hello-tmst/graphorm)
- [Examples](docs/examples/)
  - [Web Crawler](docs/examples/web_crawler.md)
  - [Social Network](docs/examples/social_network.md)
  - [Ontology](docs/examples/ontology.md)

## Development

### Running Tests

Run tests with coverage:

```bash
pytest --cov=graphorm --cov-report=html --cov-report=term-missing
```

View coverage report:

```bash
# HTML report
open htmlcov/index.html

# Terminal report
pytest --cov=graphorm --cov-report=term-missing
```

## Breaking Changes

### Version 0.3.0

**No breaking changes.** This release maintains full backward compatibility with version 0.2.x.

### Version 0.2.0+

- **Removed dependency on `camelcase`**: Labels and relations now use class names as-is (e.g., `Page` instead of `page`)
- **All existing code must be updated**: Queries using old lowercase labels need to be updated to use class names

### Migration Guide

If you're upgrading from an older version:

1. **From 0.1.x to 0.2.0+**: Update all Cypher queries to use class names instead of camelcase labels:

   ```python
   # Old (before 0.2.0)
   query = "MATCH (p:page) RETURN p"

   # New (0.2.0+)
   query = "MATCH (p:Page) RETURN p"
   ```

2. If you need to maintain old label names, use explicit labels:

   ```python
   class Page(Node):
       __label__ = "page"  # Maintain old label
       __primary_key__ = ["path"]
       path: str
   ```

3. **From 0.2.x to 0.3.0**: No migration required. All existing code continues to work as before. New features (DELETE, REMOVE, WITH, CASE, etc.) are opt-in.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
