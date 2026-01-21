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

When writing Cypher queries, use the actual label name (class name or explicit label):

```python
# For class Page (label is "Page")
result = graph.query("MATCH (p:Page) RETURN p")

# For class with explicit label
class MyPage(Node):
    __label__ = "Page"
    __primary_key__ = ["path"]
    path: str

# Query still uses "Page"
result = graph.query("MATCH (p:Page) RETURN p")
```

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

# Query
result = graph.query("""
    MATCH (p:Page)-[:Linked]->(target)
    WHERE p.parsed = false
    RETURN p, target
""")

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
