# GraphORM

GraphORM is a Python ORM for graph databases, specifically designed for RedisGraph/FalkorDB.

## Features

- Simple and intuitive API for working with graph databases
- Type-safe node and edge definitions
- Automatic property management with validation
- Support for explicit labels and relation names
- Batch operations support

## Installation

```bash
pip install graphorm
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

## Breaking Changes

### Version 0.2.0+

- **Removed dependency on `camelcase`**: Labels and relations now use class names as-is (e.g., `Page` instead of `page`)
- **All existing code must be updated**: Queries using old lowercase labels need to be updated to use class names

### Migration Guide

If you're upgrading from an older version:

1. Update all Cypher queries to use class names instead of camelcase labels:
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

## License

[Add your license here]
