# Web Crawler Use Case

Этот пример демонстрирует использование GraphORM для построения веб-краулера, который отслеживает страницы и связи между ними.

## Модели данных

```python
from graphorm import Node, Edge, Graph, select, Relationship

class Page(Node):
    __primary_key__ = ["path"]
    __indexes__ = ["path", "parsed", "domain"]
    
    path: str
    domain: str = ""
    parsed: bool = False
    title: str = ""
    error: str = None
    
    # Ленивая загрузка связанных страниц
    linked_pages = Relationship("Linked", direction="outgoing")
    linked_from = Relationship("Linked", direction="incoming")

class Linked(Edge):
    weight: float = 1.0
    discovered_at: str = ""
```

## Инициализация графа

```python
graph = Graph("web_crawler", host="localhost", port=6379)
graph.create()  # Автоматически создаст индексы из __indexes__
```

## Добавление страниц

```python
# Создание страниц
page1 = Page(path="/home", domain="example.com", title="Home Page")
page2 = Page(path="/about", domain="example.com", title="About Page")
page3 = Page(path="/contact", domain="example.com", title="Contact Page")

# Использование транзакции для группировки операций
with graph.transaction() as tx:
    tx.add_node(page1)
    tx.add_node(page2)
    tx.add_node(page3)
    tx.add_edge(Linked(page1, page2, weight=0.9))
    tx.add_edge(Linked(page1, page3, weight=0.8))
    tx.add_edge(Linked(page2, page3, weight=0.7))
    # Автоматический flush при выходе
```

## Поиск непроиндексированных страниц

```python
from graphorm import select, indegree, outdegree

# Найти все непроиндексированные страницы без ошибок
stmt = select().match(Page.alias("p")).where(
    (Page.alias("p").parsed == False) & 
    (Page.alias("p").error.is_null())
).limit(10)

result = graph.execute(stmt)

for row in result.result_set:
    page = row[0]
    print(f"Need to parse: {page.properties['path']}")
```

## Поиск страниц с наибольшей степенью

```python
# Найти страницы с наибольшим количеством связей
total_degree = indegree(Page.alias("p")) + outdegree(Page.alias("p"))

stmt = select().match(Page.alias("p")).where(
    outdegree(Page.alias("p")) > 0
).returns(
    Page.alias("p"),
    total_degree.label("degree")
).orderby(
    total_degree.desc()
).limit(20)

result = graph.execute(stmt)

for row in result.result_set:
    page = row[0]
    degree = row[1]
    print(f"{page.properties['path']}: {degree} connections")
```

## Навигация по связям

```python
# Найти все страницы, связанные с конкретной страницей
page = graph.get_node(Page(path="/home"))

if page:
    # Ленивая загрузка связанных страниц
    linked = page.linked_pages
    print(f"Page {page.properties['path']} links to:")
    for linked_page in linked:
        print(f"  - {linked_page.properties['path']}")
```

## Поиск путей между страницами

```python
from graphorm import select

# Найти путь от одной страницы к другой
PageA = Page.alias("a")
PageB = Page.alias("b")

stmt = select().match(
    (PageA, Linked.alias("r"), PageB)
).where(
    PageA.path == "/home"
).returns(
    PageA,
    Linked.alias("r"),
    PageB
)

result = graph.execute(stmt)

for row in result.result_set:
    src, edge, dst = row
    print(f"{src.properties['path']} -> {dst.properties['path']} (weight: {edge.properties.get('weight', 1.0)})")
```

## Bulk-операции для массовой вставки

```python
# Массовая вставка тысяч страниц
pages_data = [
    {"path": f"/page{i}", "domain": "example.com", "parsed": False, "title": f"Page {i}"}
    for i in range(10000)
]

result = graph.bulk_upsert(Page, pages_data, batch_size=1000)
print(f"Inserted {len(pages_data)} pages")
```

## Обновление страниц

```python
# Обновить статус парсинга
page = graph.get_node(Page(path="/home"))
if page:
    graph.update_node(page, {"parsed": True, "title": "Updated Home Page"})
```

## Статистика графа

```python
# Подсчитать общее количество страниц
from graphorm import count

stmt = select().match(Page.alias("p")).returns(count(Page.alias("p")))
result = graph.execute(stmt)
total_pages = result.result_set[0][0]
print(f"Total pages: {total_pages}")

# Подсчитать проиндексированные страницы
stmt = select().match(Page.alias("p")).where(
    Page.alias("p").parsed == True
).returns(count(Page.alias("p")))
result = graph.execute(stmt)
parsed_pages = result.result_set[0][0]
print(f"Parsed pages: {parsed_pages}")
```

## Полный пример

```python
from graphorm import Node, Edge, Graph, select, Relationship, indegree, outdegree

# Определение моделей
class Page(Node):
    __primary_key__ = ["path"]
    __indexes__ = ["path", "parsed"]
    path: str
    domain: str = ""
    parsed: bool = False
    title: str = ""
    linked_pages = Relationship("Linked", direction="outgoing")

class Linked(Edge):
    weight: float = 1.0

# Создание графа
graph = Graph("web_crawler", host="localhost", port=6379)
graph.create()

# Создание страниц и связей
with graph.transaction() as tx:
    pages = [
        Page(path="/", domain="example.com", title="Home"),
        Page(path="/about", domain="example.com", title="About"),
        Page(path="/contact", domain="example.com", title="Contact"),
    ]
    
    for page in pages:
        tx.add_node(page)
    
    tx.add_edge(Linked(pages[0], pages[1], weight=0.9))
    tx.add_edge(Linked(pages[0], pages[2], weight=0.8))
    tx.add_edge(Linked(pages[1], pages[2], weight=0.7))

# Поиск корневых страниц (с наибольшей степенью)
total_degree = indegree(Page.alias("p")) + outdegree(Page.alias("p"))
stmt = select().match(Page.alias("p")).where(
    outdegree(Page.alias("p")) > 0
).returns(
    Page.alias("p"),
    total_degree.label("degree")
).orderby(
    total_degree.desc()
).limit(10)

result = graph.execute(stmt)
for row in result.result_set:
    page, degree = row
    print(f"{page.properties['path']}: degree={degree}")

# Очистка
graph.delete()
```
