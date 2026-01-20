# GraphORM - Полное описание ORM системы

## Обзор

**GraphORM** — это Python ORM для работы с графовыми базами данных, специально разработанная для **RedisGraph/FalkorDB**. Система предоставляет объектно-ориентированный интерфейс для работы с узлами (nodes), рёбрами (edges) и запросами Cypher, аналогичный SQLAlchemy 2.0.

**Версия:** 0.1.11  
**Python:** >= 3.10  
**Зависимости:** redis (>=5.0.1), prettytable (>=3.8.0)

---

## Архитектура системы

### Основные компоненты

1. **Модели данных:**
   - `Node` — базовый класс для узлов графа
   - `Edge` — базовый класс для рёбер графа
   - `Common` — общий базовый класс с управлением свойствами

2. **Query Builder API:**
   - `Select` — построитель запросов Cypher
   - `Property` — дескриптор свойств для построения условий
   - `Expression` — классы выражений (BinaryExpression, ArithmeticExpression, Function)

3. **Драйверы:**
   - `RedisDriver` — драйвер для RedisGraph/FalkorDB
   - `Driver` — базовый интерфейс драйвера

4. **Вспомогательные компоненты:**
   - `Graph` — основной класс для работы с графом
   - `QueryResult` — результат выполнения запроса
   - `Registry` — реестр классов узлов и рёбер
   - `Path` — представление пути в графе

---

## Модели данных

### Node (Узел)

**Базовый класс:** `Node(Common)`

#### Определение узла

```python
from graphorm import Node

class Page(Node):
    __primary_key__ = ["path"]  # Первичный ключ (строка или список)
    __label__ = "Page"  # Опционально: явная метка (по умолчанию = имя класса)
    
    # Аннотированные свойства автоматически становятся Property дескрипторами
    path: str
    parsed: bool = False
    title: str = ""
    error: str = None
```

#### Особенности:

- **Автоматическое создание Property дескрипторов:** Все аннотированные свойства автоматически становятся `Property` объектами при доступе как атрибуты класса
- **Первичный ключ:** Может быть строкой (одно поле) или списком (составной ключ)
- **Метки:** По умолчанию используется имя класса, можно задать явно через `__label__`
- **Алиасы:** Каждый узел имеет уникальный алиас для запросов

#### Методы:

- `merge()` — генерирует MERGE запрос для узла
- `set_alias(alias: str)` — устанавливает алиас
- `update(properties: dict)` — обновляет свойства

### Edge (Ребро)

**Базовый класс:** `Edge(Common)`

#### Определение ребра

```python
from graphorm import Edge, Node

class Linked(Edge):
    __relation_name__ = "Linked"  # Опционально: явное имя отношения
    
    # Свойства ребра
    weight: float = 1.0
    created_at: str = None
```

#### Особенности:

- **Связь узлов:** Ребро всегда связывает два узла (`src_node`, `dst_node`)
- **Имя отношения:** По умолчанию = имя класса, можно задать через `__relation_name__`
- **Автоматическое создание Property дескрипторов:** Аналогично Node

#### Создание ребра:

```python
page1 = Page(path="/page1")
page2 = Page(path="/page2")

link = Linked(page1, page2, weight=0.8)
```

---

## Query Builder API (SQLAlchemy 2.0 стиль)

### Основные концепции

Query Builder позволяет строить запросы Cypher объектно-ориентированным способом, без написания сырых строк Cypher.

### Select Statement

#### Базовое использование

```python
from graphorm import select, Graph

graph = Graph("my_graph", host="localhost", port=6379)

# Простой запрос
stmt = select(Page)
result = graph.execute(stmt)
# Генерирует: MATCH (page:Page) RETURN page
```

#### WHERE условия

```python
# Простое условие
stmt = select(Page).where(Page.path == "/home")
# Генерирует: MATCH (page:Page) WHERE page.path = $param_0 RETURN page

# Множественные условия (AND)
stmt = select(Page).where(
    (Page.parsed == False) & (Page.error.is_null())
)
# Генерирует: MATCH (page:Page) WHERE (page.parsed = $param_0 AND page.error IS NULL) RETURN page

# OR условия
stmt = select(Page).where(
    (Page.path == "/home") | (Page.path == "/about")
)
# Генерирует: MATCH (page:Page) WHERE (page.path = $param_0 OR page.path = $param_1) RETURN page
```

#### Операторы Property

```python
# Сравнение
Page.path == "/home"      # Равенство
Page.path != "/home"      # Неравенство (генерирует <>)
Page.path < "/z"          # Меньше
Page.path <= "/z"         # Меньше или равно
Page.path > "/a"          # Больше
Page.path >= "/a"         # Больше или равно

# IN / NOT IN
Page.path.in_(["/home", "/about", "/contact"])

# Строковые операторы
Page.path.contains("home")        # CONTAINS
Page.path.starts_with("/")        # STARTS WITH
Page.path.ends_with(".html")      # ENDS WITH
Page.path.like(".*home.*")        # =~ (regex, может не поддерживаться в RedisGraph)

# NULL проверки
Page.error.is_null()              # IS NULL
Page.error.is_not_null()          # IS NOT NULL

# Сортировка
Page.path.asc()                   # ORDER BY page.path ASC
Page.path.desc()                  # ORDER BY page.path DESC
```

#### RETURN, ORDER BY, LIMIT, SKIP

```python
# Явный RETURN
stmt = select(Page).returns(Page, Page.path, Page.title)

# ORDER BY
stmt = select(Page).orderby(Page.path.asc())

# LIMIT и SKIP
stmt = select(Page).limit(10).skip(5)
# Генерирует: MATCH (page:Page) RETURN page SKIP 5 LIMIT 10

# Комбинированный запрос
stmt = select(Page).where(
    Page.parsed == False
).returns(
    Page,
    Page.path,
    Page.title
).orderby(
    Page.path.asc()
).limit(20)
```

#### Алиасы (Aliases)

```python
from graphorm import aliased

# Создание алиаса для класса
PageAlias = aliased(Page, "p")
stmt = select(PageAlias).where(PageAlias.path == "/home")

# Или с автоматическим именем (lowercase)
PageAlias = aliased(Page)  # Алиас = "page"
```

#### Функции Cypher

```python
from graphorm import count, indegree, outdegree

# COUNT
stmt = select(Page).returns(count(Page))
# Генерирует: MATCH (page:Page) RETURN COUNT(page)

# Indegree и Outdegree
stmt = select(Page).where(outdegree(Page) > 0)
# Генерирует: MATCH (page:Page) WHERE OUTDEGREE(page) > $param_0 RETURN page

# Арифметические операции с функциями
total_degree = indegree(Page) + outdegree(Page)
stmt = select(Page).returns(
    Page,
    total_degree.label("degree")  # Алиас для результата
).orderby(
    total_degree.desc()
).limit(20)
# Генерирует: 
# MATCH (page:Page) 
# RETURN page, INDEGREE(page) + OUTDEGREE(page) AS degree 
# ORDER BY degree DESC 
# LIMIT 20
```

#### Доступные функции:

- `count(*args)` — COUNT
- `sum(expression)` — SUM
- `avg(expression)` — AVG
- `min(expression)` — MIN
- `max(expression)` — MAX
- `indegree(node)` — INDEGREE
- `outdegree(node)` — OUTDEGREE

#### Арифметические операции

```python
# Сложение, вычитание, умножение, деление
total = indegree(Page) + outdegree(Page)
diff = indegree(Page) - outdegree(Page)
product = indegree(Page) * 2
quotient = indegree(Page) / 2

# Сравнение арифметических выражений
stmt = select(Page).where(
    (indegree(Page) + outdegree(Page)) > 10
)

# Алиасы для арифметических выражений
total_degree = (indegree(Page) + outdegree(Page)).label("degree")
stmt = select(Page).returns(Page, total_degree)
```

---

## Работа с Graph

### Создание и подключение

```python
from graphorm import Graph

# Вариант 1: С указанием host/port
graph = Graph("my_graph", host="localhost", port=6379, password="password")

# Вариант 2: С существующим Redis connection
import redis
conn = redis.Redis(host="localhost", port=6379)
graph = Graph("my_graph", connection=conn)

# Создание графа в БД
graph.create()

# Удаление графа
graph.delete()
```

### Добавление узлов и рёбер

```python
# Добавление узлов
page1 = Page(path="/page1", parsed=False, title="Page 1")
page2 = Page(path="/page2", parsed=True, title="Page 2")

graph.add_node(page1)  # Возвращает 1 если новый, 0 если уже существует
graph.add_node(page2)

# Добавление рёбер
link = Linked(page1, page2, weight=0.8)
graph.add_edge(link)

# Сохранение изменений (batch commit)
graph.flush(batch_size=50)  # batch_size=0 отключает батчинг
```

### Выполнение запросов

```python
# Сырой Cypher запрос
result = graph.query(
    "MATCH (p:Page) WHERE p.parsed = false RETURN p LIMIT 10",
    params={"limit": 10}
)

# Query Builder запрос
stmt = select(Page).where(Page.parsed == False).limit(10)
result = graph.execute(stmt)

# Read-only запросы
result = graph.query("MATCH (p:Page) RETURN p", read_only=True)
result = graph.execute(stmt, read_only=True)
```

### Обновление узлов

```python
# Получение узла
page = graph.get_node(Page(path="/page1"))

# Обновление свойств
graph.update_node(page, {"parsed": True, "title": "Updated Title"})
```

### Получение узлов и рёбер

```python
# Получение узла по первичному ключу
page = graph.get_node(Page(path="/page1"))

# Получение ребра
edge = graph.get_edge(Linked(page1, page2))
```

---

## QueryResult

### Структура результата

```python
result = graph.execute(select(Page).limit(10))

# Результаты
result.result_set  # Список списков: [[row1_col1, row1_col2], [row2_col1, row2_col2], ...]
result.header       # Заголовки колонок
result.statistics   # Статистика выполнения запроса

# Свойства статистики
result.nodes_created
result.nodes_deleted
result.properties_set
result.relationships_created
result.relationships_deleted
result.run_time_ms

# Вспомогательные методы
result.is_empty()
result.pretty_print()  # Красивый вывод в консоль
```

### Пример работы с результатами

```python
result = graph.execute(select(Page).where(Page.parsed == False))

if not result.is_empty():
    for row in result.result_set:
        page = row[0]  # Первая колонка - объект Page
        print(f"Path: {page.properties['path']}")
        print(f"Parsed: {page.properties['parsed']}")
```

---

## Управление свойствами

### PropertiesManager

Система использует `PropertiesManager` для изоляции пользовательских свойств от внутренних атрибутов.

#### Доступ к свойствам

```python
page = Page(path="/test", parsed=True, title="Test")

# Получение всех свойств (без внутренних атрибутов)
props = page.properties
# Возвращает: {'path': '/test', 'parsed': True, 'title': 'Test'}
# НЕ включает: __id__, __alias__, __graph__, __relations__, и т.д.

# Обновление свойств
page.update({"parsed": False, "title": "Updated"})
```

#### Валидация

Свойства валидируются на основе аннотаций типов в классе:

```python
class Page(Node):
    path: str
    parsed: bool = False
    count: int = 0

page = Page(path="/test", parsed=True, count=10)  # OK
page.update({"count": "invalid"})  # Может вызвать ошибку валидации
```

---

## Параметризованные запросы

Все значения в Query Builder автоматически параметризуются для безопасности и производительности:

```python
stmt = select(Page).where(Page.path == "/home")
cypher = stmt.to_cypher()
params = stmt.get_params()

# cypher: "MATCH (page:Page) WHERE page.path = $param_0 RETURN page"
# params: {"param_0": "/home"}
```

---

## Batch операции

### Автоматический батчинг

При вызове `graph.flush(batch_size=50)` узлы и рёбра коммитятся батчами:

1. **Узлы коммитятся первыми** (все батчи узлов)
2. **Рёбра коммитятся после** (все батчи рёбер)

Это гарантирует, что все узлы существуют перед созданием рёбер.

### Отключение батчинга

```python
graph.flush(batch_size=0)  # Коммит всех элементов одним запросом
```

---

## Расширенные возможности

### Path (Путь в графе)

```python
# Path возвращается из запросов, возвращающих пути
result = graph.query("MATCH path = (a:Page)-[*]->(b:Page) RETURN path LIMIT 1")

if not result.is_empty():
    path = result.result_set[0][0]
    nodes = path.nodes()
    edges = path.edges()
    first_node = path.first_node()
    last_node = path.last_node()
```

### Registry (Реестр классов)

Система автоматически регистрирует все классы Node и Edge:

```python
from graphorm import Registry

# Получение класса по метке
PageClass = Registry.get_node("Page")

# Получение класса ребра по имени отношения
LinkedClass = Registry.get_edge("Linked")
```

### Схема графа

```python
# Получение меток узлов
label = graph.get_label(0)  # По индексу

# Получение типов свойств
prop = graph.get_property(0)  # По индексу

# Получение типов отношений
relation = graph.get_relation(0)  # По индексу
```

---

## Обработка ошибок

### Исключения

```python
from graphorm import (
    GraphORMError,
    NodeNotFoundError,
    EdgeNotFoundError,
    QueryExecutionError,
    ConnectionError
)

try:
    result = graph.execute(stmt)
except QueryExecutionError as e:
    print(f"Ошибка выполнения запроса: {e}")
except ConnectionError as e:
    print(f"Ошибка подключения: {e}")
```

---

## Примеры использования

### Полный пример

```python
from graphorm import Node, Edge, Graph, select, indegree, outdegree

# Определение моделей
class Page(Node):
    __primary_key__ = ["path"]
    path: str
    parsed: bool = False
    title: str = ""

class Linked(Edge):
    weight: float = 1.0

# Создание графа
graph = Graph("web_graph", host="localhost", port=6379)
graph.create()

# Создание узлов
pages = [
    Page(path="/", parsed=True, title="Home"),
    Page(path="/about", parsed=False, title="About"),
    Page(path="/contact", parsed=False, title="Contact"),
]

for page in pages:
    graph.add_node(page)
graph.flush()

# Создание рёбер
link1 = Linked(pages[0], pages[1], weight=0.9)
link2 = Linked(pages[0], pages[2], weight=0.8)
graph.add_edge(link1)
graph.add_edge(link2)
graph.flush()

# Запрос непроиндексированных страниц
stmt = select(Page).where(
    (Page.parsed == False) & (Page.error.is_null())
).limit(10)
result = graph.execute(stmt)

# Запрос страниц с наибольшей степенью
total_degree = indegree(Page) + outdegree(Page)
stmt = select(Page).where(
    outdegree(Page) > 0
).returns(
    Page,
    total_degree.label("degree")
).orderby(
    total_degree.desc()
).limit(20)
result = graph.execute(stmt)

# Обработка результатов
for row in result.result_set:
    page = row[0]
    degree = row[1]
    print(f"{page.properties['path']}: degree={degree}")

# Очистка
graph.delete()
```

---

## Ограничения и особенности

### RedisGraph/FalkorDB специфичные ограничения

1. **Оператор `=~` (regex):** RedisGraph не поддерживает оператор `=~`. Используйте `starts_with()` или `contains()` вместо `like()`.

2. **Оператор `!=`:** В Cypher используется `<>` вместо `!=`. Query Builder автоматически конвертирует `!=` в `<>`.

3. **Порядок клауз:** В Cypher важен порядок: `MATCH`, `WHERE`, `RETURN`, `ORDER BY`, `SKIP`, `LIMIT`. Query Builder автоматически соблюдает этот порядок.

4. **RETURN обязателен:** В Cypher запрос должен заканчиваться RETURN или другой командой. Query Builder автоматически добавляет RETURN, если он не указан явно.

---

## Производительность

### Рекомендации

1. **Используйте батчинг:** `graph.flush(batch_size=50)` для больших объёмов данных
2. **Read-only запросы:** Используйте `read_only=True` для запросов только на чтение
3. **Параметризация:** Всегда используйте параметризованные запросы (автоматически в Query Builder)
4. **Индексы:** Создавайте индексы на часто используемых свойствах через сырые Cypher запросы

---

## Версионирование

### Текущая версия: 0.1.11

### История изменений

- **0.2.0+:** Удалена зависимость от `camelcase`, метки теперь используют имена классов как есть
- **Query Builder API:** Добавлен объектно-ориентированный Query Builder в стиле SQLAlchemy 2.0
- **Property дескрипторы:** Автоматическое создание Property дескрипторов для аннотированных свойств
- **Арифметические операции:** Поддержка арифметических операций с функциями Cypher

---

## Заключение

GraphORM предоставляет мощный и удобный интерфейс для работы с графовыми базами данных RedisGraph/FalkorDB, сочетая простоту использования с гибкостью и производительностью. Query Builder API позволяет писать типобезопасные запросы без необходимости знания синтаксиса Cypher, а автоматическое управление свойствами упрощает работу с данными.
