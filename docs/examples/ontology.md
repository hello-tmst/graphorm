# Ontology / Knowledge Graph Use Case

Этот пример демонстрирует использование GraphORM для построения онтологии или графа знаний с сущностями и отношениями.

## Модели данных

```python
from graphorm import Node, Edge, Graph, select, Relationship

class Entity(Node):
    __primary_key__ = ["entity_id"]
    __indexes__ = ["entity_id", "name", "type"]

    entity_id: str
    name: str = ""
    type: str = ""  # Person, Organization, Concept, etc.
    description: str = ""

    # Ленивая загрузка связей
    related_entities = Relationship("RELATES_TO", direction="both")
    instances = Relationship("INSTANCE_OF", direction="outgoing")
    subclasses = Relationship("SUBCLASS_OF", direction="outgoing")

class RELATES_TO(Edge):
    relation_type: str = ""  # "influenced_by", "collaborated_with", etc.
    strength: float = 1.0

class INSTANCE_OF(Edge):
    pass

class SUBCLASS_OF(Edge):
    pass

class PART_OF(Edge):
    pass
```

## Инициализация

```python
graph = Graph("ontology", host="localhost", port=6379)
graph.create()
```

## Создание онтологии

```python
# Создание сущностей
person = Entity(entity_id="Person", name="Person", type="Concept", description="A human being")
organization = Entity(entity_id="Organization", name="Organization", type="Concept", description="A group of people")
company = Entity(entity_id="Company", name="Company", type="Concept", description="A business organization")
alice = Entity(entity_id="alice_001", name="Alice", type="Person", description="Software engineer")
acme_corp = Entity(entity_id="acme_001", name="Acme Corp", type="Company", description="Tech company")

# Создание иерархии и связей
with graph.transaction() as tx:
    tx.add_node(person)
    tx.add_node(organization)
    tx.add_node(company)
    tx.add_node(alice)
    tx.add_node(acme_corp)

    # Иерархия: Company является подклассом Organization
    tx.add_edge(SUBCLASS_OF(company, organization))

    # Alice является экземпляром Person
    tx.add_edge(INSTANCE_OF(alice, person))

    # Acme Corp является экземпляром Company
    tx.add_edge(INSTANCE_OF(acme_corp, company))

    # Alice работает в Acme Corp
    tx.add_edge(RELATES_TO(alice, acme_corp, relation_type="works_at", strength=0.9))
```

## Поиск всех экземпляров концепта

```python
# Найти всех людей (экземпляры Person)
EntityE = Entity.alias("e")
EntityP = Entity.alias("p")

stmt = select().match(
    (EntityE, INSTANCE_OF.alias("i"), EntityP)
).where(
    EntityP.entity_id == "Person"
).returns(
    EntityE
)

result = graph.execute(stmt)
people = [row[0] for row in result.result_set]
print(f"People: {[p.properties['name'] for p in people]}")
```

## Поиск подклассов

```python
# Найти все подклассы Organization
stmt = select().match(
    (EntityE, SUBCLASS_OF.alias("s"), EntityP)
).where(
    EntityP.entity_id == "Organization"
).returns(
    EntityE
)

result = graph.execute(stmt)
subclasses = [row[0] for row in result.result_set]
print(f"Subclasses of Organization: {[s.properties['name'] for s in subclasses]}")
```

## Поиск связанных сущностей

```python
# Найти все сущности, связанные с Alice
EntityA = Entity.alias("a")
EntityB = Entity.alias("b")

stmt = select().match(
    (EntityA, RELATES_TO.alias("r"), EntityB)
).where(
    EntityA.entity_id == "alice_001"
).returns(
    EntityB,
    RELATES_TO.alias("r")
)

result = graph.execute(stmt)
for row in result.result_set:
    related, relation = row
    print(f"Alice {relation.properties['relation_type']} {related.properties['name']}")
```

## Поиск по типу связи

```python
# Найти все сущности, связанные отношением "works_at"
stmt = select().match(
    (EntityA, RELATES_TO.alias("r"), EntityB)
).where(
    RELATES_TO.alias("r").relation_type == "works_at"
).returns(
    EntityA,
    EntityB
)

result = graph.execute(stmt)
for row in result.result_set:
    employee, employer = row
    print(f"{employee.properties['name']} works at {employer.properties['name']}")
```

## Транзитивные запросы (многоуровневые связи)

```python
# Найти все компании, в которых работают люди
# (Person) -[INSTANCE_OF]-> (Person concept)
# (Person) -[RELATES_TO:works_at]-> (Company) -[INSTANCE_OF]-> (Company concept)

PersonConcept = Entity.alias("pc")
PersonInstance = Entity.alias("pi")
CompanyInstance = Entity.alias("ci")
CompanyConcept = Entity.alias("cc")

stmt = select().match(
    (PersonInstance, INSTANCE_OF.alias("i1"), PersonConcept),
    (PersonInstance, RELATES_TO.alias("r"), CompanyInstance),
    (CompanyInstance, INSTANCE_OF.alias("i2"), CompanyConcept)
).where(
    (PersonConcept.entity_id == "Person") &
    (RELATES_TO.alias("r").relation_type == "works_at")
).returns(
    CompanyInstance
)

result = graph.execute(stmt)
companies = [row[0] for row in result.result_set]
print(f"Companies with employees: {[c.properties['name'] for c in companies]}")
```

## Bulk-операции для массового создания сущностей

```python
# Массовое создание сущностей из данных
entities_data = [
    {"entity_id": f"entity_{i}", "name": f"Entity {i}", "type": "Concept", "description": f"Description {i}"}
    for i in range(1000)
]

result = graph.bulk_upsert(Entity, entities_data, batch_size=500)
print(f"Created {len(entities_data)} entities")
```

## Поиск по описанию (текстовый поиск)

```python
# Найти сущности, содержащие определенное слово в описании
stmt = select().match(Entity.alias("e")).where(
    Entity.alias("e").description.contains("software")
).returns(
    Entity.alias("e")
)

result = graph.execute(stmt)
matching_entities = [row[0] for row in result.result_set]
print(f"Entities with 'software' in description: {[e.properties['name'] for e in matching_entities]}")
```

## Статистика онтологии

```python
from graphorm import count

# Количество сущностей каждого типа
stmt = select().match(Entity.alias("e")).where(
    Entity.alias("e").type == "Person"
).returns(
    count(Entity.alias("e"))
)

result = graph.execute(stmt)
person_count = result.result_set[0][0]
print(f"Total persons: {person_count}")
```

## Полный пример

```python
from graphorm import Node, Edge, Graph, select

# Определение моделей
class Entity(Node):
    __primary_key__ = ["entity_id"]
    __indexes__ = ["entity_id", "type"]
    entity_id: str
    name: str = ""
    type: str = ""

class SUBCLASS_OF(Edge):
    pass

class INSTANCE_OF(Edge):
    pass

# Создание графа
graph = Graph("ontology", host="localhost", port=6379)
graph.create()

# Создание онтологии
with graph.transaction() as tx:
    animal = Entity(entity_id="Animal", name="Animal", type="Concept")
    mammal = Entity(entity_id="Mammal", name="Mammal", type="Concept")
    dog = Entity(entity_id="Dog", name="Dog", type="Concept")
    my_dog = Entity(entity_id="buddy_001", name="Buddy", type="Dog")

    tx.add_node(animal)
    tx.add_node(mammal)
    tx.add_node(dog)
    tx.add_node(my_dog)

    # Иерархия: Mammal -> Animal, Dog -> Mammal
    tx.add_edge(SUBCLASS_OF(mammal, animal))
    tx.add_edge(SUBCLASS_OF(dog, mammal))

    # Buddy является экземпляром Dog
    tx.add_edge(INSTANCE_OF(my_dog, dog))

# Найти все млекопитающие (прямые и транзитивные подклассы Animal)
EntityE = Entity.alias("e")
EntityA = Entity.alias("a")

stmt = select().match(
    (EntityE, SUBCLASS_OF.alias("s"), EntityA)
).where(
    EntityA.entity_id == "Animal"
).returns(EntityE)

result = graph.execute(stmt)
mammals = [row[0] for row in result.result_set]
print(f"Mammals: {[m.properties['name'] for m in mammals]}")

graph.delete()
```
