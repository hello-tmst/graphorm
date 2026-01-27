# Social Network Use Case

Этот пример демонстрирует использование GraphORM для построения социальной сети с пользователями, друзьями и постами.

## Модели данных

```python
from graphorm import Node, Edge, Graph, select, Relationship

class User(Node):
    __primary_key__ = ["user_id"]
    __indexes__ = ["user_id", "email", "username"]

    user_id: int
    username: str = ""
    email: str = ""
    name: str = ""
    created_at: str = ""

    # Ленивая загрузка связей
    friends = Relationship("FRIEND", direction="both")
    posts = Relationship("POSTED", direction="outgoing")
    liked_posts = Relationship("LIKED", direction="outgoing")

class Post(Node):
    __primary_key__ = ["post_id"]
    __indexes__ = ["post_id", "author_id", "created_at"]

    post_id: int
    author_id: int
    content: str = ""
    created_at: str = ""
    likes_count: int = 0

    # Ленивая загрузка
    author = Relationship("POSTED", direction="incoming")
    likers = Relationship("LIKED", direction="incoming")

class FRIEND(Edge):
    since: str = ""
    status: str = "active"  # active, blocked, etc.

class POSTED(Edge):
    created_at: str = ""

class LIKED(Edge):
    created_at: str = ""
```

## Инициализация

```python
graph = Graph("social_network", host="localhost", port=6379)
graph.create()  # Автоматически создаст индексы
```

## Создание пользователей и постов

```python
# Создание пользователей
alice = User(user_id=1, username="alice", email="alice@example.com", name="Alice")
bob = User(user_id=2, username="bob", email="bob@example.com", name="Bob")
charlie = User(user_id=3, username="charlie", email="charlie@example.com", name="Charlie")

# Создание постов
post1 = Post(post_id=1, author_id=1, content="Hello world!", likes_count=5)
post2 = Post(post_id=2, author_id=2, content="Nice day today!", likes_count=3)

# Использование транзакции
with graph.transaction() as tx:
    tx.add_node(alice)
    tx.add_node(bob)
    tx.add_node(charlie)
    tx.add_node(post1)
    tx.add_node(post2)

    # Добавление дружбы
    tx.add_edge(FRIEND(alice, bob, since="2024-01-01", status="active"))
    tx.add_edge(FRIEND(bob, charlie, since="2024-01-15", status="active"))

    # Связь постов с авторами
    tx.add_edge(POSTED(alice, post1, created_at="2024-01-20"))
    tx.add_edge(POSTED(bob, post2, created_at="2024-01-21"))

    # Лайки
    tx.add_edge(LIKED(charlie, post1, created_at="2024-01-20"))
    tx.add_edge(LIKED(bob, post1, created_at="2024-01-20"))
```

## Поиск друзей пользователя

```python
# Используя Query Builder
UserA = User.alias("a")
UserB = User.alias("b")

stmt = select().match(
    (UserA, FRIEND.alias("f"), UserB)
).where(
    UserA.user_id == 1
).returns(
    UserB
)

result = graph.execute(stmt)
friends = [row[0] for row in result.result_set]
print(f"Alice's friends: {[f.properties['username'] for f in friends]}")

# Или используя ленивую загрузку
alice = graph.get_node(User(user_id=1))
if alice:
    alice.__graph__ = graph  # Установить ссылку на graph для ленивой загрузки
    friends = alice.friends
    print(f"Alice's friends: {[f.properties['username'] for f in friends]}")
```

## Поиск постов пользователя

```python
# Найти все посты пользователя
UserA = User.alias("a")
PostP = Post.alias("p")

stmt = select().match(
    (UserA, POSTED.alias("r"), PostP)
).where(
    UserA.user_id == 1
).returns(
    PostP
).orderby(
    PostP.created_at.desc()
)

result = graph.execute(stmt)
posts = [row[0] for row in result.result_set]
print(f"Alice's posts: {len(posts)}")
```

## Поиск самых популярных постов

```python
from graphorm import count

# Найти посты с наибольшим количеством лайков
stmt = select().match(Post.alias("p")).returns(
    Post.alias("p")
).orderby(
    Post.alias("p").likes_count.desc()
).limit(10)

result = graph.execute(stmt)
for row in result.result_set:
    post = row[0]
    print(f"Post {post.properties['post_id']}: {post.properties['likes_count']} likes")
```

## Поиск друзей друзей (2-hop)

```python
# Найти друзей друзей Alice
UserA = User.alias("a")
UserB = User.alias("b")
UserC = User.alias("c")

stmt = select().match(
    (UserA, FRIEND.alias("f1"), UserB),
    (UserB, FRIEND.alias("f2"), UserC)
).where(
    (UserA.user_id == 1) & (UserC.user_id != 1)
).returns(
    UserC
)

result = graph.execute(stmt)
friends_of_friends = [row[0] for row in result.result_set]
print(f"Friends of Alice's friends: {[f.properties['username'] for f in friends_of_friends]}")
```

## Поиск пользователей, которые лайкнули пост

```python
# Найти всех пользователей, которые лайкнули конкретный пост
UserU = User.alias("u")
PostP = Post.alias("p")

stmt = select().match(
    (UserU, LIKED.alias("l"), PostP)
).where(
    PostP.post_id == 1
).returns(
    UserU
)

result = graph.execute(stmt)
likers = [row[0] for row in result.result_set]
print(f"Users who liked post 1: {[u.properties['username'] for u in likers]}")
```

## Bulk-операции для массового создания пользователей

```python
# Массовое создание пользователей
users_data = [
    {"user_id": i, "username": f"user{i}", "email": f"user{i}@example.com", "name": f"User {i}"}
    for i in range(1000, 2000)
]

result = graph.bulk_upsert(User, users_data, batch_size=500)
print(f"Created {len(users_data)} users")
```

## Обновление данных

```python
# Обновить количество лайков поста
post = graph.get_node(Post(post_id=1))
if post:
    graph.update_node(post, {"likes_count": post.properties["likes_count"] + 1})
```

## Статистика сети

```python
from graphorm import count, avg

# Общее количество пользователей
stmt = select().match(User.alias("u")).returns(count(User.alias("u")))
result = graph.execute(stmt)
total_users = result.result_set[0][0]
print(f"Total users: {total_users}")

# Среднее количество друзей на пользователя
# (требует более сложного запроса с агрегацией)
```

## Полный пример

```python
from graphorm import Node, Edge, Graph, select, Relationship

# Определение моделей
class User(Node):
    __primary_key__ = ["user_id"]
    __indexes__ = ["user_id", "username"]
    user_id: int
    username: str = ""
    email: str = ""
    friends = Relationship("FRIEND", direction="both")

class Post(Node):
    __primary_key__ = ["post_id"]
    post_id: int
    author_id: int
    content: str = ""
    likes_count: int = 0

class FRIEND(Edge):
    since: str = ""

class POSTED(Edge):
    pass

# Создание графа
graph = Graph("social_network", host="localhost", port=6379)
graph.create()

# Создание данных
with graph.transaction() as tx:
    alice = User(user_id=1, username="alice")
    bob = User(user_id=2, username="bob")
    post1 = Post(post_id=1, author_id=1, content="Hello!", likes_count=10)

    tx.add_node(alice)
    tx.add_node(bob)
    tx.add_node(post1)
    tx.add_edge(FRIEND(alice, bob))
    tx.add_edge(POSTED(alice, post1))

# Поиск друзей
UserA = User.alias("a")
UserB = User.alias("b")

stmt = select().match(
    (UserA, FRIEND.alias("f"), UserB)
).where(
    UserA.user_id == 1
).returns(UserB)

result = graph.execute(stmt)
for row in result.result_set:
    friend = row[0]
    print(f"Friend: {friend.properties['username']}")

graph.delete()
```
