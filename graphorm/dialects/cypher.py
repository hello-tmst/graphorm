from graphorm.node import Node
from graphorm.edge import Edge


class CypherQuery:
    def merge(self, item) -> "CypherQuery":
        return self
