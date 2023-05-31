from stringcase import camelcase


class _Registry:
    __dict__ = {}

    def add_node_label(self, node: type):
        self.__dict__[camelcase(node.__name__)] = node

    def add_edge_relation(self, edge: type):
        self.__dict__[camelcase(edge.__name__)] = edge

    def get_node(self, label: str) -> type:
        return self.__dict__[label]

    def get_edge(self, relation: str) -> type:
        return self.__dict__[relation]


Registry = _Registry()
