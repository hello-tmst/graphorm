class _Registry:
    __dict__ = {}

    def add_node_label(self, node: type):
        # Use explicit label if set, otherwise use class name
        if hasattr(node, "__label__"):
            label = node.__label__
        else:
            label = node.__name__
        self.__dict__[label] = node

    def add_edge_relation(self, edge: type):
        # Use explicit relation name if set, otherwise use class name
        if hasattr(edge, "__relation_name__"):
            relation = edge.__relation_name__
        else:
            relation = edge.__name__
        self.__dict__[relation] = edge

    def get_node(self, label: str) -> type:
        if label not in self.__dict__:
            raise KeyError(f"Node with label '{label}' not found in registry. Available labels: {list(self.__dict__.keys())}")
        return self.__dict__[label]

    def get_edge(self, relation: str) -> type:
        if relation not in self.__dict__:
            raise KeyError(f"Edge with relation '{relation}' not found in registry. Available relations: {list(self.__dict__.keys())}")
        return self.__dict__[relation]


Registry = _Registry()
