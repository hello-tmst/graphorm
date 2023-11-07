from logging import getLogger

from .node import Node
from .edge import Edge

logger = getLogger(__file__)


class Graph:
    def __init__(self, name: str):
        """

        :param name:
        :return:
        """
        self.name = name  # Graph key

        self.nodes = {}
        self.edges = {}
        self._labels = []  # List of node labels.
        self._property_keys = []  # List of property keys.
        self._relationship_types = []  # List of relationship types.
        self.version = 0  # Graph version

    def get_label(self, idx: int):
        """
        Returns a label by it's index.

        :param idx: The index of the label
        :return:
        """
        try:
            label = self._labels[idx]
        except IndexError:
            # Refresh labels.
            self._refresh_labels()
            label = self._labels[idx]
        return label

    def get_property(self, idx: int):
        """
        Returns a property by it's index.

        :param idx: The index of the property
        :return:
        """
        try:
            prop = self._properties[idx]
        except IndexError:
            # Refresh properties.
            self._refresh_property_keys()
            prop = self._properties[idx]
        return prop

    def add_node(self, node: Node) -> None:
        """
        Adds a node to the graph.

        :param node:
        :return:
        """
        self.nodes[node.alias] = node

    def add_edge(self, edge: Edge) -> None:
        """
        Adds an edge to the graph.

        :param edge:
        :return:
        """
        if edge.src_node.alias not in self.nodes:
            self.add_node(edge.src_node)
        if edge.dst_node.alias not in self.nodes:
            self.add_node(edge.dst_node)
        self.edges[edge.alias] = edge

    def _refresh_labels(self) -> list[str]:
        return self._call_procedure("db.labels")

    def _refresh_property_keys(self) -> list[str]:
        return self._call_procedure("db.propertyKeys")

    def _refresh_relationship_types(self) -> list[str]:
        return self._call_procedure("db.relationshipTypes")

    def _call_procedure(self, q: str):
        return self._unpack(self.call_procedure(q, read_only=True).result_set)

    def _clear_schema(self):
        self._labels = []
        self._properties = []
        self._relationship_types = []

    @staticmethod
    def _unpack(result_set: list[list[str]]) -> list[str]:
        result: list[str | None] = [None] * len(result_set)
        for i, l in enumerate(result_set):
            result[i] = l[0]
        return result
