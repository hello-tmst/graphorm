from graphorm.drivers.base import Driver
from .node import Node
from .edge import Edge
from .types import CMD
from .graph import Graph
from .query_result import QueryResult


class NodeMixin(Driver):
    def get(self, graph: Graph, node: Node) -> Node | None:
        """
        Get node from the graph.

        :param graph:
        :param node: The instance of node
        :return: Node, if it in the graph, else None
        """
        q = f"MATCH {node.__str_pk__()} RETURN {node.alias}"
        result = self.query(CMD.QUERY, graph.name, q, graph=graph).result_set
        if len(result) > 0:
            _node = result[0][0]
            _node.set_alias(node.alias)
            return _node


class EdgeMixin(Driver):
    def get(self, graph: Graph, edge: Edge) -> Edge | None:
        """
        Get edge from the graph.

        :param graph:
        :param edge: The instance of node
        :return: Edge, if it in the graph, else None
        """
        q = f"MATCH {edge.src_node.__str_pk__()}-{edge.__str_pk__()}->{edge.dst_node.__str_pk__()} RETURN {edge.relation}"
        result = self.query(CMD.QUERY, graph.name, q, graph=graph).result_set
        if len(result) > 0:
            _edge = result[0][0]
            _edge.set_alias(edge.alias)
            return _edge


class GraphMixin(Driver):
    def create(self, graph: Graph, timeout: int = None) -> QueryResult:
        """
        Create a new graph.

        :param graph:
        :param timeout:
        :return QueryResult:
        """
        return self.query(CMD.QUERY, graph.name, "RETURN 0", timeout=timeout)

    def delete(self, graph: Graph) -> QueryResult:
        """
        Delete a graph.

        :param graph:
        :return:
        """
        return self.query(CMD.DELETE, graph.name)
