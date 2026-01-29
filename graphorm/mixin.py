from graphorm.drivers.base import Driver

from .edge import Edge
from .graph import Graph
from .node import Node
from .query_result import QueryResult
from .types import CMD


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
        match_clause, params = graph._build_edge_match_clause(edge)
        q = f"MATCH {match_clause} RETURN {edge.alias}"
        result = self.query(CMD.QUERY, graph.name, q, params=params, graph=graph).result_set
        if len(result) > 0:
            _edge = result[0][0]
            _edge.set_alias(edge.alias)
            return _edge
        return None


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
