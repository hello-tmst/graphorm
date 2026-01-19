from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from graphorm.types import CMD
    from graphorm.common import Common
    from graphorm.query_result import QueryResult
    from graphorm.graph import Graph


class Driver(ABC):
    """Abstract base class for graph database drivers."""

    @abstractmethod
    def query(
        self,
        cmd: "CMD",
        graph_name: str,
        /,
        q: str = "",
        params: dict = None,
        timeout: int = None,
        read_only: bool = False,
    ) -> "QueryResult":
        """
        Execute a query against the graph.

        :param cmd: Command type (QUERY, RO_QUERY, DELETE)
        :param graph_name: Name of the graph
        :param q: The query string
        :param params: Query parameters
        :param timeout: Maximum runtime for read queries in milliseconds
        :param read_only: Execute a readonly query if set to True
        :return: QueryResult object
        """
        pass

    @abstractmethod
    def call_procedure(
        self,
        graph_name: str,
        procedure: str,
        *args,
        read_only: bool = False,
        graph=None,
        **kwargs,
    ) -> "QueryResult":
        """
        Call a stored procedure.

        :param graph_name: Name of the graph
        :param procedure: Procedure name
        :param args: Procedure arguments
        :param read_only: Execute as read-only if set to True
        :param graph: Graph instance (optional, for schema access in QueryResult)
        :param kwargs: Additional procedure parameters
        :return: QueryResult object
        """
        pass

    @abstractmethod
    def commit(
        self,
        graph: "Graph",
        items: list["Common"] = None,
    ) -> "QueryResult | None":
        """
        Commit changes to the graph.

        :param graph: Graph instance
        :param items: List of nodes and edges to commit. If None, uses graph's nodes and edges
        :return: QueryResult object or None if nothing to commit
        """
        pass
