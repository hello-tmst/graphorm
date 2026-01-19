from logging import getLogger
from typing import TYPE_CHECKING

import redis

from .node import Node
from .edge import Edge
from .types import CMD
from .query_result import QueryResult
from .utils import quote_string

if TYPE_CHECKING:
    from graphorm.drivers.base import Driver

logger = getLogger(__file__)


class Graph:
    __slots__ = {"_name", "_nodes", "_edges", "_labels", "_property_keys", "_relationship_types", "_driver"}

    def __init__(
        self,
        name: str,
        connection: redis.Redis = None,
        host: str = None,
        port: int = 6379,
        password: str = None,
    ):
        """
        Initialize a Graph instance.
        
        :param name: Name of the graph
        :param connection: Redis connection object (optional)
        :param host: Redis host (required if connection not provided)
        :param port: Redis port (default: 6379)
        :param password: Redis password (optional)
        """
        self._name: str = name  # Graph key
        
        # Initialize driver
        from .drivers.redis import RedisDriver
        if connection:
            # Extract connection parameters if available
            try:
                conn_kwargs = connection.connection_pool.connection_kwargs
                driver_host = conn_kwargs.get('host', 'localhost')
                driver_port = conn_kwargs.get('port', 6379)
                driver_password = conn_kwargs.get('password')
            except AttributeError:
                # Fallback if connection_pool structure is different
                driver_host = 'localhost'
                driver_port = 6379
                driver_password = None
            
            self._driver = RedisDriver(driver_host, driver_port, driver_password)
            self._driver.connection = connection
        elif host:
            self._driver = RedisDriver(host, port, password)
        else:
            raise ValueError("Either connection or host must be provided")

        # Initialize collections
        self._nodes: dict[str, Node] = {}  # Dictionary of nodes by alias
        self._edges: dict[str, Edge] = {}  # Dictionary of edges by alias
        
        # Schema metadata
        self._labels: list[str] = []  # List of node labels.
        self._property_keys: list[str] = []  # List of property keys.
        self._relationship_types: list[str] = []  # List of relationship types.
        # self.version = 0  # Graph version
    
    @property
    def driver(self) -> "Driver":
        """Get the driver instance."""
        return self._driver

    @property
    def name(self) -> str:
        return self._name

    @property
    def nodes(self) -> dict[str, Node]:
        return self._nodes

    @property
    def edges(self) -> dict[str, Node]:
        return self._edges

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
            prop = self._property_keys[idx]
        except IndexError:
            # Refresh properties.
            self._refresh_property_keys()
            prop = self._property_keys[idx]
        return prop

    def add_node(self, node: Node) -> int:
        """
        Adds a node to the graph.

        :param node: Node instance to add
        :return: 1 if node is new, 0 if node already exists (by primary key)
        """
        # Check if node with same primary key already exists
        for existing_node in self._nodes.values():
            if node == existing_node:
                # Node already exists, return 0
                return 0
        
        # New node, add it
        self._nodes[node.alias] = node
        return 1

    def add_edge(self, edge: Edge) -> int:
        """
        Adds an edge to the graph.

        :param edge: Edge instance to add
        :return: 1 if edge is new, 0 if edge already exists
        """
        # Ensure source and destination nodes exist
        if edge.src_node.alias not in self._nodes:
            self.add_node(edge.src_node)
        if edge.dst_node.alias not in self._nodes:
            self.add_node(edge.dst_node)
        
        # Check if edge already exists
        for existing_edge in self._edges.values():
            if edge == existing_edge:
                # Edge already exists, return 0
                return 0
        
        # New edge, add it
        self._edges[edge.alias] = edge
        return 1

    def _refresh_labels(self) -> list[str]:
        result = self._driver.call_procedure(self._name, "db.labels", read_only=True, graph=self)
        self._labels = self._unpack(result.result_set)
        return self._labels

    def _refresh_property_keys(self) -> list[str]:
        result = self._driver.call_procedure(self._name, "db.propertyKeys", read_only=True, graph=self)
        self._property_keys = self._unpack(result.result_set)
        return self._property_keys

    def _refresh_relationship_types(self) -> list[str]:
        result = self._driver.call_procedure(self._name, "db.relationshipTypes", read_only=True, graph=self)
        self._relationship_types = self._unpack(result.result_set)
        return self._relationship_types

    def _clear_schema(self):
        self._labels = []
        self._property_keys = []
        self._relationship_types = []

    def get_relation(self, idx: int) -> str:
        """
        Returns a relationship type by its index.

        :param idx: The index of the relationship type
        :return: Relationship type string
        """
        try:
            relation = self._relationship_types[idx]
        except IndexError:
            # Refresh relationship types.
            self._refresh_relationship_types()
            relation = self._relationship_types[idx]
        return relation

    def flush(self) -> QueryResult:
        """
        Flush all pending changes to the database.

        :return: QueryResult object
        """
        items = list(self._nodes.values()) + list(self._edges.values())
        result = self._driver.commit(self, items)
        # Optionally clear local cache after successful flush
        # self._nodes.clear()
        # self._edges.clear()
        return result

    def query(
        self,
        q: str,
        params: dict = None,
        timeout: int = None,
        read_only: bool = False,
    ) -> QueryResult:
        """
        Execute a Cypher query against the graph.

        :param q: Cypher query string
        :param params: Query parameters dictionary
        :param timeout: Maximum runtime for read queries in milliseconds
        :param read_only: Execute as read-only query if set to True
        :return: QueryResult object
        """
        result = self._driver.query(CMD.QUERY, self._name, q, params, timeout, read_only, graph=self)
        return result

    def update_node(self, node: Node, properties: dict) -> QueryResult:
        """
        Update properties of a node in the graph.

        :param node: Node instance to update
        :param properties: Dictionary of properties to update
        :return: QueryResult object
        """
        # Generate SET clause
        set_clauses = []
        for key, value in properties.items():
            if value is not None:
                set_clauses.append(f"{node.alias}.{key}={quote_string(value)}")
        
        if not set_clauses:
            raise ValueError("No properties to update")
        
        # Generate query
        query = f"MATCH {node.__str_pk__()} SET {', '.join(set_clauses)}"
        
        # Execute query
        result = self.query(query)
        
        # Update local node if it exists in cache
        if node.alias in self._nodes:
            self._nodes[node.alias].update(properties)
        
        return result

    def get_node(self, node: Node) -> Node | None:
        """
        Get a node from the graph by its primary key.

        :param node: Node instance with primary key set
        :return: Node instance if found, None otherwise
        """
        q = f"MATCH {node.__str_pk__()} RETURN {node.alias}"
        result = self.query(q)
        if len(result.result_set) > 0:
            found_node = result.result_set[0][0]
            found_node.set_alias(node.alias)
            return found_node
        return None

    def get_edge(self, edge: Edge) -> Edge | None:
        """
        Get an edge from the graph.

        :param edge: Edge instance to search for
        :return: Edge instance if found, None otherwise
        """
        q = f"MATCH {edge.src_node.__str_pk__()}-{edge.__str_pk__()}->{edge.dst_node.__str_pk__()} RETURN {edge.alias}"
        result = self.query(q)
        if len(result.result_set) > 0:
            found_edge = result.result_set[0][0]
            found_edge.set_alias(edge.alias)
            return found_edge
        return None

    def create(self, timeout: int = None) -> QueryResult:
        """
        Create a new graph in the database.

        :param timeout: Maximum runtime in milliseconds
        :return: QueryResult object
        """
        return self._driver.query(CMD.QUERY, self._name, "RETURN 0", timeout=timeout)

    def delete(self) -> QueryResult:
        """
        Delete the graph from the database.

        :return: QueryResult object
        """
        return self._driver.query(CMD.DELETE, self._name)

    @staticmethod
    def _unpack(result_set: list[list[str]]) -> list[str]:
        result: list[str | None] = [None] * len(result_set)
        for i, l in enumerate(result_set):
            result[i] = l[0]
        return result
