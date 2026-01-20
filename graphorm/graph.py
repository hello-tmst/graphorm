from logging import getLogger
from typing import TYPE_CHECKING

import redis

from .node import Node
from .edge import Edge
from .types import CMD
from .query_result import QueryResult
from .utils import quote_string, format_cypher_value

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

    def flush(self, batch_size: int = 50) -> QueryResult:
        """
        Flush all pending changes to the database in batches.

        :param batch_size: Number of items to commit per batch (default: 50). Set to 0 or negative to disable batching.
        :return: QueryResult object
        """
        items = list(self._nodes.values()) + list(self._edges.values())
        result = self._driver.commit(self, items, batch_size=batch_size)
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
    
    def execute(self, stmt, read_only: bool = False, timeout: int = None) -> QueryResult:
        """
        Execute a Select statement.
        
        :param stmt: Select statement object
        :param read_only: Execute as read-only query if set to True
        :param timeout: Maximum runtime for read queries in milliseconds
        :return: QueryResult object
        """
        cypher = stmt.to_cypher()
        params = stmt.get_params()
        return self.query(cypher, params=params, read_only=read_only, timeout=timeout)

    def update_node(self, node: Node, properties: dict) -> QueryResult:
        """
        Update properties of a node in the graph.

        :param node: Node instance to update
        :param properties: Dictionary of properties to update
        :return: QueryResult object
        """
        # Ensure node has all required properties for primary key lookup
        # If node was retrieved from graph, it might be missing some properties
        # We need to ensure primary key properties are present
        if hasattr(node, '__primary_key__'):
            if isinstance(node.__primary_key__, str):
                pk = node.__primary_key__
                if pk not in node.properties:
                    logger.warning(
                        f"update_node: Primary key '{pk}' not in node properties. "
                        f"Node properties: {node.properties}"
                    )
            elif isinstance(node.__primary_key__, list):
                for pk in node.__primary_key__:
                    if pk not in node.properties:
                        logger.warning(
                            f"update_node: Primary key '{pk}' not in node properties. "
                            f"Node properties: {node.properties}"
                        )
        
        # Generate SET clause
        set_clauses = []
        for key, value in properties.items():
            # Include value if it's not None (this includes True, False, 0, empty strings, etc.)
            if value is not None:
                value_str = format_cypher_value(value)
                set_clauses.append(f"{node.alias}.{key}={value_str}")
            # Note: False is not None, so it's already handled above
        
        if not set_clauses:
            raise ValueError("No properties to update")
        
        # Generate query - use node alias in MATCH to ensure we find the right node
        pk_pattern = node.__str_pk__()
        query = f"MATCH {pk_pattern} SET {', '.join(set_clauses)} RETURN {node.alias}"
        
        # Log query for debugging
        logger.debug(f"update_node query: {query}")
        logger.debug(f"update_node node properties: {node.properties}")
        logger.debug(f"update_node node __str_pk__: {pk_pattern}")
        logger.debug(f"update_node update properties: {properties}")
        
        # Execute query
        result = self.query(query)
        
        # Check if node was found and updated
        if hasattr(result, 'result_set') and len(result.result_set) == 0:
            logger.error(
                f"update_node: Node not found for update. "
                f"Query: {query}, "
                f"Node properties: {node.properties}, "
                f"Update properties: {properties}, "
                f"Node __str_pk__: {pk_pattern}"
            )
            # Try to find the node with a simpler query to debug
            debug_query = f"MATCH (n) WHERE n.path = {quote_string(node.properties.get('path', ''))} RETURN n LIMIT 1"
            debug_result = self.query(debug_query)
            if len(debug_result.result_set) > 0:
                found_node = debug_result.result_set[0][0]
                logger.error(
                    f"update_node: Node exists in graph but not found by primary key. "
                    f"Found node properties: {found_node.properties}, "
                    f"Expected primary key: {node.properties.get('path', '')}"
                )
            else:
                logger.error(f"update_node: Node does not exist in graph at all.")
        
        # Check if update was successful by verifying properties_set in statistics
        if hasattr(result, 'statistics') and 'Properties set' in result.statistics:
            properties_set = result.statistics.get('Properties set', 0)
            if properties_set == 0:
                logger.error(
                    f"update_node: No properties were set. "
                    f"Query: {query}, "
                    f"Node properties: {node.properties}, "
                    f"Update properties: {properties}, "
                    f"Result statistics: {result.statistics}"
                )
            else:
                logger.debug(f"update_node: Successfully set {properties_set} properties")
        
        # Update local node if it exists in cache
        # But also check by primary key, as node from graph may have different alias
        if node.alias in self._nodes:
            self._nodes[node.alias].update(properties)
        else:
            # Try to find node in cache by primary key and update it
            # This is important when updating a node retrieved from graph (different alias)
            if hasattr(node, '__primary_key__'):
                if isinstance(node.__primary_key__, str):
                    pk = node.__primary_key__
                    pk_value = node.properties.get(pk)
                    if pk_value is not None:
                        for cached_alias, cached_node in self._nodes.items():
                            if (hasattr(cached_node, '__primary_key__') and 
                                isinstance(cached_node.__primary_key__, str) and
                                cached_node.__primary_key__ == pk and
                                cached_node.properties.get(pk) == pk_value):
                                # Found node by primary key, update it
                                cached_node.update(properties)
                                logger.debug(f"update_node: Updated cached node by primary key {pk}={pk_value}")
                                break
                elif isinstance(node.__primary_key__, list):
                    # For composite primary keys, check all key values match
                    for cached_alias, cached_node in self._nodes.items():
                        if hasattr(cached_node, '__primary_key__'):
                            if isinstance(cached_node.__primary_key__, list):
                                # Check if all primary key values match
                                match = True
                                for pk in node.__primary_key__:
                                    if cached_node.properties.get(pk) != node.properties.get(pk):
                                        match = False
                                        break
                                if match:
                                    # Found node by primary key, update it
                                    cached_node.update(properties)
                                    logger.debug(f"update_node: Updated cached node by composite primary key")
                                    break
        
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
