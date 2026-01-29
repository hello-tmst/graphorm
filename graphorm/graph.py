from __future__ import annotations

from logging import getLogger
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
    TypeVar,
    Union,
)

import redis

from .edge import Edge
from .node import Node
from .query_result import QueryResult
from .types import CMD
from .utils import (
    format_cypher_value,
    get_pk_fields,
    quote_string,
)

if TYPE_CHECKING:
    from graphorm.delete import Delete
    from graphorm.drivers.base import Driver
    from graphorm.select import Select

logger = getLogger(__file__)

N = TypeVar("N", bound=Node)


class Graph:
    __slots__ = {
        "_name",
        "_nodes",
        "_edges",
        "_labels",
        "_property_keys",
        "_relationship_types",
        "_driver",
    }

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
                driver_host = conn_kwargs.get("host", "localhost")
                driver_port = conn_kwargs.get("port", 6379)
                driver_password = conn_kwargs.get("password")
            except AttributeError:
                # Fallback if connection_pool structure is different
                driver_host = "localhost"
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
        result = self._driver.call_procedure(
            self._name, "db.labels", read_only=True, graph=self
        )
        self._labels = self._unpack(result.result_set)
        return self._labels

    def _refresh_property_keys(self) -> list[str]:
        result = self._driver.call_procedure(
            self._name, "db.propertyKeys", read_only=True, graph=self
        )
        self._property_keys = self._unpack(result.result_set)
        return self._property_keys

    def _refresh_relationship_types(self) -> list[str]:
        result = self._driver.call_procedure(
            self._name, "db.relationshipTypes", read_only=True, graph=self
        )
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
        params: Optional[dict[str, Any]] = None,
        timeout: Optional[int] = None,
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
        result = self._driver.query(
            CMD.QUERY, self._name, q, params, timeout, read_only, graph=self
        )
        return result

    def execute(
        self,
        stmt: Union["Select", "Delete"],
        read_only: bool = False,
        timeout: Optional[int] = None,
    ) -> QueryResult:
        """
        Execute a Select or Delete statement.

        :param stmt: Select or Delete statement object
        :param read_only: Execute as read-only query if set to True (ignored for Delete)
        :param timeout: Maximum runtime for read queries in milliseconds
        :return: QueryResult object
        """
        from .delete import Delete

        cypher = stmt.to_cypher()
        params = stmt.get_params()
        # Delete statements are always write operations
        actual_read_only = read_only and not isinstance(stmt, Delete)
        return self.query(
            cypher, params=params, read_only=actual_read_only, timeout=timeout
        )

    def delete_node(self, node: Node, detach: bool = False) -> QueryResult:
        """
        Delete a node from the graph.

        :param node: Node instance to delete
        :param detach: If True, use DETACH DELETE (removes all relationships)
        :return: QueryResult object
        """
        pk_pattern = node.__str_pk__()
        delete_keyword = "DETACH DELETE" if detach else "DELETE"
        query = f"MATCH {pk_pattern} {delete_keyword} {node.alias}"
        return self.query(query)

    def _build_edge_match_clause(self, edge: Edge) -> tuple[str, dict]:
        """
        Build MATCH clause for an edge. Returns (match_clause, params) for use in
        MATCH {match_clause} RETURN r or MATCH {match_clause} DELETE r.
        Supports both Node endpoints and int node IDs (e.g. from parsed query results).
        Uses safe string concatenation for IDs (FalkorDB/RedisGraph do not support $params).
        """
        params: dict = {}
        # Source
        if isinstance(edge.src_node, Node):
            src_pat = edge.src_node.__str_pk__()
            if not src_pat or not src_pat.startswith("("):
                raise ValueError(f"Invalid source node pattern: {src_pat}")
        else:
            src_pat = "(src)"
            src_id_safe = str(int(edge.src_node))
        # Destination
        if isinstance(edge.dst_node, Node):
            dst_pat = edge.dst_node.__str_pk__()
            if not dst_pat or not dst_pat.startswith("("):
                raise ValueError(f"Invalid destination node pattern: {dst_pat}")
        else:
            dst_pat = "(dst)"
            dst_id_safe = str(int(edge.dst_node))
        # Both Node: single path pattern
        if isinstance(edge.src_node, Node) and isinstance(edge.dst_node, Node):
            match_clause = f"{src_pat}-[{edge.alias}:{edge.relation}]->{dst_pat}"
            return match_clause, params
        # Both int: WHERE id(src)=... AND id(dst)=...
        if not isinstance(edge.src_node, Node) and not isinstance(edge.dst_node, Node):
            match_clause = (
                f"(src), (dst) WHERE id(src) = {src_id_safe} AND id(dst) = {dst_id_safe} "
                f"MATCH (src)-[{edge.alias}:{edge.relation}]->(dst)"
            )
            return match_clause, params
        # Mixed: one Node, one int
        if isinstance(edge.src_node, Node):
            match_clause = (
                f"{src_pat}, (dst) WHERE id(dst) = {dst_id_safe} "
                f"MATCH ({edge.src_node.alias})-[{edge.alias}:{edge.relation}]->(dst)"
            )
        else:
            match_clause = (
                f"(src), {dst_pat} WHERE id(src) = {src_id_safe} "
                f"MATCH (src)-[{edge.alias}:{edge.relation}]->({edge.dst_node.alias})"
            )
        return match_clause, params

    def delete_edge(self, edge: Edge) -> QueryResult:
        """
        Delete an edge from the graph.

        :param edge: Edge instance to delete
        :return: QueryResult object
        """
        match_clause, params = self._build_edge_match_clause(edge)
        query = f"MATCH {match_clause} DELETE {edge.alias}"
        return self.query(query, params=params)

    def _find_cached_node_by_pk(self, node: Node) -> Node | None:
        """Find a node in the local cache by alias or by primary key."""
        if node.alias in self._nodes:
            return self._nodes[node.alias]
        pk_fields = get_pk_fields(node)
        if not pk_fields:
            return None
        for cached_node in self._nodes.values():
            cached_pk_fields = get_pk_fields(cached_node)
            if cached_pk_fields == pk_fields and all(
                cached_node.properties.get(f) == node.properties.get(f) for f in pk_fields
            ):
                return cached_node
        return None

    def update_node(
        self, node: Node, properties: dict = None, remove: list[str] = None
    ) -> QueryResult:
        """
        Update properties of a node in the graph.

        :param node: Node instance to update
        :param properties: Dictionary of properties to update (optional)
        :param remove: List of property names to remove (optional)
        :return: QueryResult object
        """
        if properties is None:
            properties = {}

        # Ensure node has all required properties for primary key lookup
        pk_fields = get_pk_fields(node)
        for pk in pk_fields:
            if pk not in node.properties:
                logger.warning(
                    f"update_node: Primary key '{pk}' not in node properties. "
                    f"Node properties: {node.properties}"
                )

        # Generate SET clause
        set_clauses = []
        if properties:
            for key, value in properties.items():
                # Include value if it's not None (this includes True, False, 0, empty strings, etc.)
                if value is not None:
                    value_str = format_cypher_value(value)
                    set_clauses.append(f"{node.alias}.{key}={value_str}")

        # Generate REMOVE clause
        remove_clauses = []
        if remove:
            for prop_name in remove:
                remove_clauses.append(f"{node.alias}.{prop_name}")

        if not set_clauses and not remove_clauses:
            raise ValueError("No properties to update or remove")

        # Generate query - use node alias in MATCH to ensure we find the right node
        pk_pattern = node.__str_pk__()
        query_parts = [f"MATCH {pk_pattern}"]

        if set_clauses:
            query_parts.append(f"SET {', '.join(set_clauses)}")

        if remove_clauses:
            query_parts.append(f"REMOVE {', '.join(remove_clauses)}")

        query_parts.append(f"RETURN {node.alias}")
        query = " ".join(query_parts)

        # Log query for debugging
        logger.debug(f"update_node query: {query}")
        logger.debug(f"update_node node properties: {node.properties}")
        logger.debug(f"update_node node __str_pk__: {pk_pattern}")
        if properties:
            logger.debug(f"update_node update properties: {properties}")
        if remove:
            logger.debug(f"update_node remove properties: {remove}")

        # Execute query
        result = self.query(query)

        # Check if node was found and updated
        if hasattr(result, "result_set") and len(result.result_set) == 0:
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
        if hasattr(result, "statistics") and "Properties set" in result.statistics:
            properties_set = result.statistics.get("Properties set", 0)
            if properties_set == 0:
                logger.error(
                    f"update_node: No properties were set. "
                    f"Query: {query}, "
                    f"Node properties: {node.properties}, "
                    f"Update properties: {properties}, "
                    f"Result statistics: {result.statistics}"
                )
            else:
                logger.debug(
                    f"update_node: Successfully set {properties_set} properties"
                )

        # Update local node if it exists in cache
        cached = self._find_cached_node_by_pk(node)
        if cached is not None:
            cached.update(properties)
            logger.debug("update_node: Updated cached node")

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
            # __graph__ is already set by QueryResult.parse_node()
            return found_node
        return None

    def get_edge(self, edge: Edge) -> Edge | None:
        """
        Get an edge from the graph.

        :param edge: Edge instance to search for
        :return: Edge instance if found, None otherwise
        """
        match_clause, params = self._build_edge_match_clause(edge)
        q = f"MATCH {match_clause} RETURN {edge.alias}"
        result = self.query(q, params=params)
        if len(result.result_set) > 0:
            found_edge = result.result_set[0][0]
            found_edge.set_alias(edge.alias)
            return found_edge
        return None

    def create(self, timeout: Optional[int] = None) -> QueryResult:
        """
        Create a new graph in the database.
        Automatically creates indexes for all Node classes with __indexes__ attribute.

        :param timeout: Maximum runtime in milliseconds
        :return: QueryResult object
        """
        result = self._driver.query(CMD.QUERY, self._name, "RETURN 0", timeout=timeout)

        # Automatically create indexes for all Node classes with __indexes__ attribute
        self.create_all_indexes()

        return result

    def create_all_indexes(self) -> None:
        """
        Create all indexes defined in Node classes with __indexes__ attribute.
        """
        from .registry import Registry

        for _name, cls in Registry.__dict__.items():
            if not isinstance(cls, type) or not issubclass(cls, Node) or cls is Node:
                continue
            indexes = getattr(cls, "__indexes__", [])
            if not isinstance(indexes, list):
                continue
            for prop in indexes:
                cls.create_index(prop, self)

    def drop_index(self, label: str, property_name: str) -> QueryResult:
        """
        Drop an index on a property for a node label.

        :param label: Node label
        :param property_name: Property name
        :return: QueryResult object
        """
        query = f"DROP INDEX ON :{label}({property_name})"
        return self.query(query)

    def list_indexes(self) -> list[dict[str, Any]]:
        """
        List all indexes in the graph via FalkorDB db.indexes() procedure.

        :return: List of index information dictionaries (label, properties, type, status)
        """
        try:
            result = self._driver.call_procedure(
                self._name,
                "db.indexes",
                y=["types", "label", "properties", "status"],
                read_only=True,
                graph=self,
            )
            indexes = []
            for row in result.result_set:
                props = row[2] if isinstance(row[2], list) else [row[2]]
                indexes.append({
                    "type": row[0],
                    "label": row[1],
                    "properties": props,
                    "status": row[3],
                })
            return indexes
        except Exception as e:
            msg = str(e).lower()
            if "procedure not found" in msg or "db.indexes" in msg:
                logger.debug("db.indexes() unavailable (old DB version?): %s", e)
                return []
            raise

    def bulk_upsert(
        self, node_class: type[N], data: list[dict[str, Any]], batch_size: int = 1000
    ) -> Optional[QueryResult]:
        """
        Bulk upsert nodes using UNWIND for efficient insertion.

        :param node_class: Node class to upsert
        :param data: List of property dictionaries
        :param batch_size: Number of nodes per batch (default: 1000)
        :return: QueryResult object from last batch
        """
        if not data:
            return None

        # Get label for the node class
        if hasattr(node_class, "__labels__"):
            label = list(node_class.__labels__)[0]
        elif hasattr(node_class, "__label__"):
            label = node_class.__label__
        else:
            label = node_class.__name__

        # Get primary key
        if hasattr(node_class, "__primary_key__"):
            pk = node_class.__primary_key__
            if isinstance(pk, str):
                pk_fields = [pk]
            else:
                pk_fields = pk
        else:
            pk_fields = []

        last_result = None

        # Process in batches
        for i in range(0, len(data), batch_size):
            batch = data[i : i + batch_size]

            # Build UNWIND query
            # Format: UNWIND $nodes AS n MERGE (p:Label {pk1: n.pk1, pk2: n.pk2}) SET p.prop1 = n.prop1, ...

            # Build MERGE pattern with primary key
            if pk_fields:
                merge_pattern_parts = []
                for pk_field in pk_fields:
                    merge_pattern_parts.append(f"{pk_field}: n.{pk_field}")
                merge_pattern = "{" + ", ".join(merge_pattern_parts) + "}"
            else:
                # No primary key - use all properties for MERGE
                merge_pattern = ""

            # Build SET clause for all properties
            set_clauses = []
            # Get all property names from first item in batch (assuming all have same structure)
            if batch:
                all_props = set(batch[0].keys())
                # Exclude primary key fields from SET (they're in MERGE)
                props_to_set = all_props - set(pk_fields)
                for prop in sorted(props_to_set):
                    set_clauses.append(f"p.{prop} = n.{prop}")

            # Build query
            if pk_fields:
                if set_clauses:
                    query = f"UNWIND $nodes AS n MERGE (p:{label} {merge_pattern}) SET {', '.join(set_clauses)}"
                else:
                    query = f"UNWIND $nodes AS n MERGE (p:{label} {merge_pattern})"
            else:
                # No primary key - use CREATE with SET
                if set_clauses:
                    query = f"UNWIND $nodes AS n CREATE (p:{label}) SET {', '.join(set_clauses)}"
                else:
                    # No properties to set - just create nodes
                    query = f"UNWIND $nodes AS n CREATE (p:{label})"

            # Execute with batch data as parameter
            params = {"nodes": batch}
            last_result = self.query(query, params=params)

        return last_result

    def delete(self) -> QueryResult:
        """
        Delete the graph from the database.

        :return: QueryResult object
        """
        return self._driver.query(CMD.DELETE, self._name)

    def transaction(self) -> "Transaction":
        """
        Create a transaction context manager for grouping operations.

        :return: Transaction instance
        """
        return Transaction(self)

    @staticmethod
    def _unpack(result_set: list[list[str]]) -> list[str]:
        result: list[str | None] = [None] * len(result_set)
        for i, l in enumerate(result_set):
            result[i] = l[0]
        return result


class Transaction:
    """
    Transaction context manager for grouping graph operations.

    Automatically flushes changes when exiting the context (if no exception occurred).
    """

    def __init__(self, graph: Graph):
        """
        Initialize transaction.

        :param graph: Graph instance
        """
        self.graph = graph
        self._nodes: list[Node] = []
        self._edges: list[Edge] = []

    def add_node(self, node: Node) -> "Transaction":
        """
        Add a node to the transaction.

        :param node: Node instance to add
        :return: Self for chaining
        """
        self._nodes.append(node)
        return self

    def add_edge(self, edge: Edge) -> "Transaction":
        """
        Add an edge to the transaction.

        :param edge: Edge instance to add
        :return: Self for chaining
        """
        self._edges.append(edge)
        return self

    def flush(self, batch_size: int = 50) -> QueryResult:
        """
        Flush all pending changes in the transaction.

        :param batch_size: Number of items to commit per batch
        :return: QueryResult object
        """
        # Add nodes and edges to graph
        for node in self._nodes:
            self.graph.add_node(node)
        for edge in self._edges:
            self.graph.add_edge(edge)

        # Flush graph
        result = self.graph.flush(batch_size=batch_size)

        # Clear transaction lists
        self._nodes.clear()
        self._edges.clear()

        return result

    def __enter__(self) -> "Transaction":
        """Enter transaction context."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit transaction context, flush if no exception."""
        if exc_type is None:
            self.flush()
