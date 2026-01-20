import redis
from logging import getLogger

from graphorm.drivers.base import Driver
from graphorm.types import CMD
from graphorm.common import Common
from graphorm.query_result import QueryResult
from graphorm.utils import quote_string, stringify_param_value
from graphorm.exceptions import QueryExecutionError, ConnectionError

from graphorm.dialects.cypher import CypherQuery

logger = getLogger(__name__)


class RedisDriver(Driver):
    def __init__(self, host: str, port: int = 6379, password: str = None):
        self.connection = redis.Redis(host, port, password=password)

    def call_procedure(self, graph_name: str, procedure: str, *args, read_only: bool = False, graph=None, **kwagrs) -> QueryResult:
        args = [quote_string(arg) for arg in args]
        q = "CALL {}({})".format(procedure, ",".join(args))

        y = kwagrs.get("y", None)
        if y:
            q += " YIELD %s" % ",".join(y)

        return self.query(CMD.RO_QUERY, graph_name, q, read_only=read_only, graph=graph)

    def query(self, cmd: CMD, graph_name: str,  /, q: str = "", params: dict = None, timeout: int = None, read_only: bool = False, graph=None) -> QueryResult:
        """
        Executes a query against the graph.

        :param cmd: Command type
        :param graph_name: Name of the graph
        :param q: the query
        :param params: query parameters
        :param timeout: maximum runtime for read queries in milliseconds
        :param read_only: executes a readonly query if set to True
        :param graph: Graph instance (optional, for schema access in QueryResult)
        :return: QueryResult
        """

        # GRAPH.DELETE command has different syntax - only takes graph name
        if cmd == CMD.DELETE:
            command = [cmd, graph_name]
        else:
            # maintain original 'q'
            query = q

            # handle query parameters
            if params is not None:
                query = self._build_params_header(params) + query

            # construct query command
            # ask for compact result-set format
            # specify known graph version
            # command = [cmd, self.name, query, "--compact", "version", self.version]
            match (cmd, read_only):
                case (CMD.QUERY, True):
                    cmd = CMD.RO_QUERY
            command = [cmd, graph_name, query, "--compact"]

            # include timeout is specified
            if timeout:
                if not isinstance(timeout, int):
                    raise QueryExecutionError("Timeout argument must be a positive integer")
                command += ["timeout", timeout]

        # issue query
        try:
            response = self.connection.execute_command(*command)
            # Pass graph if provided, otherwise pass self (for backward compatibility)
            # QueryResult now handles simple responses (like from GRAPH.DELETE) correctly
            return QueryResult(graph if graph is not None else self, response)
        except redis.exceptions.ConnectionError as e:
            logger.error(f"Connection error while executing query: {e}")
            raise ConnectionError(f"Failed to connect to Redis: {e}") from e
        except redis.exceptions.TimeoutError as e:
            logger.error(f"Timeout error while executing query: {e}")
            raise QueryExecutionError(f"Query timeout: {e}") from e
        except redis.exceptions.ResponseError as e:
            error_msg = str(e)
            if "wrong number of arguments" in error_msg:
                logger.warning("Note: RedisGraph Python requires server version 2.2.8 or above")
            if "unknown command" in error_msg and read_only:
                # `GRAPH.RO_QUERY` is unavailable in older versions.
                logger.debug("Falling back to non-read-only query")
                return self.query(cmd, graph_name, q, params, timeout, read_only=False, graph=graph)
            logger.error(f"Query execution error: {error_msg}")
            raise QueryExecutionError(f"Query failed: {error_msg}") from e
        except Exception as e:
            logger.error(f"Unexpected error during query execution: {e}", exc_info=True)
            raise QueryExecutionError(f"Unexpected error: {e}") from e

    def commit(self, graph, items: list[Common] = None, batch_size: int = 50) -> QueryResult | None:
        """
        Commit changes to the graph in batches to avoid overwhelming FalkorDB.
        
        Nodes are committed first, then edges, to ensure all nodes exist before creating edges.

        :param graph: Graph instance
        :param items: List of nodes and edges to commit. If None, uses graph's nodes and edges
        :param batch_size: Number of items to commit per batch (default: 50). Set to 0 or negative to disable batching.
        :return: QueryResult | None (returns result of last batch)
        """
        if items is None:
            items = list(graph.nodes.values()) + list(graph.edges.values())
        
        if len(items) == 0:
            return None

        # If batch_size is 0 or negative, commit all at once (original behavior)
        if batch_size <= 0:
            query = " ".join(item.merge() for item in items)
            return self.query(CMD.QUERY, graph.name, query, graph=graph)

        # Separate nodes and edges - edges need nodes to exist first
        nodes = []
        edges = []
        for item in items:
            # Check if item is an edge (has src_node attribute) or node (has labels attribute)
            if hasattr(item, 'src_node') and hasattr(item, 'dst_node'):
                edges.append(item)
            else:
                nodes.append(item)
        
        # Commit nodes first in batches
        last_result = None
        if nodes:
            total_node_batches = (len(nodes) + batch_size - 1) // batch_size
            for i in range(0, len(nodes), batch_size):
                batch = nodes[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                
                try:
                    # For batch_size > 0, execute queries individually to avoid syntax errors
                    # Multiple MERGE statements cannot be simply joined with spaces
                    if batch_size > 0:
                        for item in batch:
                            query = item.merge()
                            logger.debug(
                                f"Committing node in batch {batch_num}/{total_node_batches} "
                                f"to graph {graph.name}: {query[:100]}..."
                            )
                            last_result = self.query(CMD.QUERY, graph.name, query, graph=graph)
                    else:
                        # If batch_size is 0 or negative, join with semicolons (original behavior for compatibility)
                        query = " ".join(item.merge() for item in batch)
                        logger.debug(
                            f"Committing nodes batch {batch_num}/{total_node_batches} "
                            f"({len(batch)} items) to graph {graph.name}"
                        )
                        last_result = self.query(CMD.QUERY, graph.name, query, graph=graph)
                except Exception as e:
                    logger.error(
                        f"Error committing nodes batch {batch_num}/{total_node_batches} "
                        f"({len(batch)} items) to graph {graph.name}: {e}",
                        exc_info=True
                    )
                    continue
        
        # Then commit edges in batches
        if edges:
            total_edge_batches = (len(edges) + batch_size - 1) // batch_size
            for i in range(0, len(edges), batch_size):
                batch = edges[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                
                try:
                    # Commit edges one by one to avoid "node can't be redeclared" errors
                    # when multiple edges reference the same node
                    for edge_idx, edge in enumerate(batch, 1):
                        query = edge.merge()
                        logger.debug(
                            f"Committing edge {edge_idx}/{len(batch)} in batch {batch_num}/{total_edge_batches} "
                            f"to graph {graph.name}"
                        )
                        last_result = self.query(CMD.QUERY, graph.name, query, graph=graph)
                except Exception as e:
                    logger.error(
                        f"Error committing edges batch {batch_num}/{total_edge_batches} "
                        f"({len(batch)} items) to graph {graph.name}: {e}",
                        exc_info=True
                    )
                    continue
        
        if last_result is None and len(items) > 0:
            logger.warning(
                f"All batches failed for graph {graph.name}. "
                f"Total items: {len(items)} (nodes: {len(nodes)}, edges: {len(edges)}), batch_size: {batch_size}"
            )
        
        return last_result

    @staticmethod
    def _build_params_header(params) -> str:
        if not isinstance(params, dict):
            raise TypeError("'params' must be a dict")
        # Header starts with "CYPHER"
        params_header = "CYPHER "
        for key, value in params.items():
            params_header += str(key) + "=" + stringify_param_value(value) + " "
        return params_header

