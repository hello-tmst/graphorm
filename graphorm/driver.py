import redis

from .types import CMD
from .graph import Graph
from .query_result import QueryResult
from .utils import quote_string, stringify_param_value


class Driver:
    def __init__(self, host: str, port: int = 6379, password: str = None):
        self.connection = redis.Redis(host, port, password=password)

    def call_procedure(self, graph: Graph, procedure: str, *args, read_only: bool = False, **kwagrs) -> QueryResult:
        args = [quote_string(arg) for arg in args]
        q = "CALL {}({})".format(procedure, ",".join(args))

        y = kwagrs.get("y", None)
        if y:
            q += " YIELD %s" % ",".join(y)

        return self.query(CMD.RO_QUERY, graph, q, read_only=read_only)

    def query(self, cmd: CMD, graph: Graph,  /, q: str = "", params: dict = None, timeout: int = None, read_only: bool = False) -> QueryResult:
        """
        Executes a query against the graph.

        :param cmd:
        :param q: the query
        :param graph: the graph object
        :param params: query parameters
        :param timeout: maximum runtime for read queries in milliseconds
        :param read_only: executes a readonly query if set to True
        :return:
        """

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
        command = [cmd, graph.name, query, "--compact"]

        # include timeout is specified
        if timeout:
            if not isinstance(timeout, int):
                raise Exception("Timeout argument must be a positive integer")
            command += ["timeout", timeout]

        # issue query
        try:
            response = self.connection.execute_command(*command)
            return QueryResult(self, response)
        except redis.exceptions.ResponseError as e:
            if "wrong number of arguments" in str(e):
                print("Note: RedisGraph Python requires server version 2.2.8 or above")
            if "unknown command" in str(e) and read_only:
                # `GRAPH.RO_QUERY` is unavailable in older versions.
                return self.query(cmd, graph, q, params, timeout, read_only=False)
            raise e

    @staticmethod
    def _build_params_header(params) -> str:
        if not isinstance(params, dict):
            raise TypeError("'params' must be a dict")
        # Header starts with "CYPHER"
        params_header = "CYPHER "
        for key, value in params.items():
            params_header += str(key) + "=" + stringify_param_value(value) + " "
        return params_header

    def commit(self, graph: Graph) -> QueryResult | None:
        """
        Create entire graph.

        :return: QueryResult | None
        """
        if len(graph.nodes) == 0 and len(graph.edges) == 0:
            return None

        query = " ".join(item.merge() for item in [*graph.nodes.values(), *graph.edges.values()])

        return self.query(CMD.QUERY, graph, query)


class GraphMixin(Driver):
    def create(self, graph: Graph, timeout: int = None) -> QueryResult:
        """
        Create a new graph.

        :param graph:
        :param timeout:
        :return QueryResult:
        """
        return self.query(CMD.QUERY, graph, "RETURN 0", timeout=timeout)

    def delete(self, graph: Graph):
        """
        Delete a graph.

        :param graph:
        :return:
        """
        return self.query(CMD.DELETE, graph)
