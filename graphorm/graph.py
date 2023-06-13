import redis
from logging import getLogger

from .query_result import QueryResult
from .utils import quote_string
from .utils import stringify_param_value
from .node import Node
from .edge import Edge

logger = getLogger(__file__)


class Graph:
    def __init__(self, name, redis_con=None, host="localhost", port=6379, password=None):
        """
        Create a new graph.

        Args:
            name: string that represents the name of the graph
            redis_con: connection to Redis
        """
        self.name = name  # Graph key
        if redis_con is not None:
            self.redis_con = redis_con
        else:
            self.redis_con = redis.Redis(host, port, password=password)

        self.nodes = {}
        self.edges = []
        self._labels = []  # List of node labels.
        self._properties = []  # List of properties.
        self._relationshipTypes = []  # List of relation types.
        self.version = 0  # Graph version

    def _clear_schema(self):
        self._labels = []
        self._properties = []
        self._relationshipTypes = []

    def _refresh_labels(self):
        lbls = self.labels()

        # Unpack data.
        self._labels = [None] * len(lbls)
        for i, l in enumerate(lbls):
            self._labels[i] = l[0]

    def _refresh_attributes(self):
        props = self.property_keys()

        # Unpack data.
        self._properties = [None] * len(props)
        for i, p in enumerate(props):
            self._properties[i] = p[0]

    def get_label(self, idx):
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

    def get_property(self, idx):
        """
        Returns a property by it's index.

        :param idx: The index of the property
        :return:
        """
        try:
            propertie = self._properties[idx]
        except IndexError:
            # Refresh properties.
            self._refresh_attributes()
            propertie = self._properties[idx]
        return propertie

    def add_node(self, node: Node, update: bool = False):
        """
        Adds a node to the graph.

        :param node:
        :param update:
        :return:
        """
        if self.get_node(node):
            if update:
                self.nodes[node.alias] = node
                return 1
            return 0
        else:
            self.nodes[node.alias] = node
            return 1

    def get_node(self, node: Node) -> Node | None:
        """
        Get node from the graph.

        :param node: The instance of node
        :return: Node, if it in the graph, else None
        """
        result = self.query(f"MATCH {node.__str_pk__()} RETURN {node.alias}").result_set
        if len(result) > 0:
            _node = result[0][0]
            _node.set_alias(node.alias)
            return _node

    def update_node(self, node: Node, properties: dict):
        """
        Updates a node to the graph.

        :param node:
        :param properties:
        :return:
        """
        node.update(properties)
        self.add_node(node, update=True)

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
        self.edges.append(edge)

    def commit(self):
        """
        Create entire graph.

        :return:
        """
        if len(self.nodes) == 0 and len(self.edges) == 0:
            return None

        query = ""
        query += " ".join(node.merge() for node in self.nodes.values()) + " "
        query += " ".join(edge.merge() for edge in self.edges)

        return self.query(query)

    def flush(self):
        """
        Commit the graph and reset the edges and nodes to zero length.
        """
        self.commit()
        self.nodes = {}
        self.edges = []

    @staticmethod
    def _build_params_header(params):
        if not isinstance(params, dict):
            raise TypeError("'params' must be a dict")
        # Header starts with "CYPHER"
        params_header = "CYPHER "
        for key, value in params.items():
            params_header += str(key) + "=" + stringify_param_value(value) + " "
        return params_header

    def create(self, timeout=None):
        return self.query("return 0", timeout=timeout)

    def delete(self):
        """
        Deletes graph.
        """
        self._clear_schema()
        return self.redis_con.execute_command("GRAPH.DELETE", self.name)

    def query(self, q, params=None, timeout=None, read_only=False):
        """
        Executes a query against the graph.

        Args:
            q: the query
            params: query parameters
            timeout: maximum runtime for read queries in milliseconds
            read_only: executes a readonly query if set to True
        """

        # maintain original 'q'
        query = q

        # handle query parameters
        if params is not None:
            query = self._build_params_header(params) + query

        # construct query command
        # ask for compact result-set format
        # specify known graph version
        cmd = "GRAPH.RO_QUERY" if read_only else "GRAPH.QUERY"
        # command = [cmd, self.name, query, "--compact", "version", self.version]
        command = [cmd, self.name, query, "--compact"]

        # include timeout is specified
        if timeout:
            if not isinstance(timeout, int):
                raise Exception("Timeout argument must be a positive integer")
            command += ["timeout", timeout]

        # issue query
        try:
            response = self.redis_con.execute_command(*command)
            return QueryResult(self, response)
        except redis.exceptions.ResponseError as e:
            if "wrong number of arguments" in str(e):
                print("Note: RedisGraph Python requires server version 2.2.8 or above")
            if "unknown command" in str(e) and read_only:
                # `GRAPH.RO_QUERY` is unavailable in older versions.
                return self.query(q, params, timeout, read_only=False)
            raise e

    def call_procedure(self, procedure, *args, read_only=False, **kwagrs):
        args = [quote_string(arg) for arg in args]
        q = "CALL {}({})".format(procedure, ",".join(args))

        y = kwagrs.get("y", None)
        if y:
            q += " YIELD %s" % ",".join(y)

        return self.query(q, read_only=read_only)

    def labels(self):
        return self.call_procedure("db.labels", read_only=True).result_set

    def property_keys(self):
        return self.call_procedure("db.propertyKeys", read_only=True).result_set
