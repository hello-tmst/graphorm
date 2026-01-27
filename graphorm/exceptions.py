"""Custom exceptions for GraphORM."""


class GraphORMError(Exception):
    """Base exception for all GraphORM errors."""

    pass


class NodeNotFoundError(GraphORMError):
    """Raised when a node is not found in the graph."""

    pass


class EdgeNotFoundError(GraphORMError):
    """Raised when an edge is not found in the graph."""

    pass


class QueryExecutionError(GraphORMError):
    """Raised when a query execution fails."""

    pass


class ConnectionError(GraphORMError):
    """Raised when there's an error connecting to the database."""

    pass
