from enum import Enum


class CMD(str, Enum):
    QUERY = "GRAPH.QUERY"
    RO_QUERY = "GRAPH.RO_QUERY"
    DELETE = "GRAPH.DELETE"
