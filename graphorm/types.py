from enum import StrEnum


class CMD(StrEnum):
    QUERY = "GRAPH.QUERY"
    RO_QUERY = "GRAPH.RO_QUERY"
    DELETE = "GRAPH.DELETE"
