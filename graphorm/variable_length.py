"""
Variable-length path descriptor for MATCH patterns.

Used in select().match((src, EdgeClass.variable_length(min_hops, max_hops), dst))
to generate Cypher like (a)-[:REL*1..3]->(b) without raw strings.
"""

from typing import Any


class VariableLength:
    """
    Descriptor for variable-length relationship patterns in MATCH.

    Use via Edge.variable_length(min_hops, max_hops) or VariableLength(EdgeClass, min_hops, max_hops).
    """

    __slots__ = ("edge_class", "min_hops", "max_hops")

    def __init__(
        self,
        edge_class: type,
        min_hops: int | None = None,
        max_hops: int | None = None,
    ) -> None:
        """
        :param edge_class: Edge subclass (must have __relation__).
        :param min_hops: Minimum number of hops (None = unbounded lower).
        :param max_hops: Maximum number of hops (None = unbounded upper).
        """
        if min_hops is not None and min_hops < 0:
            raise ValueError("min_hops must be >= 0")
        if max_hops is not None and max_hops < 0:
            raise ValueError("max_hops must be >= 0")
        if min_hops is not None and max_hops is not None and min_hops > max_hops:
            raise ValueError("min_hops must be <= max_hops")

        self.edge_class: type = edge_class
        self.min_hops: int | None = min_hops
        self.max_hops: int | None = max_hops
