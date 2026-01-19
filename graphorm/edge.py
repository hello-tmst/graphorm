import json
from logging import getLogger

from stringcase import camelcase
from .registry import Registry
from .utils import quote_string, random_string
from .node import Node
from .common import Common

logger = getLogger(__file__)


class Edge(Common):
    __slots__ = {"__alias__", "__relation__", "src_node", "dst_node"}

    def __new__(cls, src_node: Node, dst_node: Node, *, _id: int = None, **kwargs) -> Common:
        obj = super().__new__(cls, **kwargs)

        if src_node is None or dst_node is None:
            raise ValueError("Both src_node & dst_node must be provided")

        setattr(obj, "__id__", _id)
        setattr(obj, "__alias__", random_string())
        setattr(obj, "src_node", src_node)
        setattr(obj, "dst_node", dst_node)
        return obj

    def __init_subclass__(cls) -> None:
        setattr(cls, "__relation__", camelcase(cls.__name__))

        Registry.add_edge_relation(cls)

    def set_alias(self, alias: str) -> None:
        """
        Set Node alias.

        :param alias:
        :return:
        """
        setattr(self, "__alias__", alias)

    @property
    def alias(self) -> str:
        return self.__alias__

    @property
    def relation(self) -> str:
        return self.__relation__

    def __str_pk__(self) -> str:
        """
        Generate primary key of Node instance.

        :return:
        """
        res = "["
        res += f"{self.alias}:{self.relation}"
        res += "]"
        return res

    def __str__(self):
        # Source node.
        if isinstance(self.src_node, Node):
            res = f"({str(self.src_node.alias)})"
        else:
            res = "()"

        # Edge
        res += "-["
        res += f"{self.alias}:{self.relation}"
        if self.properties:
            props = ",".join(
                f"{k}:{quote_string(v)}" for k, v in sorted(self.properties.items()) if v is not None
            )
            res += "{" + props + "}"
        res += "]->"

        # Dest node.
        if isinstance(self.dst_node, Node):
            res += f"({str(self.dst_node.alias)})"
        else:
            res += "()"

        return res

    @property
    def properties(self) -> dict:
        return self.__dict__

    def merge(self):
        return "MERGE " + str(self)

    def __eq__(self, rhs):
        # Quick positive check, if both IDs are set.
        if self.id is not None and rhs.id is not None and self.id == rhs.id:
            return True

        # Source and destination nodes should match.
        if self.src_node != rhs.src_node:
            return False

        if self.dst_node != rhs.dst_node:
            return False

        # Relation should match.
        if self.relation != rhs.relation:
            return False

        # Quick check for number of properties.
        if len(self.properties) != len(rhs.properties):
            return False

        # Compare properties.
        if self.properties != rhs.properties:
            return False

        return True

    def __hash__(self) -> int:
        return hash((self.relation, self.src_node, self.dst_node, json.dumps(self.properties)))
