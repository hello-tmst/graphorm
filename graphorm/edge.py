from stringcase import camelcase

from .registry import Registry
from .utils import quote_string
from .node import CommonNode
from .common import Common


class CommonEdge(Common):
    __slots__ = {"src_node", "dst_node", "__relation__"}

    def __init__(self, src_node, dst_node, _id=None, **data):
        if src_node is None or dst_node is None:
            raise ValueError("Both src_node & dst_node must be provided")

        if _id:
            self.__id__ = _id
        self.src_node = src_node
        self.dst_node = dst_node

        for key, value in data.items():
            setattr(self, key, value)

    def __init_subclass__(cls) -> None:
        cls.__relation__ = camelcase(cls.__name__)

        Registry.add_edge_relation(cls)

    @property
    def relation(self) -> str:
        return self.__relation__

    def __str__(self):
        # Source node.
        if isinstance(self.src_node, CommonNode):
            res = f"({str(self.src_node.alias)})"
        else:
            res = "()"

        # Edge
        res += "-["
        res += ":" + self.__relation__
        if self.__dict__:
            props = ",".join(
                key + ":" + str(quote_string(val)) for key, val in sorted(self.__dict__.items())
            )
            res += "{" + props + "}"
        res += "]->"

        # Dest node.
        if isinstance(self.dst_node, CommonNode):
            res += f"({str(self.dst_node.alias)})"
        else:
            res += "()"

        return res

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
        if len(self.__dict__) != len(rhs.properties):
            return False

        # Compare properties.
        if self.__dict__ != rhs.properties:
            return False

        return True
