import json
from logging import getLogger
from stringcase import camelcase

from .registry import Registry
from .common import Common
from .utils import quote_string, random_string

logger = getLogger(__file__)


class Node(Common):
    __slots__ = {"__alias__", "__primary_key__", "__labels__"}

    def __new__(cls, /, *, _id: int = None, **kwargs) -> Common:
        """
        Create new instance of Node.

        :param _id:
        :param kwargs:
        """
        obj = super().__new__(cls, **kwargs)

        setattr(obj, "__id__", _id)
        setattr(obj, "__alias__", random_string())
        return obj

    def __init_subclass__(cls) -> None:
        setattr(cls, "__labels__", {camelcase(cls.__name__)})

        Registry.add_node_label(cls)

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
    def labels(self) -> set[str]:
        return self.__labels__

    @property
    def properties(self) -> dict:
        return self.__dict__

    def merge(self) -> str:
        return "MERGE " + str(self)

    def __str_pk__(self) -> str:
        """
        Generate primary key of Node instance.

        :return:
        """
        res = "("
        res += f"{self.alias}"
        if self.labels:
            res += ":"
            res += ":".join(self.labels)
        if isinstance(self.__primary_key__, str):
            pk = self.__primary_key__
            res += "{" + f"{pk}:{str(quote_string(self.properties[pk]))}" + "}"
        elif isinstance(self.__primary_key__, list):
            props = ",".join(f"{pk}:{str(quote_string(self.properties[pk]))}" for pk in self.__primary_key__)
            res += "{" + props + "}"
        res += ")"
        return res

    def __str__(self) -> str:
        """
        Generate Node instance insertion constraint.

        :return:
        """

        res = self.__str_pk__()
        res += " SET "
        if self.properties:
            res += ", ".join(
                f"{self.alias}.{k}={str(quote_string(v))}"
                for k, v in sorted(self.properties.items()) if v is not None
            )
        return res

    def __eq__(self, rhs):
        # Quick positive check, if both IDs are set.
        if self.id is not None and rhs.id is not None and self.id != rhs.id:
            return False

        # Label should match.
        if set(self.labels) ^ set(rhs.labels):
            return False

        # Quick check for number of properties.
        if len(self.properties) != len(rhs.properties):
            return False

        # Compare properties.
        if self.properties != rhs.properties:
            return False

        return True

    def __hash__(self) -> int:
        return hash((frozenset(self.labels), json.dumps(self.properties)))
