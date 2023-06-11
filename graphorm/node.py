from logging import getLogger
from stringcase import camelcase

from .registry import Registry
from .common import Common
from .utils import quote_string, random_string

logger = getLogger(__file__)


class Node(Common):
    __slots__ = {"__alias__", "__label__", "__primary_key__"}

    __labels__ = None

    def __new__(cls, /, *, _id: int = None, **kwargs) -> Common:
        obj = super().__new__(cls, **kwargs)

        setattr(obj, "__id__", _id)
        setattr(obj, "__alias__", random_string())
        return obj

    def __init_subclass__(cls) -> None:
        setattr(cls, "__label__", camelcase(cls.__name__))

        Registry.add_node_label(cls)

    def set_alias(self, alias: str) -> None:
        setattr(self, "__alias__", alias)

    @property
    def alias(self) -> str:
        return self.__alias__

    @property
    def label(self) -> str:
        return self.__label__

    def merge(self) -> str:
        return "MERGE " + str(self)

    def __str_pk__(self) -> str:
        res = "("
        res += f"{self.__alias__}:{self.__label__}"
        if self.__labels__:
            res += ":"
            res += ":".join(self.__labels__)
        if isinstance(self.__primary_key__, str):
            pk = self.__primary_key__
            res += "{" + f"{pk}:{str(quote_string(self.__dict__[pk]))}" + "}"
        elif isinstance(self.__primary_key__, list):
            res += "{"
            res += ",".join(f"{pk}:{str(quote_string(self.__dict__[pk]))}" for pk in self.__primary_key__)
            res += "}"
        res += ")"
        return res

    def __str__(self) -> str:
        res = self.__str_pk__()
        res += " SET "
        if self.__dict__:
            res += ", ".join(
                f"{self.__alias__}.{k}={str(quote_string(v))}"
                for k, v in sorted(self.__dict__.items()) if v is not None
            )
        return res

    def __eq__(self, rhs):
        # Quick positive check, if both IDs are set.
        if self.id is not None and rhs.id is not None and self.id != rhs.id:
            return False

        # Label should match.
        if self.label != rhs.label:
            return False

        # Quick check for number of properties.
        if len(self.__dict__) != len(rhs.properties):
            return False

        # Compare properties.
        if self.__dict__ != rhs.properties:
            return False

        return True
