from stringcase import camelcase

from .registry import Registry
from .common import Common
from .utils import quote_string, random_string


class CommonNode(Common):
    __slots__ = {"__alias__", "__label__", "__primary_key__"}

    def __init__(self, _id=None, **data) -> None:
        if _id:
            self.__id__ = _id

        self.__alias__ = random_string()

        for key, value in data.items():
            setattr(self, key, value)

    def __init_subclass__(cls) -> None:
        cls.__label__ = camelcase(cls.__name__)

        Registry.add_node_label(cls)

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
        res += self.__alias__
        res += f":{self.__label__}"
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
                for k, v in sorted(self.__dict__.items())
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
