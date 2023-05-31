from abc import ABCMeta
from typing import Any

_base_class_defined = False


class CommonMetaclass(ABCMeta):
    __sealed_methods__ = {}

    def __new__(mcs, cls_name: str, bases: tuple[type[Any], ...], namespace: dict[str, Any], **kwargs: Any) -> type:
        global _base_class_defined
        if _base_class_defined:
            for key in namespace:
                for b in bases:
                    try:
                        if key in mcs.__sealed_methods__:
                            raise TypeError(
                                "*%s.%s* is *sealed* and therefore may not be overriden in descendants" % (
                                    b.__name__, key))
                    except AttributeError:
                        continue
            cls: type[Common] = super().__new__(mcs, cls_name, bases, namespace, **kwargs)  # type: ignore
            return cls
        else:
            _base_class_defined = True
            return super().__new__(mcs, cls_name, bases, namespace, **kwargs)


class Common(metaclass=CommonMetaclass):
    __slots__ = {"__id__", "__dict__"}

    def __new__(cls, *args, **data: Any) -> "Common":
        values = cls._validate(data)
        obj = super().__new__(cls)
        setattr(obj, "__dict__", values)
        return obj

    def __init_subclass__(cls) -> None:
        if Common in cls.__bases__:
            _check_not_declarative(cls, Common)
        super().__init_subclass__()

    @property
    def id(self):
        return self.__id__

    def update(self, data):
        self.__dict__.update(data)

    @classmethod
    def _validate(cls, data):
        values = {}

        for key in cls.__annotations__:
            if (value := data.get(key, cls.__dict__.get(key))) is not None:
                values[key] = value
        return values


def _check_not_declarative(cls: type[Any], base: type[Any]) -> None:
    cls_dict = cls.__dict__
    __primary_key__ = cls_dict.get("__primary_key__")
    if __primary_key__ is None:
        return
    elif isinstance(__primary_key__, str) and __primary_key__ not in cls_dict["__annotations__"]:
        raise ValueError("<__primary_key__> field is not implemented")
    elif isinstance(__primary_key__, list | tuple) and not set(__primary_key__) <= set(cls_dict["__annotations__"]):
        raise ValueError("<__primary_key__> fields is not implemented")
