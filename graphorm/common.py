import inspect
import builtins
from abc import ABCMeta
from typing import Any, Callable

_base_class_defined = False


class CommonMetaclass(ABCMeta):
    __sealed_methods__ = {}

    def __new__(mcs, cls_name: str, bases: tuple[type[Any], ...], namespace: dict[str, Any], **kwargs: Any) -> type:
        global _base_class_defined
        if _base_class_defined:
            base_annotations = {}
            for b in bases:
                base_annotations.update(b.__annotations__)
            if annotations := namespace.get("__annotations__"):
                base_annotations.update(annotations)
            namespace["__annotations__"] = base_annotations
            cls: type[Common] = super().__new__(mcs, cls_name, bases, namespace, **kwargs)  # type: ignore
            setattr(cls, "__init__", _init_fn(cls))
            cls.__doc__ = (cls.__name__ +
                           str(inspect.signature(cls)).replace(' -> None', ''))
            return cls
        else:
            _base_class_defined = True
            return super().__new__(mcs, cls_name, bases, namespace, **kwargs)


class Common(metaclass=CommonMetaclass):
    __slots__ = {"__id__", "__dict__"}

    def __new__(cls, **data: Any) -> "Common":
        obj = super().__new__(cls)
        setattr(obj, "__dict__", cls._validate(data))
        return obj

    @property
    def id(self) -> int:
        return self.__id__

    def update(self, data) -> None:
        self.__dict__.update(data)

    @classmethod
    def _validate(cls, data) -> dict:
        values = {}
        for key in cls.__annotations__:
            if (value := data.get(key, cls.__dict__.get(key))) is not None:
                values[key] = value
        return values


def _init_fn(cls) -> Callable:
    args = ["self", "*args", "_id=None"]
    body = []
    for key, value in cls.__annotations__.items():
        args.append(f"{key}: {value.__name__} = None")
        body.append(f"  self.{key} = {key}")
    body = "\n".join(body) or "  pass"
    args = ", ".join(args)
    txt = f' def __init__({args}) -> None:\n{body}'

    _locals = {"BUILTINS": builtins}

    local_vars = ', '.join(_locals.keys())
    txt = f"def __create_fn__({local_vars}):\n{txt}\n return __init__"
    ns = {}
    exec(txt, None, ns)
    return ns['__create_fn__'](**_locals)
