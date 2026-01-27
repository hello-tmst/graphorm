import builtins
import inspect
from abc import ABCMeta
from collections.abc import Callable
from typing import (
    Any,
)

from .properties import (
    DefaultPropertiesValidator,
    PropertiesManager,
)

_base_class_defined = False

COMMON_INTERNAL_KEYS = frozenset(
    {
        "__id__",
        "__alias__",
        "__graph__",
        "__relations__",
        "__primary_key__",
        "__labels__",
        "__relation__",
        "src_node",
        "dst_node",
        "_properties_manager",
    }
)


class CommonMetaclass(ABCMeta):
    __sealed_methods__ = {}

    def __new__(
        mcs,
        cls_name: str,
        bases: tuple[type[Any], ...],
        namespace: dict[str, Any],
        **kwargs: Any,
    ) -> type:
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
            cls.__doc__ = cls.__name__ + str(inspect.signature(cls)).replace(
                " -> None", ""
            )
            return cls
        else:
            _base_class_defined = True
            return super().__new__(mcs, cls_name, bases, namespace, **kwargs)


class _AliasDescriptor:
    """Descriptor that handles both classmethod and instance property for alias."""

    def __get__(self, obj: Any, owner: type) -> Any:
        if obj is None:
            return owner._alias_classmethod
        return obj.__alias__


class Common(metaclass=CommonMetaclass):
    __slots__ = {"__id__", "__dict__", "_properties_manager"}
    alias = _AliasDescriptor()

    def __new__(cls, **data: Any) -> "Common":
        obj = super().__new__(cls)
        validated_data = cls._validate(data)
        setattr(obj, "__dict__", validated_data)

        # Initialize PropertiesManager after __dict__ is set
        obj._init_properties_manager()
        return obj

    def _init_properties_manager(self) -> None:
        """Initialize PropertiesManager from current __dict__."""
        internal_keys = set(COMMON_INTERNAL_KEYS)
        # Extract only user-defined properties from __dict__
        properties_data = {
            k: v for k, v in self.__dict__.items() if k not in internal_keys
        }

        # Create validator with class annotations
        validator = DefaultPropertiesValidator(self.__class__.__annotations__)

        # Initialize PropertiesManager
        properties_manager = PropertiesManager(
            initial_data=properties_data,
            validator=validator,
            internal_keys=internal_keys,
        )
        setattr(self, "_properties_manager", properties_manager)

    @property
    def id(self) -> int:
        return self.__id__

    def update(self, data) -> None:
        """
        Update properties using PropertiesManager.

        This method updates both PropertiesManager and __dict__ to maintain
        backward compatibility.
        """
        if hasattr(self, "_properties_manager"):
            # Update PropertiesManager (with validation)
            self._properties_manager.update(data)
            # Sync to __dict__ for backward compatibility
            for key, value in data.items():
                if key not in self._properties_manager._internal_keys:
                    self.__dict__[key] = value
        else:
            # Fallback for objects created before PropertiesManager integration
            self.__dict__.update(data)
            # Try to initialize PropertiesManager if it doesn't exist
            if not hasattr(self, "_properties_manager"):
                self._init_properties_manager()

    @property
    def properties(self) -> dict:
        """
        Get properties as a dictionary.

        This property returns a view of properties managed by PropertiesManager,
        excluding internal attributes for clean separation.

        If PropertiesManager doesn't exist or needs sync, it will be initialized/updated.
        """
        if not hasattr(self, "_properties_manager"):
            self._init_properties_manager()
        else:
            # Sync PropertiesManager with current __dict__ to ensure consistency
            # This handles cases where attributes were set after PropertiesManager initialization
            internal_keys = self._properties_manager._internal_keys
            current_props = {
                k: v for k, v in self.__dict__.items() if k not in internal_keys
            }
            # Update PropertiesManager with any new properties from __dict__
            for key, value in current_props.items():
                if key not in self._properties_manager:
                    self._properties_manager.set(key, value)

        return self._properties_manager.items()

    @classmethod
    def _validate(cls, data) -> dict:
        values = {}
        for key in cls.__annotations__:
            # Check if key is explicitly provided in data
            if key in data:
                value = data[key]
                # Include value if it's not None (this includes False, 0, empty strings, etc.)
                # False is not None, so it will be included
                if value is not None:
                    values[key] = value
            else:
                # Use default from class dict if key not in data
                default_value = cls.__dict__.get(key)
                # Include default if it's not None (this includes False)
                if default_value is not None:
                    values[key] = default_value
        return values


def _init_fn(cls) -> Callable:
    args = ["self", "*args", "_id=None"]
    body = []
    for key, value in cls.__annotations__.items():
        args.append(f"{key}: {value.__name__} = None")
        body.append(f"  self.{key} = {key}")
    body = "\n".join(body) or "  pass"
    args = ", ".join(args)
    txt = f" def __init__({args}) -> None:\n{body}"

    _locals = {"BUILTINS": builtins}

    local_vars = ", ".join(_locals.keys())
    txt = f"def __create_fn__({local_vars}):\n{txt}\n return __init__"
    ns = {}
    exec(txt, None, ns)
    return ns["__create_fn__"](**_locals)
