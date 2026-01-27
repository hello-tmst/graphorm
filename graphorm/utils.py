import random
import string
from typing import List


def random_string(length: int = 10) -> str:
    return "".join(
        random.choice(string.ascii_lowercase) for _ in range(length)
    )  # nosec


def quote_string(v):
    if isinstance(v, bytes):
        v = v.decode()
    elif not isinstance(v, str):
        return v
    if len(v) == 0:
        return '""'

    v = v.replace("\\", "\\\\")
    v = v.replace('"', '\\"')

    return '"{}"'.format(v)


def format_cypher_value(value):
    """
    Format a value for use in Cypher queries.
    Handles strings, booleans, numbers, None, lists, and dicts.

    :param value: Value to format
    :return: Formatted string for Cypher
    """
    if isinstance(value, bool):
        return "true" if value else "false"
    elif isinstance(value, str):
        return quote_string(value)
    elif value is None:
        return "null"
    elif isinstance(value, (list, tuple)):
        return f'[{",".join(map(format_cypher_value, value))}]'
    elif isinstance(value, dict):
        return (
            f'{{{",".join(f"{k}:{format_cypher_value(v)}" for k, v in value.items())}}}'
        )
    else:
        return str(value)


def get_pk_fields(obj) -> List[str]:
    """
    Return primary key field names as a list from obj.__primary_key__.
    :param obj: Node or similar with __primary_key__ (str or list)
    :return: list of field names
    """
    pk = getattr(obj, "__primary_key__", None)
    if pk is None:
        return []
    if isinstance(pk, str):
        return [pk]
    if isinstance(pk, list):
        return list(pk)
    return []


def format_pk_cypher_map(obj) -> str:
    """
    Format primary key properties as Cypher map string "{ pk1: value1, pk2: value2 }".
    Uses obj.properties and format_cypher_value. Does not include alias/labels.
    :param obj: Node or similar with __primary_key__ and .properties
    :return: Cypher map string or empty string if no pk
    """
    fields = get_pk_fields(obj)
    if not fields:
        return ""
    props = getattr(obj, "properties", None)
    if props is None:
        return ""
    parts = []
    for k in fields:
        v = (
            props.get(k)
            if hasattr(props, "get")
            else (props[k] if k in props else None)
        )
        parts.append(f"{k}:{format_cypher_value(v)}")
    return "{" + ", ".join(parts) + "}"


def stringify_param_value(value):
    if isinstance(value, str):
        return quote_string(value)
    elif value is None:
        return "null"
    elif isinstance(value, (list, tuple)):
        return f'[{",".join(map(stringify_param_value, value))}]'
    elif isinstance(value, dict):
        return f'{{{",".join(f"{k}:{stringify_param_value(v)}" for k, v in value.items())}}}'
    else:
        return str(value)
