import random
import string


def random_string(length: int = 10) -> str:
    return "".join(random.choice(string.ascii_lowercase) for _ in range(length))  # nosec


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
        return f'{{{",".join(f"{k}:{format_cypher_value(v)}" for k, v in value.items())}}}'
    else:
        return str(value)


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
