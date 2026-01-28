# serial.py
from typing import Any, Callable, Type
import orjson

_SERIALIZERS: dict[type, Callable[[Any], Any]] = {}
_DESERIALIZERS: dict[type, Callable[[Any], Any]] = {}


def register(
    cls: type,
    *,
    serializer: Callable[[Any], Any],
    deserializer: Callable[[Any], Any],
) -> None:
    _SERIALIZERS[cls] = serializer
    _DESERIALIZERS[cls] = deserializer


def serialize_default(obj):
    try:
        fn = _SERIALIZERS[type(obj)]
    except KeyError:
        raise TypeError(f"No serializer registered for {type(obj)!r}")
    return fn(obj)


def deserialize(cls: type, data):
    try:
        fn = _DESERIALIZERS[cls]
    except KeyError:
        raise TypeError(f"No deserializer registered for {cls!r}")
    return fn(data)

def serializable(cls: type):
    def decorator(serializer_fn):
        _SERIALIZERS[cls] = serializer_fn
        return serializer_fn
    return decorator


def deserializable(cls: type):
    def decorator(deserializer_fn):
        _DESERIALIZERS[cls] = deserializer_fn
        return deserializer_fn
    return decorator
