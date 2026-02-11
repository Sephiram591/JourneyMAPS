"""Type-based IO registry for JourneyMAPS.

This module provides a minimal registry that maps Python types to callables
capable of writing/reading instances of those types to/from disk. It is used
by :class:`~jmaps.journey.path.PathResult` to persist file-based results.
"""

from pathlib import Path
from typing import Any, Callable

_WRITERS: dict[type, Callable[[Any, Path], None]] = {}
_READERS: dict[type, Callable[[Path], Any]] = {}
_RESOLVED_WRITERS: dict[type, Callable[[Any, Path], None]] = {}


def register(
    cls: type,
    *,
    writer: Callable[[Any, Path], None],
    reader: Callable[[Path], Any],
) -> None:
    """Register reader and writer functions for a type.

    Args:
        cls: Type whose instances can be written and read.
        writer: Callable that serializes ``cls`` instances to ``file_path``.
        reader: Callable that deserializes an instance of ``cls`` from ``file_path``.
    """
    _WRITERS[cls] = writer
    _RESOLVED_WRITERS[cls] = writer
    _READERS[cls] = reader


def write(obj: Any, file_path: Path) -> type:
    """Write an object to disk using the best registered writer.

    Resolution walks the method-resolution-order (MRO) of ``type(obj)`` so that
    writers registered on parent classes are reused for subclasses.

    Args:
        obj: Object instance to serialize.
        file_path: Path to the target file.

    Returns:
        type: The type on which the writer was originally registered.

    Raises:
        TypeError: If no writer is registered for the object's type or its parents.
    """
    cls = type(obj)

    try:
        fn = _RESOLVED_WRITERS[cls]
    except KeyError:
        for typ in cls.__mro__:
            if typ in _WRITERS:
                fn = _WRITERS[typ]
                _RESOLVED_WRITERS[cls] = fn
                cls = typ
                break
        else:
            raise TypeError(
                f"No writer registered for {cls!r} or its parent classes"
            )

    fn(obj, file_path)
    return cls


def read(cls: type, file_path: Path) -> Any:
    """Read an object of the given type from disk.

    Args:
        cls: Type to read.
        file_path: Path to the source file.

    Returns:
        Any: Deserialized instance of ``cls``.

    Raises:
        TypeError: If no reader is registered for ``cls``.
    """
    try:
        fn = _READERS[cls]
    except KeyError:
        raise TypeError(f"No reader registered for {cls!r}")
    return fn(file_path)


def writable(cls: type):
    """Decorator registering a function as the writer for ``cls``.

    The decorated function must accept ``(obj, file_path)``.

    Args:
        cls: Type whose instances will be written by the decorated function.

    Returns:
        Callable: Decorator that registers the given writer function.
    """

    def decorator(writer_fn: Callable[[Any, Path], None]):
        _WRITERS[cls] = writer_fn
        return writer_fn

    return decorator


def readable(cls: type):
    """Decorator registering a function as the reader for ``cls``.

    The decorated function must accept ``(file_path)`` and return an instance
    of ``cls``.

    Args:
        cls: Type whose instances will be reconstructed by the decorated function.

    Returns:
        Callable: Decorator that registers the given reader function.
    """

    def decorator(reader_fn: Callable[[Path], Any]):
        _READERS[cls] = reader_fn
        return reader_fn

    return decorator
