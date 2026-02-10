# io_registry.py

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
    _WRITERS[cls] = writer
    _RESOLVED_WRITERS[cls] = writer
    _READERS[cls] = reader
    
def write(obj: Any, file_path: Path) -> None:
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
    try:
        fn = _READERS[cls]
    except KeyError:
        raise TypeError(f"No reader registered for {cls!r}")
    return fn(file_path)


def writable(cls: type):
    def decorator(writer_fn: Callable[[Any, Path], None]):
        _WRITERS[cls] = writer_fn
        return writer_fn
    return decorator


def readable(cls: type):
    def decorator(reader_fn: Callable[[Path], Any]):
        _READERS[cls] = reader_fn
        return reader_fn
    return decorator
