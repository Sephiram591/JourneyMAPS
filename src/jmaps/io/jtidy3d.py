import tidy3d as td
from pathlib import Path
from typing import Any
from jmaps.journey.io import readable, writable
import importlib

def load_object(path: str):
    parts = path.split(".")
    
    # Find the longest valid module prefix
    for i in range(len(parts), 0, -1):
        module_name = ".".join(parts[:i])
        try:
            module = importlib.import_module(module_name)
            break
        except ModuleNotFoundError:
            continue
    else:
        raise ImportError(f"Cannot import any module from {path}")

    obj = module
    for attr in parts[i:]:
        obj = getattr(obj, attr)

    return obj

@writable(td.components.base.Tidy3dBaseModel)
def tidy3d_writer(obj: td.components.base.Tidy3dBaseModel, file_path: Path) -> None:
    """Write any Python object using pickle."""
    obj.to_file(str(file_path.with_suffix(".hdf5")))

@readable(td.components.base.Tidy3dBaseModel)
def tidy3d_reader(root_cls:str, file_path: Path) -> Any:
    """Read any Python object using pickle."""
    return load_module(root_cls).from_file(str(file_path.with_suffix(".hdf5")))