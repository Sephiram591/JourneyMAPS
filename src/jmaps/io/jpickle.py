import pickle
from pathlib import Path
from typing import Any
from jmaps.journey.io import readable, writable

@writable(object)
def pickle_writer(obj: Any, file_path: Path) -> None:
    """Write any Python object using pickle."""
    with file_path.with_suffix(".pkl").open("wb") as f:
        pickle.dump(obj, f, protocol=pickle.HIGHEST_PROTOCOL)

@readable(object)
def pickle_reader(root_cls:str, file_path: Path) -> Any:
    """Read any Python object using pickle."""
    with file_path.with_suffix(".pkl").open("rb") as f:
        return pickle.load(f)