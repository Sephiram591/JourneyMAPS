from __future__ import annotations

import importlib
import inspect
from functools import update_wrapper
from typing import Any, Callable, Generic, ParamSpec, TypeVar, Dict, Tuple, Protocol, overload, Hashable
from collections.abc import Mapping, Sequence
from pydantic import BaseModel, Field
from pathlib import Path
from datetime import datetime, timezone
from jmaps.journey.io import read, write
from jmaps.journey.containers import (
    ContainerDecodingError,
    DEFAULT_CONTAINER_REGISTRY,
    MISSING_ENV_VALUE,
    contains_missing_environment_values,
    deserialize_reduced_environment,
    register_container_adapter,
    serialize_reduced_environment,
)
from jmaps.journey.jmalc import (
    is_sql_type,
    create_tables,
    DBPath,
    DBPathVersion,
    DBResult,
)
from sqlalchemy.orm import Session, scoped_session, sessionmaker
from sqlalchemy import select, Null, create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import PendingRollbackError
import json
import hashlib
from jmaps.journey.schema import (
    normalized_function_ast,
    analyze_env_schema,
    FunctionCall,
    EnvPath,
)

P = ParamSpec("P")
R = TypeVar("R")

db_session: scoped_session[Session] = None
engine: Engine = None
jpath_registry: Dict[str, JPath] = {}
result_directory: str = None

def init_db(engine_arg:str | Engine, res_directory:str) -> None:
    """Initialize the database session and create tables if they do not exist.

    Args:
        engine (str | Engine): SQLAlchemy engine or connection string.

    """
    global db_session
    global engine
    global result_directory
    if isinstance(engine_arg, str):
        engine = create_engine(engine_arg)
    create_tables(engine)
    SessionFactory = sessionmaker(bind=engine)
    db_session = scoped_session(SessionFactory)
    result_directory = res_directory

def get_filename(hashable: dict) -> str:
    """Compute a deterministic key from a JSON-serializable mapping.

    The key is currently a SHA256 hash of the canonical JSON representation and
    is used to derive cache file names for path results.

    Args:
        hashable: JSON-serializable mapping (typically environment SQL data).

    Returns:
        str: Hex-encoded SHA256 digest.
    """
    dumped = json.dumps(hashable, sort_keys=True, separators=(",", ":"))
    key = hashlib.sha256(dumped.encode("utf-8")).hexdigest()
    return str(key)

class JBuffer(dict):
    """Deferred callable whose argument values participate in env schemas.

    ``args`` and ``kwargs`` are ordinary contained data. The callable reference,
    argument ordering, and variadic-argument information are structural and are
    persisted only in ``env_schema`` by the externally registered adapter below.
    """

    _DATA_KEYS = ("args", "kwargs")

    def __init__(self, jvar: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        """Bind ``args`` and ``kwargs`` to an importable callable.

        Parameters
        ----------
        jvar
            Module-level callable to invoke. It must be recoverable from its
            ``__module__`` and ``__qualname__`` so a persisted environment can
            reconstruct the buffer in a later process.
        *args, **kwargs
            Values supplied to ``jvar``. They may contain nested ``JBuffer``
            objects or any other registered environment containers.
        """
        super().__init__()

        module_name, qualname = _get_importable_callable_reference(jvar)
        signature = inspect.signature(jvar)
        binding = signature.bind(*args, **kwargs)
        bound_kwargs = binding.kwargs

        var_positional_name: str | None = None
        var_keyword_name: str | None = None
        for name, parameter in signature.parameters.items():
            if parameter.kind is inspect.Parameter.VAR_POSITIONAL:
                var_positional_name = name
            elif parameter.kind is inspect.Parameter.VAR_KEYWORD:
                var_keyword_name = name

        # BoundArguments.args canonicalizes all values that can safely be passed
        # positionally. Keep their parameter names so users can address leaves
        # by name while env_schema preserves the required call order.
        args_order = tuple(
            name
            for name in binding.arguments
            if name not in bound_kwargs and name != var_keyword_name
        )
        kwargs_order = tuple(bound_kwargs)

        self.jvar = jvar
        self._jvar_module = module_name
        self._jvar_qualname = qualname
        self._args_order = args_order
        self._kwargs_order = kwargs_order
        self._var_positional_name = var_positional_name

        self["args"] = {
            name: binding.arguments[name]
            for name in args_order
        }
        self["kwargs"] = {
            name: bound_kwargs[name]
            for name in kwargs_order
        }

        # This is a convenient runtime description, but the adapter deliberately
        # excludes it from reduced_env. The authoritative copy is schema metadata.
        self["jvar"] = f"{module_name}.{qualname}"

    def __call__(self) -> Any:
        """Recursively evaluate contained buffers and invoke ``jvar``."""
        args_data = self.get("args", MISSING_ENV_VALUE)
        kwargs_data = self.get("kwargs", MISSING_ENV_VALUE)

        if (
            args_data is MISSING_ENV_VALUE
            or kwargs_data is MISSING_ENV_VALUE
            or contains_missing_environment_values(args_data)
            or contains_missing_environment_values(kwargs_data)
        ):
            raise ValueError(
                "Cannot call a sparse JBuffer reconstructed from an environment "
                "that did not save every argument value."
            )
        if not isinstance(args_data, Mapping):
            raise TypeError("JBuffer['args'] must be a mapping.")
        if not isinstance(kwargs_data, Mapping):
            raise TypeError("JBuffer['kwargs'] must be a mapping.")

        active_container_ids: set[int] = set()

        def evaluate(value: Any) -> Any:
            if isinstance(value, JBuffer):
                return value()

            adapter = DEFAULT_CONTAINER_REGISTRY.for_value(value)
            if adapter is None:
                return value

            object_id = id(value)
            if object_id in active_container_ids:
                raise ValueError("Cyclic container encountered in JBuffer arguments.")

            active_container_ids.add(object_id)
            try:
                evaluated_children = [
                    (selector, evaluate(child))
                    for selector, child in adapter.iter_children(value)
                ]
                return adapter.build(
                    evaluated_children,
                    adapter.get_metadata(value),
                )
            finally:
                active_container_ids.remove(object_id)

        positional_args: list[Any] = []
        for name in self._args_order:
            try:
                value = evaluate(args_data[name])
            except KeyError as exc:
                raise ValueError(
                    f"Cannot call sparse JBuffer: positional argument "
                    f"{name!r} was not saved."
                ) from exc

            if name == self._var_positional_name:
                try:
                    positional_args.extend(value)
                except TypeError as exc:
                    raise TypeError(
                        f"JBuffer variadic positional argument {name!r} must "
                        "evaluate to an iterable."
                    ) from exc
            else:
                positional_args.append(value)

        keyword_args: dict[str, Any] = {}
        for name in self._kwargs_order:
            try:
                keyword_args[name] = evaluate(kwargs_data[name])
            except KeyError as exc:
                raise ValueError(
                    f"Cannot call sparse JBuffer: keyword argument "
                    f"{name!r} was not saved."
                ) from exc

        # self['jvar'] is a persisted name string; self.jvar is the callable.
        return self.jvar(*positional_args, **keyword_args)


def _resolve_qualified_callable(module_name: str, qualname: str) -> Callable[..., Any]:
    """Resolve an importable callable from schema metadata."""
    if not module_name or not qualname or "<locals>" in qualname:
        raise ValueError(
            "JBuffer callables must be module-level importable objects; local "
            "functions, lambdas, and closures are not reversible."
        )

    obj: Any = importlib.import_module(module_name)
    for component in qualname.split("."):
        obj = getattr(obj, component)
    if not callable(obj):
        raise TypeError(f"{module_name}.{qualname} does not resolve to a callable.")
    return obj


def _same_callable(left: Callable[..., Any], right: Callable[..., Any]) -> bool:
    """Compare functions and bound class methods without accepting instance methods."""
    if left is right:
        return True
    if inspect.ismethod(left) and inspect.ismethod(right):
        return left.__func__ is right.__func__ and left.__self__ is right.__self__
    return False


def _get_importable_callable_reference(
    jvar: Callable[..., Any],
) -> tuple[str, str]:
    """Return and validate the durable reference used in env_schema."""
    module_name = getattr(jvar, "__module__", None)
    qualname = getattr(jvar, "__qualname__", None)
    if not isinstance(module_name, str) or not isinstance(qualname, str):
        raise TypeError(
            "JBuffer requires a callable with string __module__ and "
            "__qualname__ attributes."
        )

    resolved = _resolve_qualified_callable(module_name, qualname)
    if not _same_callable(jvar, resolved):
        raise TypeError(
            f"JBuffer callable {module_name}.{qualname} is not recoverable as "
            "the same callable. Bound instance methods are not supported."
        )
    return module_name, qualname


def _jbuffer_iter_children(value: JBuffer):
    """Expose only original argument data as container children."""
    return (
        ("args", dict.__getitem__(value, "args")),
        ("kwargs", dict.__getitem__(value, "kwargs")),
    )


def _jbuffer_normalize_selector(value: JBuffer, selector: Any) -> str:
    """Restrict addressable JBuffer entries to value-bearing children."""
    if selector not in JBuffer._DATA_KEYS:
        raise KeyError(selector)
    return selector


def _jbuffer_get_child(value: JBuffer, selector: Any) -> Any:
    selector = _jbuffer_normalize_selector(value, selector)
    return dict.__getitem__(value, selector)


def _jbuffer_metadata(value: JBuffer) -> Mapping[str, Any]:
    """Return reconstruction information stored exclusively in env_schema."""
    return {
        "jvar_module": value._jvar_module,
        "jvar_qualname": value._jvar_qualname,
        "args_order": list(value._args_order),
        "kwargs_order": list(value._kwargs_order),
        "var_positional_name": value._var_positional_name,
    }


def _metadata_name_tuple(metadata: Mapping[str, Any], key: str) -> tuple[str, ...]:
    raw = metadata.get(key)
    if not isinstance(raw, list) or not all(isinstance(item, str) for item in raw):
        raise ContainerDecodingError(
            f"JBuffer schema metadata {key!r} must be a list of strings."
        )
    if len(raw) != len(set(raw)):
        raise ContainerDecodingError(
            f"JBuffer schema metadata {key!r} contains duplicate names."
        )
    return tuple(raw)


def _build_jbuffer(
    children: Sequence[tuple[Any, Any]],
    metadata: Mapping[str, Any],
) -> JBuffer:
    """Rebuild complete or sparse JBuffer data without invoking __init__."""
    module_name = metadata.get("jvar_module")
    qualname = metadata.get("jvar_qualname")
    if not isinstance(module_name, str) or not isinstance(qualname, str):
        raise ContainerDecodingError(
            "JBuffer schema metadata must contain string jvar_module and "
            "jvar_qualname values."
        )

    args_order = _metadata_name_tuple(metadata, "args_order")
    kwargs_order = _metadata_name_tuple(metadata, "kwargs_order")
    if set(args_order) & set(kwargs_order):
        raise ContainerDecodingError(
            "JBuffer args_order and kwargs_order may not overlap."
        )

    var_positional_name = metadata.get("var_positional_name")
    if var_positional_name is not None and not isinstance(var_positional_name, str):
        raise ContainerDecodingError(
            "JBuffer var_positional_name must be a string or None."
        )
    child_map: dict[str, Any] = {}
    for selector, child in children:
        if selector not in JBuffer._DATA_KEYS:
            raise ContainerDecodingError(
                f"Unexpected JBuffer child selector {selector!r}."
            )
        if selector in child_map:
            raise ContainerDecodingError(
                f"Duplicate JBuffer child selector {selector!r}."
            )
        child_map[selector] = child

    jvar = _resolve_qualified_callable(module_name, qualname)

    # Sparse reconstruction cannot call JBuffer.__init__, because omitted
    # argument subtrees are intentionally unavailable. Build the invariant
    # directly and mark absent top-level data children explicitly.
    buffer = JBuffer.__new__(JBuffer)
    dict.__init__(buffer)
    buffer.jvar = jvar
    buffer._jvar_module = module_name
    buffer._jvar_qualname = qualname
    buffer._args_order = args_order
    buffer._kwargs_order = kwargs_order
    buffer._var_positional_name = var_positional_name
    buffer["args"] = child_map.get("args", MISSING_ENV_VALUE)
    buffer["kwargs"] = child_map.get("kwargs", MISSING_ENV_VALUE)
    buffer["jvar"] = f"{module_name}.{qualname}"
    return buffer


# Exact-type post-registration is necessary because the built-in dict adapter
# deliberately does not claim subclasses with potentially different invariants.
register_container_adapter(
    JBuffer,
    type_id="jmaps.journey.path.JBuffer.v1",
    iter_children=_jbuffer_iter_children,
    get_child=_jbuffer_get_child,
    normalize_selector=_jbuffer_normalize_selector,
    get_metadata=_jbuffer_metadata,
    build=_build_jbuffer,
    replace=True,
)

class PathOptions(BaseModel):
    """Runtime options controlling path execution and caching."""
    disable_saving_and_loading: bool = Field(
        False,
        description="If true, results are not saved to or loaded from the database.",
    )
    plot: bool = Field(True, description="Whether to plot results after running.")
    verbose: bool = Field(False, description="Whether to print verbose output.")
    use_tqdm: bool = Field(
        True, description="Track batch/loop progress with tqdm."
    )

class PathResult(BaseModel):
    """Container for the results of a path execution.

    Attributes:
        sql: Mapping of values that are persisted in the SQL database.
        file: Mapping of values that are persisted on disk using the IO registry.
    """
    id : int | None = Field(
        None, description="The database ID of the result entry. This is set after saving to the database."
    )
    data: Dict[str, Any] = Field(
        default_factory=dict, description="Results that are saved to the sql database. If it is not a SQL backed value, it will be saved to a file instead."
    )
    completed: bool = Field(
        True, description="Whether the path has completed successfully. If False, the path was interrupted or failed."
    )
    error: Any = Field(
        None, description="An error or exception that occurred while running the path, buffered to allow partial results to be saved to the database."
    )

    def __getitem__(self, key: str) -> Any:
        """Return a result by key, preferring SQL-backed values."""
        return self.data[key]

    def __setitem__(self, key, value):
        self.data[key] = value
    
    @classmethod
    def from_db(cls, db_result: DBResult, result_directory: Path):
        loaded_result = cls(completed=db_result.completed, error=None)
        loaded_result.load(db_result, result_directory)
        return loaded_result
    
    def save(self, result_directory: Path, path_name: str, path_version: int, reduced_env: Dict[str, Any]):
        """Serialize file-backed results via the IO registry.

        Args:
            file_path: Base path to use for all file-backed results.
            path_name: name of the path in the sql database
            path_version: version number of the path in the sql database
            reduced_env: the relevant environment parameters to obtaining this path result

        Returns:
            dict[str, list[str]] | None: Schema describing how each key was written,
            or ``None`` if there are no file-backed results.
        """
        file_schema: dict[str, list[str]] = {}
        file_name = get_filename(reduced_env)
        file_path = result_directory / path_name / file_name
        sql_results = {}
        for k, v in self.data.items():
            if not is_sql_type(v):
                file_schema[k] = write(v, file_path.with_name(file_path.name + "_" + k))
            else:
                sql_results[k] = v

        db_result = DBResult(
                environment=reduced_env,
                data=sql_results if sql_results else Null(),
                path_name=path_name,
                path_version_num=path_version,
                file_name=file_name if not isinstance(file_schema, Null) else Null(),
                created_at=datetime.now(timezone.utc),
                completed=self.completed,
            )
        session = db_session()
        session.add(db_result)
        self.id = db_result.id
        # session.commit()
        return file_schema
    
    def load(self, db_result: DBResult, result_directory: Path):
        file_path = (result_directory / db_result.path_name / db_result.file_name) if db_result.file_name is not None else None
        self.data = db_result.data
        files_not_found = self.load_files(file_path, db_result.path_version.file_schema)
        if len(files_not_found) != 0:
            raise FileNotFoundError(f"Files not found for the following result keys: {files_not_found}")

    def load_files(self, file_path: Path | None, file_schema: dict[str, list[str]] | None):
        """Populate file-backed results from disk via the IO registry.

        Args:
            file_path: Base path used when the results were written. If ``None``,
                no file-backed results are loaded.
            file_schema: Schema describing how each key was written, or ``None``.
        Returns:
            An empty list if the file-backed results were loaded successfully, or a list of unloaded Paths if not.
        """
        if file_schema is None or file_path is None:
            return []
        files_not_found = []
        for k, v in file_schema.items():
            file_item_path = file_path.with_name(file_path.name + "_" + k)
            if list(file_item_path.parent.glob(file_item_path.name + "*")):
                self.data[k] = read(v[0], v[1], file_item_path)
            else:
                files_not_found.append(k)
        return files_not_found

class JPath(Generic[P, R]):
    """
    A callable wrapper that preserves parameter and return type information.
    
    This class wraps a callable function while maintaining its original signature
    for static type checking and runtime introspection.
    
    Parameters
    ----------
    func : Callable[P, R]
        The function to wrap. Must have exactly three parameters:
        'env', 'partial_result', and 'path_options', in that order.
    
    Attributes
    ----------
    func : Callable[P, R]
        The wrapped function.
    __signature__ : inspect.Signature
        The signature of the original function, used by inspect.signature().
    
    Raises
    ------
    TypeError
        If the provided function does not have exactly the required parameters
        ('env', 'partial_result', 'path_options') in the correct order.
    
    Examples
    --------
    >>> @jmap
    ... def my_path(env: int, partial_result: str, path_options: bool) -> int:
    ...     return env
    
    >>> result = my_path(1, "test", True)
    >>> result
    1
    """
    
    REQUIRED_ARGS = ("env", "path_options", "partial_result")
    
    def __init__(self, func: Callable[P, R], nondeterministic=False) -> None:
        """
        Initialize the JPath wrapper.
        
        Parameters
        ----------
        func : Callable[P, R]
            The function to wrap.
        
        Raises
        ------
        TypeError
            If the function signature does not match requirements.
        """
        self.nondeterministic=nondeterministic
        signature = inspect.signature(func)
        actual_args = tuple(signature.parameters.keys())
        
        if actual_args != self.REQUIRED_ARGS:
            raise TypeError(
                f"@jmap requires arguments {self.REQUIRED_ARGS}, in that order. "
                f"{func.__qualname__} has arguments {actual_args}."
            )
        
        self.func = func
        self.func_hash = hashlib.sha256(normalized_function_ast(func).encode()).digest()
        self.__signature__ = signature
        update_wrapper(self, func)
        
        ################################# Analyze Schema ########################################
        self.env_trees, self.function_calls = analyze_env_schema(func)
        
        # Register the path in the global path registry
        jpath_registry[func.__module__ + "." + func.__qualname__] = self
    
    def __call__(self, env: dict[Hashable, Any], path_options : PathOptions|None=None) -> PathResult:
        """
        Call the wrapped function unless there is a complete result from the database to return instead. 
        If a partial result exists, pass it to the wrapped function.
        
        Parameters
        ----------
        env : dict[str, Any]
            The environment dictionary to pass to the wrapped function.
        path_options : Any, optional
            Options for the path evaluation, by default None.

        Returns
        -------
        R
            The return value from the wrapped function.
        """
        global db_session
        global result_directory
        if path_options is None:
            path_options = PathOptions()
        if path_options.disable_saving_and_loading:
            return self.func(env, path_options, partial_result=None)
        if db_session is None or result_directory is None:
            raise ValueError("Data storage not initialized, please call init_db()")
        schema, reduced_env = self.evaluate_schema(env)

        session = db_session()

        # Find the path version corresponding to the current function
        path_name = self.func.__name__
        version_stmt = select(DBPathVersion).where(
                        DBPathVersion.name == path_name,
                        DBPathVersion.env_schema == schema,
                        DBPathVersion.ast_hash == self.func_hash,
                        DBPathVersion.nondeterministic == self.nondeterministic
                    )
        path_version = session.execute(version_stmt).scalar_one_or_none()

        result = None
        # The path version must exist, and the path must be deterministic, to load a result from the database
        if path_version is not None and not path_version.nondeterministic:
            result_stmt = select(DBResult).where(
                        DBResult.path_name == path_version.name,
                        DBResult.path_version_num == path_version.version,
                        DBResult.environment == reduced_env,
                    )
            db_result = session.execute(result_stmt).scalar_one_or_none()
            result = PathResult.from_db(db_result=db_result, result_directory=result_directory)

        # Run the function if no result was loaded, or if a partial result was loaded
        if result is None or not result.completed:
            result = self.func(env, path_options, partial_result=result)

            # Create a new path and path version if necessary and save the results
            if path_version is None:
                # Look for an existing path table in the database
                path_stmt = select(DBPath).where(
                    DBPath.name == path_name
                )
                path = session.execute(path_stmt).scalar_one_or_none()
                if path is None:
                    path = DBPath(name=path_name, description="") # TODO - add the description from the docstrings of self.func
                    session.add(path)
                    next_version = 0
                else:
                    # Find the max path version and make the new version that+1
                    max_version_stmt = (
                        select(DBPathVersion.version)
                        .where(DBPathVersion.name == path_name)
                        .order_by(DBPathVersion.version.desc())
                        .limit(1)
                    )
                    max_version_result = session.execute(max_version_stmt).scalar_one_or_none()
                    next_version = 0 if max_version_result is None else (max_version_result + 1)
                file_schema = result.save(result_directory, path_name, next_version, reduced_env)
                # Create a new DBPathVersion entry.
                path_version = DBPathVersion(
                    name=path_name,
                    version=next_version,
                    env_schema=schema,
                    file_schema=file_schema,
                    ast_hash=self.func_hash,
                    nondeterministic=self.nondeterministic,
                )
                # path_version.file_schema = file_schema
                session.add(path_version)
            else:
                file_schema = result.save(result_directory, path_name, path_version.version, reduced_env)
            if result.error is not None:
                print(f"Error running {path_name}: partial result saved to {result.id}")
                raise result.error
            session.commit()
        return result

    def evaluate_schema(
        self,
        env: dict[Hashable, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Build a reversible schema and sparse JSONB environment payload.

        Every container on a used path records its adapter type and all
        reconstruction metadata in ``env_schema``. ``reduced_env`` is only a
        nested dictionary of selected SQL/JSON-compatible leaf values; list,
        tuple, and custom-container nodes are represented as dictionaries until
        restored through ``env_schema``.
        """
        used_leaves = self.get_used_leaves(env=env)
        return serialize_reduced_environment(env, used_leaves)

    @staticmethod
    def restore_environment(
        env_schema: dict[str, Any],
        reduced_env: dict[str, Any],
    ) -> Any:
        """Reconstruct a saved reduced environment from its schema."""
        return deserialize_reduced_environment(env_schema, reduced_env)

    def get_used_leaves(
        self,
        env: Any | None = None,
    ) -> set[EnvPath]:
        """Return typed paths for all environment leaves used by this path."""
        used_leaves: set[EnvPath] = set()

        for tree in self.env_trees:
            used_leaves |= tree.get_used_leaves(env=env)

        for fn_call in self.function_calls:
            qualified_name = fn_call.get_runtime_name(self.func.__globals__)
            if qualified_name not in jpath_registry:
                continue

            called_path = jpath_registry[qualified_name]
            if env is not None:
                used_leaves |= (
                    called_path.get_used_leaves(env=env)
                    - fn_call.get_overwritten_leaves(env=env)
                )
                continue

            # Without a concrete environment, a parent overwrite hides every
            # descendant used by the called path.
            untrimmed_fn_usage = called_path.get_used_leaves(env=None)
            overwritten_leaves = fn_call.get_overwritten_leaves(env=None)

            for path in untrimmed_fn_usage:
                overwritten = any(
                    path[:prefix_length] in overwritten_leaves
                    for prefix_length in range(1, len(path) + 1)
                )
                if not overwritten:
                    used_leaves.add(path)

        return used_leaves
    
    def migrate_version(
        self,
        update_fn: Callable,
        source_version: int | None = None,
        target_version: int | None = None,
    ) -> None:
        """
        Perform a migration of a JPath version.
        
        Parameters
        ----------
        update_fn : Callable
            A function that performs the version update operation.
        source_version : int, optional
            The source version to migrate from. Default is None.
        target_version : int, optional
            The target version to migrate to. Default is None.
        
        Returns
        -------
        None
        """
        pass

class JMapDecorator(Protocol):
    def __call__(
        self,
        func: Callable[P, R],
        /,
    ) -> JPath[P, R]:
        ...


@overload
def jmap(
    func: Callable[P, R],
    /,
) -> JPath[P, R]:
    ...


@overload
def jmap(
    *,
    nondeterministic: bool = False,
) -> JMapDecorator:
    ...


def jmap(
    func: Callable[P, R] | None = None,
    *,
    nondeterministic: bool = False,
) -> JPath[P, R] | JMapDecorator:

    def decorator(fn: Callable[P, R]) -> JPath[P, R]:
        return JPath(fn, nondeterministic=nondeterministic)

    if func is None:
        return decorator

    return decorator(func)
