import inspect
from functools import update_wrapper
from typing import Any, Callable, Generic, ParamSpec, TypeVar, Dict, Tuple, Protocol, overload, Hashable
from collections.abc import Sequence
from pydantic import BaseModel, Field
from pathlib import Path
from datetime import datetime, timezone
from jmaps.journey.io import read, write
from jmaps.journey.jmalc import (
    cast_sql_type,
    get_sql_type,
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
    def __init__(
        self, jvar, *args, **kwargs
    ):
        """Initialize a :class:`JBuffer`.

        Args:
            jvar: Function or callable object to invoke.
            *args: Positional arguments (possibly :class:`JParam` instances).
            reset_condition: Rule governing when to reevaluate the callable.
            **kwargs: Keyword arguments (possibly :class:`JParam` instances).
        """
        super().__init__()
        sig = inspect.signature(jvar)
        binding = sig.bind(*args, **kwargs)
        catchall_name = None
        for name, param in sig.parameters.items():
            if param.kind == inspect.Parameter.VAR_KEYWORD:
                catchall_name = name
        self['args_list'] = [k for k in binding.arguments if k not in binding.kwargs and k != catchall_name]
        self['kwargs_list'] = [k for k in binding.kwargs]
        self.update({k: binding.arguments[k] for k in self['args_list']})
        self.update({k: binding.kwargs[k] for k in self['kwargs_list']})
        self.jvar = jvar
        self['jvar'] = self.jvar.__module__ + "." + self.jvar.__qualname__

    def __call__(self):
        def evaluate(obj):
            if isinstance(obj, JBuffer):
                return obj()
            elif isinstance(obj, dict):
                return {k: evaluate(v) for k, v in obj.items()}
            elif isinstance(obj, Sequence) and not isinstance(obj, (str, bytes)):
                return type(obj)(evaluate(v) for v in obj)
            else:
                return obj
        eval_kwargs = {k: evaluate(self[k]) for k in self['kwargs_list']}
        eval_args = [evaluate(self[k]) for k in self['args_list']]
        return self['jvar'](*eval_args, **eval_kwargs)

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
    ) -> Tuple[Dict, Dict]:
        """Evaluate the schema of the wrapped function against ``env``.

        Leaf paths are typed tuples rather than dotted strings. For example,
        ``env["records"][1]["x"]`` is represented internally as
        ``("records", 1, "x")`` so integer keys are preserved.
        """
        env_schema: dict[Hashable, Any] = {}
        reduced_env: dict[Hashable, Any] = {}

        def add_path_to_schema(path: EnvPath) -> None:
            if not path:
                raise ValueError("Environment leaf paths cannot be empty.")

            current_schema = env_schema
            current_env: Any = env
            current_reduced_env = reduced_env

            for key in path[:-1]:
                if not isinstance(current_env, dict):
                    raise ValueError(
                        f"Environment path {path!r} traverses through a "
                        f"non-dictionary value before key {key!r}."
                    )
                if key not in current_env:
                    raise ValueError(
                        f"Key {key!r} does not exist while resolving "
                        f"environment path {path!r}."
                    )
                if key not in current_schema:
                    current_schema[key] = {}
                    current_reduced_env[key] = {}

                current_schema = current_schema[key]
                current_env = current_env[key]
                current_reduced_env = current_reduced_env[key]

            leaf_key = path[-1]
            if not isinstance(current_env, dict):
                raise ValueError(
                    f"Environment path {path!r} ends inside a non-dictionary "
                    "value."
                )
            if leaf_key not in current_env:
                raise ValueError(
                    f"Key {leaf_key!r} does not exist while resolving "
                    f"environment path {path!r}."
                )

            leaf_value = current_env[leaf_key]
            current_schema[leaf_key] = get_sql_type(leaf_value)
            current_reduced_env[leaf_key] = cast_sql_type(leaf_value)

        for path in self.get_used_leaves(env=env):
            add_path_to_schema(path)

        return env_schema, reduced_env

    def get_used_leaves(
        self,
        env: dict[Hashable, Any] | None = None,
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
