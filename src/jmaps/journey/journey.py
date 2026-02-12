"""Journey orchestration and caching.

Provides the :class:`Journey` class for composing paths, validating dependency
graphs, running (possibly batched) subpaths, caching results in a SQL
database, and loading previously computed results when possible.
"""

from typing import Union, Any, Dict
from pathlib import Path
from datetime import datetime, timezone
import copy
import hashlib
import json

from pydantic import BaseModel, Field
from tqdm import tqdm
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import select, Null
from sqlalchemy.engine import Engine

from jmaps.config import PATH
from jmaps.journey.jmalc import (
    cast_sql_type,
    get_sql_schema,
    create_tables,
    DBPath,
    DBPathVersion,
    DBResult,
)
from jmaps.journey.path import JPath, JBatch, PathResult
from jmaps.journey.param import REF_SEP, JDict
class PathOptions(BaseModel):
    """Runtime options controlling path execution and caching."""

    force_run_to_depth: int = Field(
        0,
        description=(
            "Force the path tree to run even if cached. Most useful when a path "
            "has been changed but outdated cached results still exist. Because "
            "subpaths form a tree with the main path at the top, this is "
            "specified as an integer indicating how deep the forced re-run "
            "should propagate."
        ),
    )
    disable_saving_and_loading: bool = Field(
        False,
        description="If true, results are not saved to or loaded from the database.",
    )
    plot: bool = Field(True, description="Whether to plot results after running.")
    verbose: bool = Field(False, description="Whether to print verbose output.")
    batch_tqdm: bool = Field(
        False, description="Track batch progress with tqdm when running subpaths."
    )

    class Config:
        extra = "forbid"
        validate_assignment = True


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
    return key


class Journey(BaseModel):
    """Executable container for environments and paths.

    Manages validation, caching, dependency execution, and convenience helpers
    for running and introspecting complex multi-step processes.
    """

    name: str = Field(
        ...,
        description=(
            "Name describing the journey, encompassing all paths that will be run."
        ),
    )
    env: JDict = Field(default_factory=JDict)
    paths: Dict[str, JPath] = Field(default_factory=dict)
    cache_db_meta: bool = Field(True, description ='If true, does not query the database for the latest current_path_version, instead storing and retrieving it from the cache.')
    db_current_path_versions: Dict[str, int] = Field(default_factory=dict)
    db_current_path_env_schemas: Dict[str, dict] = Field(default_factory=dict)
    db_current_path_file_schemas: Dict[str, dict] = Field(default_factory=dict)
    result_directory: Path = Field(
        ..., description="Directory where file-based results are stored."
    )
    engine: Any = Field(...)
    session_factory: Any = Field(...)
    Session: Any = Field(
        ..., description="Factory for creating SQLAlchemy sessions to the cache DB."
    )

    def __init__(
        self,
        name: str,
        engine: Engine,
        env: JDict | None = None,
        paths: Union[dict[str, JPath], list[JPath]] | None = None,
        result_directory: Path | None = None,
        cache_db_meta: bool= True
    ):
        """Initialize a :class:`Journey`.

        Args:
            name: Identifier for this journey.
            engine: SQLAlchemy engine used for caching path definitions/results.
            env: Optional root environment. If omitted, an empty :class:`JDict`
                is created.
            paths: Optional mapping or list of paths. When a list is provided,
                each path is keyed by ``path.name``.
            result_directory: Base directory where file-backed results are stored.
                Defaults to ``PATH.journeys / name``.
        """
        if paths is None:
            paths = {}
        if isinstance(paths, list):
            paths = {path.name: path for path in paths}
        result_directory = (
            result_directory if result_directory is not None else PATH.data / name
        )
        result_directory.mkdir(parents=True, exist_ok=True)

        # Ensure DB tables exist (no-op if they already do).
        create_tables(engine)
        session_factory = sessionmaker(bind=engine)
        Session = scoped_session(session_factory)

        for path_name, path in paths.items():
            path_dir = result_directory / path_name
            path_dir.mkdir(parents=True, exist_ok=True)

        super().__init__(
            name=name,
            engine=engine,
            session_factory=session_factory,
            Session=Session,
            env=env if env is not None else JDict(data={}),
            paths=paths,
            result_directory=result_directory,
            cache_db_meta=cache_db_meta
        )

    def update_path(self, path: JPath, validate: bool = True):
        """Updates a single path int the journey.

        Optionally validates that all environments and subpaths used by the path
        are defined in the journey.

        Args:
            path: Path instance to add.
            validate: If ``True``, run :meth:`validate_paths` after adding.
        """
        self.paths[path.name] = path
        if validate:
            self.validate_paths(error=True)
        path_dir = self.result_directory / path.name
        path_dir.mkdir(parents=True, exist_ok=True)

    def update_paths(self, new_paths: list[JPath], validate: bool = True):
        """Update multiple paths in the journey.

        Args:
            new_paths: List of paths to add.
            validate: If ``True``, run :meth:`validate_paths` after adding.
        """
        for path in new_paths:
            self.add_path(path, validate=False)
        if validate:
            self.validate_paths(error=True)

    def get_path(self, name: str) -> JPath:
        """Return a path by name."""
        return self.paths[name]

    def get_paths(self) -> Dict[str, JPath]:
        """Return the mapping of all registered paths."""
        return self.paths

    def circular_subpaths(
        self, path_name: str, paths_prior: list[str] | None = None
    ) -> list[str]:
        """Detect circular subpath dependencies rooted at ``path_name``.

        Args:
            path_name: Name of the path to check.
            paths_prior: Accumulated list of path names visited so far.

        Returns:
            list[str]: Empty list if no circular dependency is found; otherwise
            the sequence of path names forming the cycle.
        """
        if paths_prior is None:
            paths_prior = []
        if path_name in paths_prior:
            return paths_prior + [path_name]
        paths_prior.append(path_name)
        for subpath_name in self.paths[path_name].subpaths:
            if subpath_name in self.paths:
                circular_path = self.circular_subpaths(
                    subpath_name, copy.copy(paths_prior)
                )
                if len(circular_path) > 0:
                    return circular_path
        return []

    def validate_path(
        self, path_name: str, error: bool = True, verbose: bool = True
    ) -> tuple[list[str], list[str], list[str]]:
        """Validate that a single path's dependencies are satisfiable.

        Checks that all required subpaths (batched and non-batched) exist and
        that no circular dependencies are present.

        Args:
            path_name: Name of the path to validate.
            error: Whether to raise an error if the path is invalid.
            verbose: Whether to print a message if the path is invalid.

        Returns:
            tuple[list[str], list[str], list[str]]: A tuple containing:

            * missing_subpaths: Subpaths that are not registered on this journey.
            * missing_batched_subpaths: Batched subpaths that are not registered.
            * circular_path: Sequence of path names forming a detected cycle,
              or an empty list if no cycle is found.
        """
        path = self.paths[path_name]
        missing_subpaths: list[str] = []
        missing_batched_subpaths: list[str] = []

        for subpath_name in path.subpaths:
            if subpath_name not in self.paths:
                missing_subpaths.append(subpath_name)
        circular_path = self.circular_subpaths(path_name)
        for subpath_name in path.batched_subpaths:
            if subpath_name not in self.paths:
                missing_batched_subpaths.append(subpath_name)
        if (len(missing_subpaths) > 0 or len(missing_batched_subpaths) > 0) and (
            error or verbose
        ):
            error_string = ""
            if len(missing_subpaths) > 0:
                error_string += (
                    f"{path_name} is missing subpath(s): {', '.join(missing_subpaths)}"
                )
            if len(missing_batched_subpaths) > 0:
                error_string += (
                    f"{path_name} is missing batched subpath(s): "
                    f"{', '.join(missing_batched_subpaths)}"
                )
            if len(circular_path) > 0:
                error_string += f"{path_name} is circular: {', '.join(circular_path)}"
            if error:
                raise ValueError(error_string)
            else:
                if verbose:
                    print(error_string)
        return missing_subpaths, missing_batched_subpaths, circular_path

    def validate_paths(self, error: bool = True):
        """Validate all registered paths.

        Args:
            error: If ``True``, raise an error when any invalid paths are found;
                otherwise print a summary string.
        """
        error_string = "Invalid paths"
        invalid = False
        for path_name, path in self.paths.items():
            (
                missing_subpaths,
                missing_batched_subpaths,
                circular_path,
            ) = self.validate_path(path_name, error=False, verbose=False)
            # If any envs or subpaths are missing, add to invalid paths string.
            if len(missing_subpaths) > 0:
                error_string += (
                    f"\n{path_name} is missing subpath(s): "
                    f"{', '.join(missing_subpaths)}"
                )
                invalid = True
            if len(missing_batched_subpaths) > 0:
                error_string += (
                    f"\n{path_name} is missing batched subpath(s): "
                    f"{', '.join(missing_batched_subpaths)}"
                )
                invalid = True
            if len(circular_path) > 0:
                error_string += (
                    f"\n{path_name} is circular with: {', '.join(circular_path)}"
                )
                invalid = True
        # If any paths are invalid, raise an error/warning.
        if invalid:
            if error:
                raise ValueError(error_string)
            else:
                print(error_string)

    def run(self, path_name: str, path_options: PathOptions):
        """Run a named path using the Journey's root environment.

        Args:
            path_name: Name of the path to run.
            path_options: Execution and caching options.

        Returns:
            tuple[PathResult, dict[str, Any] | None]: A tuple of the path result
            and subpath results.
        """
        if path_name not in self.paths:
            raise ValueError(f"The path '{path_name}' does not exist in this Journey")
        local_env = self.env.model_copy(deep=True)
        return self._run(local_env, path_name, path_options, is_parent=True)

    def _run(
        self, local_env: JDict, path_name: str, path_options: PathOptions, is_parent: bool = False
    ):
        """Core implementation for running a path and its subpaths.

        Handles loading from cache, executing subpaths (including batches),
        saving results, and optional plotting.

        Args:
            local_env: Environment to use for this run (usually a copy of root).
            path_name: Name of the path to run.
            path_options: Execution and caching options.
            is_parent: ``True`` if this invocation is the top-level call.

        Returns:
            tuple[PathResult, dict[str, Any] | None]: Path result and subpath results.
        """
        local_env.init_run(is_parent)
        local_env.reset_usage()

        result: PathResult | None = None
        if path_options.force_run_to_depth == 0 and not path_options.disable_saving_and_loading:
            result = self.load_path_results(local_env, path_name)
        if result is not None:
            if path_options.verbose:
                print(f"Loading {path_name}: {result}")
            # Don't load another recursion of subpaths if we are a subpath already.
            if not is_parent:
                return result, None
        else:
            if path_options.verbose:
                print(f"Running {path_name}.")
        subpath_options = path_options.model_copy()
        subpath_options.force_run_to_depth = (
            subpath_options.force_run_to_depth - 1
            if subpath_options.force_run_to_depth > 0
            else 0
        )
        subpath_results = self.run_subpaths(local_env, path_name, subpath_options)
        if result is None:
            # Run the path.
            result = self.paths[path_name].run(
                local_env, subpath_results, path_options.verbose
            )
            # Save the results to cache.
            if not path_options.disable_saving_and_loading:
                self.save_path_results(local_env, path_name, result)
        # Plot the path results.
        if path_options.plot:
            self.paths[path_name].plot(result, subpath_results)
        return result, subpath_results

    def run_subpaths(
        self, local_env: JDict, path_name: str, subpath_options: PathOptions
    ):
        """Run all subpaths required by ``path_name`` (including batched ones).

        Args:
            local_env: Environment to use for the parent path.
            path_name: Name of the parent path.
            subpath_options: Execution options propagated to subpaths.

        Returns:
            dict[str, PathResult | dict[str, PathResult]]: Mapping from subpath
            name to result or nested batch results.
        """
        subpath_results: dict[str, PathResult | dict[str, PathResult]] = {}
        # Run the subpaths, and retrieve the files their results are stored in.
        for subpath_name in self.paths[path_name].subpaths:
            batch = self.paths[path_name].get_batch(
                subpath_name, local_env, subpath_results
            )
            if batch is None:
                subpath_env = local_env.model_copy(deep=True)
                subpath_result, _ = self._run(
                    subpath_env, subpath_name, subpath_options, is_parent=False
                )
                subpath_results[subpath_name] = subpath_result
                local_env.merge_usage(subpath_env)
            else:
                subpath_results[subpath_name] = {}
                # Iterate through each element of the batch.
                if subpath_options.batch_tqdm:
                    enumerate_batch = tqdm(
                        batch.items(),
                        total=len(batch),
                        desc=f"Running {subpath_name} batch",
                    )
                else:
                    enumerate_batch = batch.items()
                update_local_env = True
                for batch_id, batch_env in enumerate_batch:
                    subpath_env = local_env.model_copy(deep=True)
                    batch_env.init_run(is_parent_path=True, parent_env=subpath_env)
                    subpath_env.replace(batch_env)
                    subpath_result, _ = self._run(
                        subpath_env, subpath_name, subpath_options, is_parent=False
                    )
                    # Update parameter usage according to subpath usage.
                    if update_local_env:
                        # These are dependent parameters, so don't count towards usage.
                        batch_env.reset_usage()
                        local_env.merge_usage(subpath_env)

                    # Save the results of the subpath.
                    subpath_results[subpath_name][batch_id] = subpath_result
        return subpath_results

    def load_path_results(self, local_env: JDict, path_name: str):
        """Load results for a path from the cache, if available.

        Args:
            local_env: Environment containing parameter trees.
            path_name: Name of the path whose results should be loaded.

        Returns:
            PathResult | None: Loaded result, or ``None`` if no matching entry
            exists in the cache.
        """
        if self.paths[path_name].save_datetime:
            return None
        session = self.Session()
        path_version_num = None
        env_schema = None
        if self.cache_db_meta and path_name in self.db_current_path_versions:
            path_version_num = self.db_current_path_versions[path_name]
        else:
            path_stmt = select(DBPath).where(DBPath.name == path_name)
            path = session.execute(path_stmt).scalar_one_or_none()
            
            if path is None:
                return None
            path_version_num = path.current_version
            if self.cache_db_meta:
                self.db_current_path_versions[path_name] = path_version_num

        if self.cache_db_meta and path_name in self.db_current_path_env_schemas:
            env_schema = self.db_current_path_env_schemas[path_name]
            file_schema = self.db_current_path_file_schemas[path_name]
        else:
            path_version_num = path.current_version
            version_stmt = select(DBPathVersion).where(
                DBPathVersion.name == path_name,
                DBPathVersion.version == path_version_num,
            )
            path_version = session.execute(version_stmt).scalar_one_or_none()
            if path_version is None:
                return None
            env_schema = path_version.env_schema
            file_schema = path_version.file_schema
            if self.cache_db_meta:
                self.db_current_path_env_schemas[path_name] = env_schema
                self.db_current_path_file_schemas[path_name] = file_schema
        temp_env: dict[str, Any] = {}
        for param_used in env_schema.keys():
            param_path = param_used.split(REF_SEP)
            jparam = local_env
            dtype = None
            for i, key in enumerate(param_path):
                if i == len(param_path) - 1:
                    dtype = jparam.data[key].dtype
                jparam = jparam[key]
            temp_env[param_used] = (
                cast_sql_type(jparam) if dtype is None else dtype(jparam)
            )
        result_stmt = select(DBResult).where(
            DBResult.path_name == path_name,
            DBResult.path_version_num == path_version_num,
            DBResult.environment == temp_env,
            DBResult.created_at == Null(),
        )
        db_result = session.execute(result_stmt).scalar_one_or_none()
        if db_result is None:
            local_env.reset_usage()
            return None
        result = PathResult(sql=db_result.data)
        result.from_file(
            Path(db_result.file_path) if db_result.file_path is not None else None,
            file_schema,
        )
        return result

    def save_path_results(self, local_env: JDict, path_name: str, result: PathResult):
        """Persist the results of a path into the cache database.

        Args:
            local_env: Environment used to generate the results.
            path_name: Name of the path.
            result: Results of the path run.
        """
        session = self.Session()
        env_sql = local_env.get_sql_data(show_unused=False, show_invisible=False)
        env_schema = get_sql_schema(env_sql)

        file_path = self.result_directory / path_name / get_filename(env_sql)
        file_schema = result.to_file(file_path)
        file_schema = file_schema if file_schema is not None else Null()
        # Check if a DBPath already exists with this name.
        path_stmt = select(DBPath).where(DBPath.name == path_name)
        path = session.execute(path_stmt).scalar_one_or_none()
        if path is None:
            path = DBPath(
                name=path_name,
                current_version=None,
                description=self.paths[path_name].changelog,
            )
            session.add(path)
            session.commit()
        version_stmt = select(DBPathVersion).where(
            DBPathVersion.name == path_name,
            DBPathVersion.env_schema == env_schema,
            DBPathVersion.file_schema == file_schema,
        )
        path_version = session.execute(version_stmt).scalar_one_or_none()

        if path_version is None:
            # Find the latest version number for this path_name.
            max_version_stmt = (
                select(DBPathVersion.version)
                .where(DBPathVersion.name == path_name)
                .order_by(DBPathVersion.version.desc())
                .limit(1)
            )
            max_version_result = session.execute(max_version_stmt).scalar_one_or_none()
            next_version = 0 if max_version_result is None else (max_version_result + 1)
            # Create a new DBPathVersion entry.
            path_version = DBPathVersion(
                name=path_name,
                changelog=self.paths[path_name].changelog,
                version=next_version,
                env_schema=env_schema,
                file_schema=file_schema,
            )
            session.add(path_version)
            session.commit()
        path_version_num = path_version.version
        path.current_version = path_version_num
        if self.cache_db_meta:
            self.db_current_path_versions[path_name] = path_version_num
            self.db_current_path_env_schemas[path_name] = env_schema
            self.db_current_path_file_schemas[path_name] = file_schema if not isinstance(file_schema, Null) else None
        # Add new DBResult entry, linking to the path_version.
        if self.paths[path_name].save_datetime:
            db_result = DBResult(
                environment=env_sql,
                data=result.sql if result.sql is not None else Null(),
                path_name=path_name,
                path_version_num=path_version_num,
                file_path=str(file_path) if not isinstance(file_schema, Null) else Null(),
                created_at=datetime.now(timezone.utc),
            )
            session.add(db_result)
        else:
            result_stmt = select(DBResult).where(
                DBResult.path_name == path_name,
                DBResult.path_version_num == path_version_num,
                DBResult.environment == env_sql,
                DBResult.created_at == Null(),
            )
            db_result = session.execute(result_stmt).scalar_one_or_none()
            if db_result is None:
                db_result = DBResult(
                    environment=env_sql,
                    data=result.sql if result.sql is not None else Null(),
                    path_name=path_name,
                    path_version_num=path_version_num,
                    file_path=str(file_path) if file_path is not None else Null(),
                    created_at=Null(),
                )
                session.add(db_result)
            else:
                db_result.data = result.sql
                db_result.file_path = str(file_path) if file_path is not None else None
        session.commit()

    # Overrides
    def get_str(self) -> str:
        """Return a human-readable string representation of the Journey."""
        string = f"Journey({self.name})\n"
        string += "Environment:\n"
        string += str(self.env.data)
        string += "\n"
        string += "Paths:\n"
        for path_name, path in self.paths.items():
            string += f"   {path_name}"
            if len(path.subpaths) > 0:
                string += ", Subpaths: " + ", ".join(path.subpaths)
        return string

    def __str__(self) -> str:
        """Return :meth:`get_str` for ``str(journey)``."""
        return self.get_str()