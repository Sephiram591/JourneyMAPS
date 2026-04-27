"""Journey orchestration and caching.

Provides the :class:`Journey` class for composing paths, validating dependency
graphs, running (possibly batched) subpaths, caching results in a SQL
database, and loading previously computed results when possible.
"""
import concurrent.futures
from typing import Union, Any, Dict
from pathlib import Path
from datetime import datetime, timezone
import copy
import hashlib
import json

from pydantic import BaseModel, Field
from tqdm import tqdm
from sqlalchemy.orm import Session, scoped_session, sessionmaker
from sqlalchemy import select, Null, create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import PendingRollbackError

from jmaps.config import PATH
from jmaps.journey.jmalc import (
    cast_sql_type,
    get_sql_schema,
    create_tables,
    DBPath,
    DBPathVersion,
    DBResult,
)
from jmaps.journey.path import JPath, JBatch, PathResult, ExecutionType, PathOptions
from jmaps.journey.param import REF_SEP, JDict, JValue, Refer

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


class Journey(BaseModel):
    """Executable container for environments and paths.

    Manages validation, caching, dependency execution, and convenience helpers
    for running and introspecting complex multi-step processes.
    """
    paths: Dict[str, JPath] = Field(default_factory=dict)
    db_engine_arg: str = Field(..., description='String for creating the SQLAlchemy engine for a postgres database')
    cache_db_meta: bool = Field(True, description ='If true, does not query the database for the latest current_path_version, instead storing and retrieving it from the cache.')
    db_current_path_versions: Dict[str, int] = Field(default_factory=dict)
    db_current_path_env_schemas: Dict[str, dict] = Field(default_factory=dict)
    db_current_path_file_schemas: Dict[str, dict] = Field(default_factory=dict)
    engine: Any = Field(None, description='Stores the connection pool sqlalchemy engine')
    session: Any = Field(None, description='Stores the sqlalchemy session')
    result_directory: Path = Field(
        ..., description="Directory where file-based results are stored."
    )

    def __init__(
        self,
        db_engine_arg: str,
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
            result_directory if result_directory is not None else PATH.data
        )
        result_directory.mkdir(parents=True, exist_ok=True)

        for path_name, path in paths.items():
            path_dir = result_directory / path_name
            path_dir.mkdir(parents=True, exist_ok=True)

        super().__init__(
            db_engine_arg=db_engine_arg,
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

    def init_session(self) -> Session:
        if self.engine is None:
            self.engine = create_engine(self.db_engine_arg)
            create_tables(self.engine)
        if self.session is None:
            self.session = Session(bind=self.engine)
        return self.session
        
    def get_session(self) -> Session:
        if self.engine is None:
            self.engine = create_engine(self.db_engine_arg)
            create_tables(self.engine)
        return Session(bind=self.engine)

    def complete_run(
        self, env: JDict, db_result_id: int, path_options: PathOptions
    ):
        """Complete a run by loading the results from the database and updating the environment.

        Args:
            env: Environment to use for this run. This will be updated with the environment parameters used in the partial run.
            db_result_id: The ID of the database result to load.
            path_options: Execution and caching options.
        """
        local_env = env.model_copy(deep=True)
        made_session = False
        if self.session is None:
            self.init_session()
            made_session = True
        try:
            result_stmt = select(DBResult).where(DBResult.id == db_result_id)
            db_result = self.session.execute(result_stmt).scalar_one()
            path_name = db_result.path_name
            if db_result is None:
                raise ValueError(f"No result found for ID {db_result_id}")
            partial_result = PathResult(sql=db_result.data, db_result_id=db_result.id, completed=db_result.completed)
            local_env.load_from_db_result(db_result)
            partial_result.from_file(
                (self.result_directory / path_name / db_result.file_name) if db_result.file_name is not None else None,
                db_result.path_version.file_schema,
            )
            subpath_options = path_options.model_copy()
            subpath_options.force_run_to_depth = (
                subpath_options.force_run_to_depth - 1
                if subpath_options.force_run_to_depth > 0
                else 0
            )
            subpath_results = self.run_subpaths(local_env, path_name, subpath_options)
            if db_result.completed:
                if path_options.verbose:
                    print(f"Run is already complete, loading it instead of running it again.")
                return partial_result, subpath_results
            # Run the path.
            result = self.paths[path_name].run(
                local_env, subpath_results, self, partial_result=partial_result, path_options=path_options
            )
            # Save the results
            self.save_path_results(local_env, path_name, result)
            if result.error is not None:
                print(f"Error running {path_name}: partial result saved to {result.db_result_id}")
                raise result.error
            self.paths[path_name].update_env(local_env, result, subpath_results)
            # Plot the path results.
            if path_options.plot:
                self.paths[path_name].plot(result, subpath_results)
        finally:
            if made_session:
                self.session.commit()
                self.session.close()
                self.session = None
                
        env.init_run(is_parent_path=True)
        env.merge_usage(local_env)
        # env.replace(local_env, merge_usage=True, merge_dtypes=False)
        if self.session is not None:
            self.session.commit()
        return result, subpath_results

    def run(self, env: JDict, path_name: str, path_options: PathOptions):
        """Run a named path using the Journey's root environment.

        Args:
            path_name: Name of the path to run.
            path_options: Execution and caching options.
            session: SQLAlchemy session to use for the database.
        Returns:
            tuple[PathResult, dict[str, Any] | None]: A tuple of the path result
            and subpath results.
        """
        if path_name not in self.paths:
            raise ValueError(f"The path '{path_name}' does not exist in this Journey")
        local_env = env.model_copy(deep=True)
        result, subpath_results = self._run(local_env, path_name, path_options, is_parent=True)
        env.init_run(is_parent_path=True)
        env.merge_usage(local_env)
        # env.replace(local_env, merge_usage=True, merge_dtypes=False)
        if self.session is not None:
            self.session.commit()
        return result, subpath_results

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
        made_session = False
        if self.session is None and not path_options.disable_saving_and_loading:
            self.init_session()
            made_session = True
        try:
            if path_options.force_run_to_depth == 0 and not path_options.disable_saving_and_loading:
                result = self.load_path_results(local_env, path_name)
            if result is not None:
                if not result.completed:
                    if path_options.verbose:
                        action_str = "Partially loaded"
                        # print(f"Partially loading {path_name}: {result}")
                else:
                    if path_options.verbose:
                        action_str = "Loaded"
                        # print(f"Loading {path_name}: {result}")
                    # Don't load another recursion of subpaths if we are a subpath already.
                    if not is_parent:
                        return result, None
            else:
                if path_options.verbose:
                    action_str = "Ran"
                    # print(f"Running {path_name}.")
            subpath_options = path_options.model_copy()
            subpath_options.plot = False
            if not is_parent:
                subpath_options.batch_tqdm = False
            subpath_options.verbose = False
            subpath_options.force_run_to_depth = (
                subpath_options.force_run_to_depth - 1
                if subpath_options.force_run_to_depth > 0
                else 0
            )
            subpath_results = self.run_subpaths(local_env, path_name, subpath_options)
            if result is None or not result.completed:
                # Run the path.
                result = self.paths[path_name].run(
                    local_env, subpath_results, self, partial_result=result, path_options=path_options
                )
                # Save the results to cache.
                if not path_options.disable_saving_and_loading:
                    self.save_path_results(local_env, path_name, result)
            if result.error is not None:
                self.session.commit()
                action_str = "Partially ran"
                # print(f"Error running {path_name}: partial result saved to {result.db_entry.id}")
                if not is_parent:
                    raise result.error
            else:
                self.paths[path_name].update_env(local_env, result, subpath_results)
                # Plot the path results.
                if path_options.plot:
                    self.paths[path_name].plot(result, subpath_results)
        finally:
            if made_session:
                try:
                    self.session.commit()
                except PendingRollbackError as e:
                    # print(f"Error committing session: {e}")
                    self.session.rollback()
                    self.session.close()
                    self.session = None
                    raise e
                if result is not None and path_options.verbose:
                    print(f"{action_str} results for {path_name} to the database, ID: {result.db_entry.id}.")
                self.session.close()
                self.session = None
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
                # local_env.replace(subpath_env, merge_usage=True, merge_dtypes=False)
            else:
                subpath_results[subpath_name] = self.run_batch(local_env, subpath_name, batch, subpath_options)
        return subpath_results

    def run_batch(self, local_env: JDict, path_name: str, batch: JBatch | JDict, path_options: PathOptions):
        """Run a batch of subpaths.

        Args:
            local_env: Environment to use for the parent path.
            path_name: Name of the parent path.
            batch: Batch of subpaths to run.
            path_options: Execution options propagated to subpaths.
            session: SQLAlchemy session to use for the database.

        Returns:
            dict[str, PathResult]: Mapping from batch ID to result.
        """
        single_run = False
        if isinstance(batch, JDict):
            batch = JBatch(runs={"default": batch})
            single_run = True
        batch_results = {}
        match batch.execution_type:
            case ExecutionType.MULTIPLE_PROCESSES:
                with concurrent.futures.ProcessPoolExecutor(max_workers=batch.max_workers) as executor:
                    usage_tracking_batch = list(batch.keys())[0]
                    journey_copy = self.model_copy(update={"engine": None, "session": None})
                    futures = [executor.submit(run_batch_id, journey_copy, local_env, path_name, path_options, batch_id, batch_env, batch_id==usage_tracking_batch)
                                for batch_id, batch_env in batch.items()]
                    if path_options.batch_tqdm:
                        enumerate_futures = tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc=f"Running {path_name} batch")
                    else:
                        enumerate_futures = concurrent.futures.as_completed(futures)
                    for future in enumerate_futures:
                        result, batch_id, usage_env = future.result()
                        if usage_env is not None:
                            local_env.merge_usage(usage_env)
                            # local_env.replace(usage_env, merge_usage=True, merge_dtypes=False)
                        batch_results[batch_id] = result
            
            case ExecutionType.MULTIPLE_THREADS:
                with concurrent.futures.ThreadPoolExecutor(max_workers=batch.max_workers) as executor:
                    usage_tracking_batch = list(batch.keys())[0]
                    session_maker = scoped_session(sessionmaker(bind=self.engine))
                    futures = [executor.submit(
                        run_batch_id, 
                        self.model_copy(update={"engine": None, "session": None}), 
                        local_env.model_copy(deep=True), 
                        path_name, path_options, 
                        batch_id, batch_env, batch_id==usage_tracking_batch, 
                        session_maker=session_maker)
                                for batch_id, batch_env in batch.items()]
                    if path_options.batch_tqdm:
                        enumerate_futures = tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc=f"Running {path_name} batch")
                    else:
                        enumerate_futures = concurrent.futures.as_completed(futures)
                    for future in enumerate_futures:
                        result, batch_id, usage_env = future.result()
                        if usage_env is not None:
                            local_env.merge_usage(usage_env)
                            # local_env.replace(usage_env, merge_usage=True, merge_dtypes=False)
                        batch_results[batch_id] = result
            
            case ExecutionType.SINGLE_PROCESS:
                # Iterate through each element of the batch sequentially.
                if path_options.batch_tqdm:
                    enumerate_batch = tqdm(
                        batch.items(),
                        total=len(batch),
                        desc=f"Running {path_name} batch",
                    )
                else:
                    enumerate_batch = batch.items()
                update_local_env = True
                for batch_id, batch_env in enumerate_batch:
                    path_env = local_env.model_copy(deep=True)
                    batch_env.init_run(is_parent_path=True, parent_env=path_env)
                    path_env.replace(batch_env)
                    result, _ = self._run(
                        path_env, path_name, path_options, is_parent=False
                    )
                    # Update parameter usage according to subpath usage.
                    if update_local_env:
                        # These are dependent parameters, so don't count towards usage.
                        batch_env.reset_usage()
                        local_env.merge_usage(path_env)
                        # local_env.replace(path_env, merge_usage=True, merge_dtypes=False)
                        update_local_env = False

                    # Save the results of the subpath.
                    batch_results[batch_id] = result
        if single_run:
            batch_results = batch_results["default"]
        return batch_results
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
        path_version_num = None
        env_schema = None
        if self.cache_db_meta and path_name in self.db_current_path_versions:
            path_version_num = self.db_current_path_versions[path_name]
        else:
            path_stmt = select(DBPath).where(DBPath.name == path_name)
            path = self.session.execute(path_stmt).scalar_one_or_none()
            
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
            path_version = self.session.execute(version_stmt).scalar_one_or_none()
            if path_version is None:
                return None
            env_schema = path_version.env_schema
            file_schema = path_version.file_schema
            if self.cache_db_meta:
                self.db_current_path_env_schemas[path_name] = env_schema
                self.db_current_path_file_schemas[path_name] = file_schema
        sql_env: dict[str, Any] = {}
        for param_used in env_schema.keys():
            param_path = param_used.split(REF_SEP)
            # print(param_path)
            jparam = local_env
            for i, key in enumerate(param_path):
                while not isinstance(jparam, JDict):
                    jparam = jparam.jparam
                if i == len(param_path) - 1:
                    if key == 'jvar':
                        # Bypass get_value to get the actual param
                        jvar = jparam.data[key].get_value()
                        param_value = jvar.__module__ + "." + jvar.__qualname__
                    else:
                        jparam[key] # Trigger the usage of the parameter
                        param_value = jparam.data[key].get_sql_data()
                else:
                    jparam = jparam.data[key]
                    jparam.used = True
            sql_env[param_used] = param_value
        result_stmt = select(DBResult).where(
            DBResult.path_name == path_name,
            DBResult.path_version_num == path_version_num,
            DBResult.environment == sql_env,
            DBResult.created_at == Null(),
            DBResult.completed == True,
        )
        db_result = self.session.execute(result_stmt).scalar_one_or_none()
        if db_result is None:
            local_env.reset_usage()
            return None
        result = PathResult(sql=db_result.data, db_entry=db_result, completed=db_result.completed)
        file_path = (self.result_directory / path_name / db_result.file_name) if db_result.file_name is not None else None

        file_loaded = result.from_file(
            file_path,
            file_schema,
        )
        if not file_loaded:
            local_env.reset_usage()
            return None
        return result

    def save_path_results(self, local_env: JDict, path_name: str, result: PathResult):
        """Persist the results of a path into the cache database.

        Args:
            local_env: Environment used to generate the results.
            path_name: Name of the path.
            result: Results of the path run.
        """
        env_sql = local_env.get_sql_data(show_unused=False, show_invisible=False)
        env_schema = get_sql_schema(env_sql)
        file_name = get_filename(env_sql)
        file_path = self.result_directory / path_name / file_name
        file_schema = result.to_file(file_path)
        file_schema = file_schema if file_schema is not None else Null()
        # Check if a DBPath already exists with this name.
        path_stmt = select(DBPath).where(DBPath.name == path_name)
        path = self.session.execute(path_stmt).scalar_one_or_none()
        if path is None:
            path = DBPath(
                name=path_name,
                current_version=None,
                description=self.paths[path_name].changelog,
            )
            self.session.add(path)
            self.session.commit()
        version_stmt = select(DBPathVersion).where(
            DBPathVersion.name == path_name,
            DBPathVersion.env_schema == env_schema,
            DBPathVersion.file_schema == file_schema,
        )
        path_version = self.session.execute(version_stmt).scalar_one_or_none()

        if path_version is None:
            # Find the latest version number for this path_name.
            max_version_stmt = (
                select(DBPathVersion.version)
                .where(DBPathVersion.name == path_name)
                .order_by(DBPathVersion.version.desc())
                .limit(1)
            )
            max_version_result = self.session.execute(max_version_stmt).scalar_one_or_none()
            next_version = 0 if max_version_result is None else (max_version_result + 1)
            # Create a new DBPathVersion entry.
            path_version = DBPathVersion(
                name=path_name,
                changelog=self.paths[path_name].changelog,
                version=next_version,
                env_schema=env_schema,
                file_schema=file_schema,
            )
            self.session.add(path_version)
            self.session.commit()
        path_version_num = path_version.version
        if result.completed:
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
                file_name=file_name if not isinstance(file_schema, Null) else Null(),
                created_at=datetime.now(timezone.utc),
                completed=result.completed,
            )
            self.session.add(db_result)
        else:
            result_stmt = select(DBResult).where(
                DBResult.path_name == path_name,
                DBResult.path_version_num == path_version_num,
                DBResult.environment == env_sql,
                DBResult.created_at == Null(),
            )
            db_result = self.session.execute(result_stmt).scalar_one_or_none()
            if db_result is None:
                db_result = DBResult(
                    environment=env_sql,
                    data=result.sql if result.sql is not None else Null(),
                    path_name=path_name,
                    path_version_num=path_version_num,
                    file_name=file_name if not isinstance(file_schema, Null) else Null(),
                    created_at=Null(),
                    completed=result.completed,
                )
                self.session.add(db_result)
            else:
                db_result.data = result.sql
                db_result.file_name = file_name if not isinstance(file_schema, Null) else Null()
                db_result.completed = result.completed
        result.db_entry = db_result
        # self.session.commit()

    # Overrides
    def get_str(self) -> str:
        """Return a human-readable string representation of the Journey."""
        # string = f"Journey({self.name})\n"
        string = "Paths:\n"
        for path_name, path in self.paths.items():
            string += f"   {path_name}"
            if len(path.subpaths) > 0:
                string += ", Subpaths: " + ", ".join(path.subpaths)
        return string

    def __str__(self) -> str:
        """Return :meth:`get_str` for ``str(journey)``."""
        return self.get_str()


def run_batch_id(journey, subpath_env: JDict, subpath_name: str, subpath_options: PathOptions, batch_id:str, batch_env: JBatch, return_environment: bool, session_maker=None):
    # subpath_env = env.model_copy(deep=True)
    if session_maker is not None:
        journey.session = session_maker()
        journey.engine = True
    batch_env.init_run(is_parent_path=True, parent_env=subpath_env) # Resolve any references
    subpath_env.replace(batch_env)
    subpath_result, _ = journey._run(
        subpath_env, subpath_name, subpath_options, is_parent=False
    )
    if return_environment:
        batch_env.reset_usage()
    else:
        subpath_env = None
    if session_maker is not None:
        journey.session.commit()
        journey.session.close()
        journey.session = None
        journey.engine = None
    return subpath_result, batch_id, subpath_env