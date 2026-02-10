"""Journey orchestration and caching.

Provides the `Journey` class for composing paths, validating dependencies,
running subpaths, caching results, and evaluating/ploting outputs.
"""
from tkinter import S
from typing import Union, Any, Dict
from jmaps.config import PATH
from jmaps.journey.jmalc import cast_sql_type, get_sql_schema, create_tables, DBPath, DBPathVersion, DBResult
from jmaps.journey.path import JPath, JBatch, PathResult
from jmaps.journey.param import REF_SEP, JDict
from pathlib import Path
from datetime import datetime, timezone
# from jmaps.journey.environment import JEnv
# from jmaps.journey import jmalc
from numpy import isin
from tqdm import tqdm
import copy
import hashlib
import json
from pydantic import BaseModel, Field
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import select, Null
from sqlalchemy.engine import Engine
class PathOptions(BaseModel):
    force_run_to_depth: int = Field(0, description="Force the path tree to run even if cached. Most useful when a path has been changed but the outdated cached result still exists. Because subpaths form a tree with the main path at the top, this is specified as an integer stating to what depth in the tree a path should be forced to run.")
    disable_saving_and_loading: bool = Field(False, description="If true, results are not saved to or loaded from the database.")
    plot: bool = Field(True, description="Plot the results after running")
    verbose: bool = Field(False, description="Print verbose output")
    batch_tqdm: bool = Field(False, description='Track the batch progress with tqdm')

    class Config:
        extra = "forbid"
        validate_assignment = True
def get_filename(hashable:dict):
    """Compute a deterministic cache filename from environment values.

    Args:
        envs: Mapping of env name to `JDict`.
        cache_dir: Directory where cache files are stored.

    Returns:
        Path to a `.dill` file uniquely representing the env values.
    """
    
    dumped = json.dumps(hashable, sort_keys=True, separators=(',', ':'))
    key = hashlib.sha256(dumped.encode('utf-8')).hexdigest()
    return key

class Journey(BaseModel):
    """Executable container for environments and paths.

    Manages validation, caching, dependency execution, and convenience helpers
    for running and introspecting complex multi-step processes.
    """
    name: str = Field(..., description='Name describing the journey, encompassing all paths that will be run.')
    env: JDict = Field(default_factory=JDict)
    paths: Dict[str, JPath] = Field(default_factory=dict)
    result_directory: Path = Field(..., description='Where the file results of this journey are stored')
    engine: Any = Field(...)
    session_factory: Any = Field(...)
    Session: Any = Field(..., description='Creates connections to the SQL server.') 
    def __init__(self, name, engine: Engine, env: JDict | None = None, paths: Union[dict[str, JPath], list[JPath]] | None = None, result_directory: Path | None = None):
        """Journey is a class that represents a journey.
        It contains a dictionary of environments and a dictionary of paths.
        
        Args:
            name (str): The name of the journey.
            envs (Union[dict[str, JDict], list[JDict]]): A dictionary or list of environments, where the key is the name of the environment. If a dictionary is provided, the keys should match the names of the environments.
            paths (Union[dict[str, JPath], list[JPath]]): A dictionary or list of paths, where the key is the name of the path. If a dictionary is provided, the keys should match the names of the paths.
        """
        if paths is None:
            paths = {}
        if isinstance(paths, list):
            paths = {path.name: path for path in paths}
        result_directory = result_directory if result_directory is not None else PATH.journeys / name
        result_directory.mkdir(parents=True, exist_ok=True)

        # Ensure DB tables exist (no-op if they already do)
        create_tables(engine)
        session_factory = sessionmaker(bind=engine)
        Session = scoped_session(session_factory)

        for path_name, path in paths.items():
            path_dir = result_directory / path_name
            path_dir.mkdir(parents=False, exist_ok=True)
        
        super().__init__(name=name, engine=engine, session_factory=session_factory, Session=Session, env=env, paths=paths, result_directory=result_directory)

    def add_path(self, path: JPath, validate: bool=True):
        '''Adds a path, optionally validating that all path environments are defined in the journey.'''
        # if path.name in self.paths:
        #     raise ValueError(f"Path {path.name} already exists in Journey {self.name}")
        self.paths[path.name] = path
        if validate:
            self.validate_paths(error=True)
        path_dir = self.result_directory / path.name
        path_dir.mkdir(parents=False, exist_ok=True)

    def add_paths(self, new_paths: list[JPath], validate: bool=True):
        '''Adds a list of paths'''
        for path in new_paths:
            self.add_path(path, validate=False)
        if validate:
            self.validate_paths(error=True)
    def get_path(self, name):
        '''Returns the path with the given name.'''
        return self.paths[name]
    def get_paths(self):
        '''Returns the paths.'''
        return self.paths

    def circular_subpaths(self, path_name: str, paths_prior: list[str] | None = None) -> bool:
        """Checks if the subpaths of a path are circular by building a tree of paths.
        
        Args:
            path_name (str): The name of the path to check.
            paths_prior (list[str] | None): A list of paths that have already been called.
        
        Returns:
            list: empty if no circular subpaths found, else the path that becomes circular
        """
        if paths_prior is None:
            paths_prior = []
        if path_name in paths_prior:
            return paths_prior + [path_name]
        paths_prior.append(path_name)
        for subpath_name in self.paths[path_name].subpaths:
            if subpath_name in self.paths:
                circular_path = self.circular_subpaths(subpath_name, copy.copy(paths_prior))
                if len(circular_path) > 0:
                    return circular_path
        return []
        

    def validate_path(self, path_name: str, error: bool=True, verbose: bool=True) -> tuple[list[str], list[str], list[str]]:
        """Validates the path by checking that all environments and subpaths used in the path are defined in the journey.
        
        Args:
            path_name (str): The name of the path to validate.
            error (bool): Whether to raise an error if the path is invalid.
            verbose (bool): Whether to print a message if the path is invalid.
        
        Returns:
            missing_envs (list[str]): A list of environment names that are missing from the journey.
            missing_subpaths (list[str]): A list of subpath names that are missing from the journey.
        """
        path = self.paths[path_name]
        missing_subpaths = []
        missing_batched_subpaths = []

        for subpath_name in path.subpaths:
            if subpath_name not in self.paths:
                missing_subpaths.append(subpath_name)
        circular_path = self.circular_subpaths(path_name)
        for subpath_name in path.batched_subpaths:
            if subpath_name not in self.paths:
                missing_batched_subpaths.append(subpath_name)
        if (len(missing_subpaths) > 0 or len(missing_batched_subpaths) > 0) and (error or verbose):
            error_string = f""
            if len(missing_subpaths) > 0:
                error_string += f"{path_name} is missing subpath(s): {', '.join(missing_subpaths)}"
            if len(missing_batched_subpaths) > 0:
                error_string += f"{path_name} is missing batched subpath(s): {', '.join(missing_batched_subpaths)}"
            if len(circular_path) > 0:
                error_string += f"{path_name} is circular: {', '.join(circular_path)}"
            if error:
                raise ValueError(error_string)
            else:
                if verbose:
                    print(error_string)
        return missing_subpaths, missing_batched_subpaths, circular_path

    def validate_paths(self, error: bool=True):
        '''Validates the paths by checking that all environments and subpaths used in the paths are defined in the journey.'''
        error_string = f"Invalid paths"
        invalid=False
        for path_name, path in self.paths.items():
            missing_subpaths, missing_batched_subpaths, circular_path = self.validate_path(path_name, error=False, verbose=False)
            # If any envs or subpaths are missing, add to invalid paths string
            if len(missing_subpaths) > 0:
                error_string += f"\n{path_name} is missing subpath(s): {', '.join(missing_subpaths)}"
                invalid=True
            if len(missing_batched_subpaths) > 0:
                error_string += f"\n{path_name} is missing batched subpath(s): {', '.join(missing_batched_subpaths)}"
                invalid=True
            if len(circular_path) > 0:
                error_string += f"\n{path_name} is circular with: {', '.join(circular_path)}"
                invalid=True
        # If any paths are invalid, raise an error/warning
        if invalid:
            if error:
                raise ValueError(error_string)
            else:
                print(error_string)
    def run(self, path_name: str, path_options: PathOptions):
        if path_name not in self.paths:
            raise ValueError(f"The path '{path_name}' does not exist in this Journey")
        local_env = self.env.model_copy(deep=True)
        return self._run(local_env, path_name, path_options, is_parent=True)

    def _run(self, local_env:JDict, path_name: str, path_options:PathOptions, is_parent: bool = False):
        """Runs a path, raising an error if the path is not found in the journey.
        
        Args:
            path_name (str): The name of the path to run.
            force_run (bool): Whether to force the path to run even if the environment matches a cached result.
            use_cache (bool): If false, the results will not be loaded or saved to the cache.
            plot (bool): Whether to plot the path results.
            verbose (bool): Whether to print verbose output.
            force_subpath_run (int): The depth at which to force the subpaths to run even if their results match a cached result. 0 - no forced subpath run, 1 - force the subpaths to run, 2 - force the subpaths to run and their subpaths to run, etc.
            lock_lambdas (bool): If true, the lambda parameters will not be reevaluated for the subpaths. For example, a lambda parameter that returns the time will return the same time for the path and all subpaths.
        Returns:
            result: The results of the path.
            subpath_results: The results of the subpaths. For each subpath run with a batch, the value will be a nested dictionary of the results for each batch id.
            cache_filepath: The filepath to the cache file. In the future, this might also be a row in a database.
        """

        local_env.init_run(is_parent)
        local_env.reset_usage()

        result = None
        if path_options.force_run_to_depth == 0 and not path_options.disable_saving_and_loading:
            result = self.load_path_results(local_env, path_name)
        if result is not None:
            if path_options.verbose:
                print(f"Obtained prior result: {result}")
            # Don't load another recursion of subpaths if we are a subpath already
            if not is_parent:
                return result, None
            
        subpath_options = path_options.model_copy()
        subpath_options.force_run_to_depth = subpath_options.force_run_to_depth - 1 if subpath_options.force_run_to_depth > 0 else 0
        subpath_results = self.run_subpaths(local_env, path_name, path_options)
        if result is None:
            # Run the path
            result = self.paths[path_name].run(local_env, subpath_results, path_options.verbose)
            # Save the results to cache
            if not path_options.disable_saving_and_loading:
                self.save_path_results(local_env, path_name, result)
        # plot the path results
        if path_options.plot:
            self.paths[path_name].plot(result, subpath_results)
        return result, subpath_results

    def run_subpaths(self, local_env:JDict, path_name: str, subpath_options:PathOptions):
        """Packs your backpack to set out on a path! Returns whatever you need to run the path, but if you've done it already, returns the results of the path.
        Note that this will run subpaths required by the path, including batched subpaths.
        
        Args:
            local_env (JDict): The environment needed by the path.
            path_name (str): The name of the path to run.
        Returns:
            subpath_results (dict[str, PathResult|dict[str, PathResult]]): The results of the subpaths. For each subpath run with a batch, the value will be a nested dictionary of the results for each batch id.
        """
        # Get the environments that are used in the path and its subpaths
        subpath_results = {}
        # Run the subpaths, and retrieve the files their results are stored in
        for subpath_name in self.paths[path_name].subpaths:
            # Only run the subpaths that are not batched
            batches = self.paths[path_name].get_batches(subpath_name, local_env, subpath_results)
            if batches is None:
                subpath_env = local_env.model_copy(deep=True)
                subpath_result, _ = self._run(subpath_env, subpath_name, subpath_options, is_parent=False)
                subpath_results[subpath_name] = subpath_result
                local_env.merge_usage(subpath_env)
            else:
                subpath_results[subpath_name] = {}
                # Iterate through each element of the batch
                if subpath_options.batch_tqdm:
                    enumerate_batch = tqdm(batches.items(), total=len(batches), desc=f"Running {subpath_name} batches")
                else:
                    enumerate_batch = batches.items()
                update_local_env = True
                for batch_id, batch_env in enumerate_batch:
                    subpath_env = local_env.model_copy(deep=True)
                    batch_env.init_run(is_parent_path=True, parent_env=subpath_env)
                    subpath_env.replace(batch_env)
                    subpath_result, _ = self._run(subpath_env, subpath_name, subpath_options, is_parent=False)
                    # Update parameter usage according to subpath usage
                    if update_local_env:
                        batch_env.reset_usage() # These are dependent parameters, so don't count towards usage
                        local_env.merge_usage(subpath_env)

                    # Save the results of the subpath
                    subpath_results[subpath_name][batch_id] = subpath_result
        return subpath_results


    def load_path_results(self, local_env: JDict, path_name:str):
        """Loads the results of a path from cache.
        
        Args:
            cache_filepath (Path): The filepath to the cache file.
            subpath_exists_error (bool): Whether to raise an error if a subpath file is not found.
        
        Returns:
            result: The results of the path. None if the path is not in the cache, or if a subpath file is not found.
            
        """
        if self.paths[path_name].save_datetime:
            return None
        session = self.Session()
        path_stmt = select(DBPath).where(DBPath.name == path_name)
        path = session.execute(path_stmt).scalar_one_or_none()
        if path is None or path.current_version is None:
            return None
        
        version_stmt = select(DBPathVersion).where(DBPathVersion.name == path_name, DBPathVersion.version == path.current_version)
        path_version = session.execute(version_stmt).scalar_one_or_none()
        if path_version is None:
            return None
        env_schema = path_version.env_schema
        temp_env = {}
        for param_used in env_schema.keys():
            param_path = param_used.split(REF_SEP)
            jparam = local_env
            dtype = None
            for i, key in enumerate(param_path):
                if i == len(param_path)-1:
                    dtype = jparam.data[key].dtype
                jparam = jparam[key]
            temp_env[param_used] = cast_sql_type(jparam) if dtype is None else dtype(jparam)
            
        result_stmt = select(DBResult).where(
            DBResult.path_name == path_name,
            DBResult.path_version == path_version.version,
            DBResult.environment == temp_env,
            DBResult.created_at == Null(),
        )
        db_result = session.execute(result_stmt).scalar_one_or_none()
        if db_result is None:
            local_env.reset_usage()
            return None
        result = PathResult(sql=db_result.data)
        result.from_file(Path(db_result.file_path) if db_result.file_path is not None else None, path_version.file_schema)
        return result

    def save_path_results(self, local_env: JDict, path_name: str, result: PathResult):
        """Saves the results of a path to cache (the database).

        Args:
            local_env (JDict): The environment used to generate the results.
            path_name (str): The name of the path.
            result (PathResult): The results of the path.
        """
        session = self.Session()
        env_sql = local_env.get_sql_data(show_unused=False, show_invisible=False)
        env_schema = get_sql_schema(env_sql)

        file_path = self.result_directory / get_filename(env_sql)
        file_schema = result.to_file(file_path)
        file_schema = file_schema if file_schema is not None else Null()
        # Check if a DBPath already exists with this name
        path_stmt = select(DBPath).where(DBPath.name == path_name)
        path = session.execute(path_stmt).scalar_one_or_none()
        if path is None:
            path = DBPath(name=path_name, current_version=None)
            session.add(path)
            session.commit()

        version_stmt = select(DBPathVersion).where(DBPathVersion.name == path_name, DBPathVersion.env_schema==env_schema, DBPathVersion.file_schema==file_schema)
        path_version = session.execute(version_stmt).scalar_one_or_none()

        if path_version is None:
            # Find the latest version number for this path_name
            max_version_stmt = (
                select(DBPathVersion.version)
                .where(DBPathVersion.name == path_name)
                .order_by(DBPathVersion.version.desc())
                .limit(1)
            )
            max_version_result = session.execute(max_version_stmt).scalar_one_or_none()
            next_version = 0 if max_version_result is None else (max_version_result + 1)
            # Create a new DBPath entry
            path_version = DBPathVersion(
                name=path_name,
                version=next_version,
                env_schema=env_schema,
                file_schema=file_schema
            )
            session.add(path_version)
            session.commit()
        path.current_version = path_version.version
        # Add new DBResult entry, linking to the path_version
        if self.paths[path_name].save_datetime:
            db_result = DBResult(
                environment=env_sql,
                data=result.sql if result.sql is not None else Null(),
                path_name=path_name,
                path_version=path_version.version,
                file_path=str(file_path) if not isinstance(file_schema, Null) else Null(),
                created_at=datetime.now(timezone.utc)
            )
            session.add(db_result)
        else:
            result_stmt = select(DBResult).where(
                DBResult.path_name == path_name,
                DBResult.path_version == path_version.version,
                DBResult.environment == env_sql,
                DBResult.created_at == Null(),
            )
            db_result = session.execute(result_stmt).scalar_one_or_none()
            if db_result is None:
                db_result = DBResult(
                    environment=env_sql,
                    data=result.sql if result.sql is not None else Null(),
                    path_name=path_name,
                    path_version=path_version.version,
                    file_path=str(file_path) if file_path is not None else Null(),
                    created_at=Null()
                )
                session.add(db_result)
            else:
                db_result.data = result.sql
                db_result.file_path = str(file_path) if file_path is not None else None
        session.commit()
    # Overrides
    def get_str(self):
        '''Returns a string representation of the journey.'''
        string = f"Journey({self.name})\n"
        string += f"Environment:\n"
        string += str(self.env.data)
        string += "\n"
        string += f"Paths:\n"
        for path_name, path in self.paths.items():
            string += f"   {path_name}"
            if len(path.subpaths) > 0:
                string += f', Subpaths: ' + ', '.join(path.subpaths) 
        return string

    def __str__(self):
        return self.get_str()