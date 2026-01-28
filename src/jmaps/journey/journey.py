"""Journey orchestration and caching.

Provides the `Journey` class for composing paths, validating dependencies,
running subpaths, caching results, and evaluating/ploting outputs.
"""
from typing import Union, Any
from jmaps.config import PATH
from jmaps.journey.path import JPath, JBatch
from pathlib import Path
from jmaps.journey.environment import JEnv
from jmaps.journey import jmalc
from tqdm import tqdm
import copy
import hashlib
import dill
import json
from pydantic import BaseModel, Field, model_copy

class PathOptions(BaseModel):
    force_run_to_depth: int = Field(0, description="Force the path tree to run even if cached. Most useful when a path has been changed but the outdated cached result still exists. Because subpaths form a tree with the main path at the top, this is specified as an integer stating to what depth in the tree a path should be forced to run.")
    use_cache: bool = Field(True, description="Use cache for results")
    plot: bool = Field(True, description="Plot the results after running")
    verbose: bool = Field(False, description="Print verbose output")
    lock_lambdas: bool = Field(False, description="Lock lambda evaluation results")

    class Config:
        extra = "forbid"
        validate_assignment = True
def get_cache_filepath(env: JEnv, cache_dir: Path):
    """Compute a deterministic cache filename from environment values.

    Args:
        envs: Mapping of env name to `JEnv`.
        cache_dir: Directory where cache files are stored.

    Returns:
        Path to a `.dill` file uniquely representing the env values.
    """
    hashable = env.get_visible_values()
    dumped = json.dumps(hashable, sort_keys=True, separators=(',', ':'), default=lambda o: o.__repr__())
    key = hashlib.sha256(dumped.encode('utf-8')).hexdigest()

    # hashable = {name: env.get_hashable_values() for name, env in envs.items()}
    # dumped = dill.dumps(hashable, protocol=dill.HIGHEST_PROTOCOL)
    # key = hashlib.sha256(dumped).hexdigest()

    cache_filepath = cache_dir / f"{key}.dill"
    return cache_filepath

class Journey:
    """Executable container for environments and paths.

    Manages validation, caching, dependency execution, and convenience helpers
    for running and introspecting complex multi-step processes.
    """
    def __init__(self, name, env: JEnv | None = None, paths: Union[dict[str, JPath], list[JPath]] | None = None, parent_cache_dir: Path | None = None):
        """Journey is a class that represents a journey.
        It contains a dictionary of environments and a dictionary of paths.
        
        Args:
            name (str): The name of the journey.
            envs (Union[dict[str, JEnv], list[JEnv]]): A dictionary or list of environments, where the key is the name of the environment. If a dictionary is provided, the keys should match the names of the environments.
            paths (Union[dict[str, JPath], list[JPath]]): A dictionary or list of paths, where the key is the name of the path. If a dictionary is provided, the keys should match the names of the paths.
        """
        if envs is None:
            envs = {}
        if paths is None:
            paths = {}
        if isinstance(envs, dict):
            self.envs = envs
        elif isinstance(envs, list):
            self.envs = {env.name: env for env in envs}
        else:
            raise TypeError(f"Invalid type for envs: {type(envs)}")
        if isinstance(paths, dict):
            self.paths = paths
        elif isinstance(paths, list):
            self.paths = {path.name: path for path in paths}
        else:
            raise TypeError(f"Invalid type for paths: {type(paths)}")
        self.name = name
        self.parent_cache_dir = parent_cache_dir if parent_cache_dir is not None else PATH.cache / name
        self.parent_cache_dir.mkdir(parents=True, exist_ok=True)
    def update_params(self, params):
        self.env.params.update(params)
    def add_path(self, path: JPath, validate: bool=True):
        '''Adds a path, optionally validating that all path environments are defined in the journey.'''
        # if path.name in self.paths:
        #     raise ValueError(f"Path {path.name} already exists in Journey {self.name}")
        self.paths[path.name] = path
        if validate:
            self.validate_paths(error=True)
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
        '''Returns a copy of the paths.'''
        return self.paths.copy()

    def circular_subpaths(self, path_name: str, paths_prior: list[str] | None = None) -> bool:
        """Checks if the subpaths of a path are circular by building a tree of paths.
        
        Args:
            path_name (str): The name of the path to check.
            paths_prior (list[str] | None): A list of paths that have already been called.
        
        Returns:
            bool: True if the subpaths are circular, False otherwise.
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
        if self.paths[path_name].subpath_batch_envs is not None:
            for subpath_name, batch_envs in path.subpath_batch_envs.items():
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
            raise ValueError(f"{path_name} is not a valid path in this Journey")
        local_env = self.env.copy()
        local_env.reset_one_shot_values()
        return self._run(local_env, path_name, path_options)

    def _run(self, local_env:JEnv, path_name: str, path_options:PathOptions):
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
        if not path_options.lock_lambdas:
            local_env.reset_one_shot_values()


        result, subpath_results = (None, None)
        cache_filepath = None
        if path_options.use_cache:
            cache_dir = self.parent_cache_dir / path_name
            cache_dir.mkdir(parents=True, exist_ok=True)

            cached_env_names = self.get_cached_env_names(path_name)
            cached_envs = {name: env for name, env in self.envs.items() if name in cached_env_names}
            cache_filepath = get_cache_filepath(cached_envs, cache_dir)

            # Load the results from cache
            if path_options.force_run_to_depth == 0:
                result, subpath_results = self.load_path_results(cache_filepath, subpath_exists_error=False)

        if result is None:
            subpath_options = model_copy(path_options)
            subpath_options.force_run_to_depth = subpath_options.force_run_to_depth - 1 if subpath_options.force_run_to_depth > 0 else 0
            subpath_results, subpath_files = self.run_subpaths(local_env, path_name, path_options)
            if path_options.verbose:
                print(f"Packed for new {path_name} run to: {cache_filepath}")
            # Run the path
            result = self.paths[path_name].run(local_env, subpath_results, path_options.verbose)
            # Save the results to cache
            if path_options.use_cache:
                self.save_path_results(result, subpath_files, cache_filepath)
        # plot the path results
        if path_options.plot:
            self.paths[path_name].plot(result, subpath_results)
        return result, subpath_results, cache_filepath

    def run_subpaths(self, local_env:JEnv, path_name: str, subpath_options:PathOptions):
        """Packs your backpack to set out on a path! Returns whatever you need to run the path, but if you've done it already, returns the results of the path.
        Note that this will run subpaths required by the path, including batched subpaths.
        
        Args:
            local_env (JEnv): The environment needed by the path.
            path_name (str): The name of the path to run.
            use_cache (bool): If false, the results will not be loaded or saved to the cache.
            verbose (bool): Whether to print verbose output.
            force_run (int): The depth at which to force the subpaths to run even if their results match a cached result. 0 - no forced subpath run, 1 - force the subpaths to run, 2 - force the subpaths to run and their subpaths to run, etc.
            get_batches (bool): If true, runs the normal subpaths and returns the batches that will be run.
            lock_lambdas (bool): If true, the lambda parameters will not be reevaluated for the subpaths. For example, a lambda parameterthat returns the time will return the same time for the path and all subpaths.
        Returns:
            subpath_results (dict[str, Any]): The results of the subpaths. For each subpath run with a batch, the value will be a nested dictionary of the results for each batch id.
            subpath_files (dict[str, str]): The file paths that contain the results of the subpaths. For each subpath run with a batch, the value will be a nested dictionary of the file paths for each batch id.
        """
        # Get the environments that are used in the path and its subpaths
        subpath_results = {}
        subpath_files = {}
        # Run the subpaths, and retrieve the files their results are stored in
        for subpath_name in self.paths[path_name].subpaths:
            # Only run the subpaths that are not batched
            batch = self.paths[path_name].subpath_batches(subpath_name, local_env, subpath_results)
            if batch is None:
                subpath_result, _, subpath_filename = self._run(local_env, subpath_name, subpath_options)
                subpath_results[subpath_name] = subpath_result
                subpath_files[subpath_name] = subpath_filename
            else:
                subpath_results[subpath_name] = {}
                subpath_files[subpath_name] = {}
                # Iterate through each element of the batch
                if self.paths[path_name].subpath_tqdm:
                    enumerate_batch = tqdm(batch.items(), total=len(batch), desc=f"Running {subpath_name} batches")
                else:
                    enumerate_batch = batch.items()
                for batch_id, batch_env in enumerate_batch:
                    subpath_env = model_copy(local_env)
                    subpath_env.update(batch_env)
                    subpath_env.reset_param_usage()
                    spb_result, _, spb_filename = self._run(subpath_env, subpath_name, subpath_options)
                    # Update parameter usage according to subpath usage
                    for param_name, used in subpath_env._param_usage:
                        if used and param_name not in batch_env.params:
                            local_env._param_usage[param_name] = True
                    # Save the results of the subpath
                    subpath_results[subpath_name][batch_id] = spb_result
                    subpath_files[subpath_name][batch_id] = spb_filename
            return subpath_results, subpath_files

    def load_path_results(self, cache_filepath: Path, subpath_exists_error: bool=False):
        """Loads the results of a path from cache.
        
        Args:
            cache_filepath (Path): The filepath to the cache file.
            subpath_exists_error (bool): Whether to raise an error if a subpath file is not found.
        
        Returns:
            result: The results of the path. None if the path is not in the cache, or if a subpath file is not found.
            subpath_results: A dictionary of the results of the subpaths. None if the path is not in the cache, or if a subpath file is not found.
        """
        if cache_filepath.exists():
            with open(cache_filepath, "rb") as f:
                dill_dict = dill.load(f)

            # Load the subpaths, or batches of subpaths
            subpath_results = {}
            for subpath_name, subpath_file in dill_dict['subpath_files'].items():
                if isinstance(subpath_file, dict):
                    subpath_results[subpath_name] = {}
                    # Load each file in the subpath batch
                    for batch_id, batch_file in subpath_file.items():
                        if not batch_file.exists():
                            if not subpath_exists_error:
                                print(f"Batch file {batch_file} not found for subpath {subpath_name}, batch {batch_id}. This will cause the parent path to be rerun.")
                            else:
                                raise FileNotFoundError(f"Batch file {batch_file} not found for subpath {subpath_name}, batch {batch_id}. This will cause the parent path to be rerun.")
                            return None, None
                        else:
                            with open(batch_file, "rb") as f:
                                subpath_results[subpath_name][batch_id] = dill.load(f)['result']
                else:
                    # Load the subpath file
                    if not subpath_file.exists():
                        if not subpath_exists_error:
                            print(f"Subpath file {subpath_file} not found for {subpath_name}. This will cause the parent path to be rerun.")
                        else:
                            raise FileNotFoundError(f"Subpath file {subpath_file} not found for {subpath_name}. This will cause the parent path to be rerun.")
                        return None, None
                    else:
                        with open(subpath_file, "rb") as f:
                            subpath_results[subpath_name] = dill.load(f)['result']
            return dill_dict['result'], subpath_results
        return None, None

    def save_path_results(self, result:Any, subpath_files: dict[str, Path | dict[str, Path]], cache_filepath: Path):
        """Saves the results of a path to cache.
        
        Args:
            result: The results of the path.
            subpath_files: A dictionary of the files that contain the results of the subpaths. If a subpath is run with a batch, the value will be a nested dictionary of the files for each batch id.
            cache_filepath: The filepath to the cache file.
            verbose: Whether to print verbose output.
        """
        with open(cache_filepath, "wb") as f:
            dill_dict = {
                'result': result,
                'subpath_files': subpath_files
            }
            dill.dump(dill_dict, f)

    # Overrides
    def get_str(self):
        '''Returns a string representation of the journey.'''
        string = f"Journey({self.name})\n"
        string += f"Environment:\n"
        param_list = [f'{name} ({type(value)},' + \
                        'lambda triggered,' if name in self.env.lambda_triggers else '' + \
                        'invisible' if name in self.env.invisible_params else '' +\
                         ')' for name, value in self.env.items()]
        string += "\n   ".join(param_list)
        string += "\n"
        string += f"Paths:\n"
        for path_name, path in self.paths.items():
            string += f"   {path_name}"
            if len(path.subpaths) > 0:
                string += f', Subpaths: ' + ', '.join(path.subpaths) 
        return string

    def __str__(self):
        return self.get_str()