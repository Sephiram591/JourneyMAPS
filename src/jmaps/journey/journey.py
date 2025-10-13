"""Journey orchestration and caching.

Provides the `Journey` class for composing paths, validating dependencies,
running subpaths, caching results, and evaluating/pondering outputs.
"""
from typing import Union, Any
from jmaps.config import PATH
from jmaps.journey.path import JPath, JBatch
from pathlib import Path
from jmaps.journey.environment import JEnv
from jmaps.journey import jmalc
from sortedcontainers import SortedSet
import copy
import hashlib
import dill

def get_cache_filepath(envs: dict[str, JEnv], cache_dir: Path):
    """Compute a deterministic cache filename from environment values.

    Args:
        envs: Mapping of env name to `JEnv`.
        cache_dir: Directory where cache files are stored.

    Returns:
        Path to a `.dill` file uniquely representing the env values.
    """
    hashable = {name: env.get_hashable_values() for name, env in envs.items()}
    dumped = dill.dumps(hashable, protocol=dill.HIGHEST_PROTOCOL)
    key = hashlib.sha256(dumped).hexdigest()
    cache_filepath = cache_dir / f"{key}.dill"
    return cache_filepath

class Journey:
    """Executable container for environments and paths.

    Manages validation, caching, dependency execution, and convenience helpers
    for running and introspecting complex multi-step processes.
    """
    def __init__(self, name, envs: Union[dict[str, JEnv], list[JEnv]] | None = None, paths: Union[dict[str, JPath], list[JPath]] | None = None, parent_cache_dir: Path | None = None):
        '''
        Journey is a class that represents a journey.
        It contains a dictionary of environments and a dictionary of paths.
        Args:
            name (str): The name of the journey.
            envs (Union[dict[str, JEnv], list[JEnv]]): A dictionary or list of environments, where the key is the name of the environment. If a dictionary is provided, the keys should match the names of the environments.
            paths (Union[dict[str, JPath], list[JPath]]): A dictionary or list of paths, where the key is the name of the path. If a dictionary is provided, the keys should match the names of the paths.
        '''
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
        self.last_envs = None
        
    # Getters and setters
    def add_env(self, env: JEnv):
        '''Adds an environment, raising an error if an environment with the same name already exists.'''
        if env.name in self.envs:
            raise ValueError(f"Environment {env.name} already exists in Journey {self.name}")
        self.envs[env.name] = env
    def add_envs(self, envs: list[JEnv]):
        '''Adds a list of environments, raising an error if an environment with the same name already exists.'''
        for env in envs:
            self.add_env(env)
    def get_env(self, name):
        '''Returns the environment with the given name.'''
        return self.envs[name]
    def get_envs(self):
        '''Returns a copy of the environments.'''
        return self.envs.copy()
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
        '''Checks if the subpaths of a path are circular by building a tree of paths.
        Args:
            path_name (str): The name of the path to check.
            paths_prior (list[str] | None): A list of paths that have already been called.
        Returns:
            bool: True if the subpaths are circular, False otherwise.
        '''
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
        '''Validates the path by checking that all environments and subpaths used in the path are defined in the journey.
        Args:
            path_name (str): The name of the path to validate.
            error (bool): Whether to raise an error if the path is invalid.
            verbose (bool): Whether to print a message if the path is invalid.
        Returns:
            missing_envs (list[str]): A list of environment names that are missing from the journey.
            missing_subpaths (list[str]): A list of subpath names that are missing from the journey.
        '''
        path = self.paths[path_name]
        missing_envs = []
        missing_subpaths = []
        missing_batched_subpaths = []
        missing_batch_envs = []

        for env_name in path.env_names:
            if env_name not in self.envs:
                missing_envs.append(env_name)
        for subpath_name in path.subpaths:
            if subpath_name not in self.paths:
                missing_subpaths.append(subpath_name)
        circular_path = self.circular_subpaths(path_name)
        if self.paths[path_name].subpath_batch_envs is not None:
            for subpath_name, batch_envs in path.subpath_batch_envs.items():
                if subpath_name not in self.paths:
                    missing_batched_subpaths.append(subpath_name)
                for env_name in batch_envs:
                    if env_name not in self.envs:
                        missing_batch_envs.append(env_name)
        if (len(missing_envs) > 0 or len(missing_subpaths) > 0) and (error or verbose):
            error_string = f""
            if len(missing_envs) > 0:
                error_string += f"{path_name} is missing environment(s): {', '.join(missing_envs)}"
            if len(missing_subpaths) > 0:
                error_string += f"{path_name} is missing subpath(s): {', '.join(missing_subpaths)}"
            if len(missing_batched_subpaths) > 0:
                error_string += f"{path_name} is missing batched subpath(s): {', '.join(missing_batched_subpaths)}"
            if len(missing_batch_envs) > 0:
                error_string += f"{path_name} is missing batch environment(s): {', '.join(missing_batch_envs)}"
            if len(circular_path) > 0:
                error_string += f"{path_name} is circular: {', '.join(circular_path)}"
            if error:
                raise ValueError(error_string)
            else:
                if verbose:
                    print(error_string)
        return missing_envs, missing_subpaths, circular_path, missing_batched_subpaths, missing_batch_envs

    def validate_paths(self, error: bool=True):
        '''Validates the paths by checking that all environments and subpaths used in the paths are defined in the journey.'''
        error_string = f"Invalid paths"
        invalid=False
        for path_name, path in self.paths.items():
            missing_envs, missing_subpaths, circular_path, missing_batched_subpaths, missing_batch_envs = self.validate_path(path_name, error=False, verbose=False)
            # If any envs or subpaths are missing, add to invalid paths string
            if len(missing_envs) > 0:
                error_string += f"\n{path_name} is missing environment(s): {', '.join(missing_envs)}"
                invalid=True
            if len(missing_subpaths) > 0:
                error_string += f"\n{path_name} is missing subpath(s): {', '.join(missing_subpaths)}"
                invalid=True
            if len(missing_batched_subpaths) > 0:
                error_string += f"\n{path_name} is missing batched subpath(s): {', '.join(missing_batched_subpaths)}"
                invalid=True
            if len(missing_batch_envs) > 0:
                error_string += f"\n{path_name} is missing batch environment(s): {', '.join(missing_batch_envs)}"
                invalid=True
            if len(circular_path) > 0:
                error_string += f"\n{path_name} has a circular subpath: {', '.join(circular_path)}"
                invalid=True
        # If any paths are invalid, raise an error/warning
        if invalid:
            if error:
                raise ValueError(error_string)
            else:
                print(error_string)
    
    def run_path(self, path_name: str, force_run: bool=False, use_cache: bool=True, ponder: bool=True, verbose: bool=False, force_subpath_run: bool=False):
        '''Runs a path, raising an error if the path is not found in the journey.
        Args:
            path_name (str): The name of the path to run.
            force_run (bool): Whether to force the path to run even if the environments match a cached result.
            use_cache (bool): If false, the results will not be loaded or saved to the cache.
            ponder (bool): Whether to ponder the path results.
            verbose (bool): Whether to print verbose output.
            force_subpath_run (bool): Whether to force the subpaths to run even if their results match a cached result.
        Returns:
            result: The results of the path.
            subpath_results: The results of the subpaths. For each subpath run with a batch, the value will be a nested dictionary of the results for each batch id.
            cache_filepath: The filepath to the cache file. In the future, this might also be a row in a database.
        '''
        backpack = self.pack_for_path(path_name, force_run=force_run, use_cache=use_cache, verbose=verbose, force_subpath_run=force_subpath_run)
        if 'result' not in backpack:
            # Run the subpaths, and retrieve the files their results are stored in
            result = self.paths[path_name].run(backpack['safe_envs_stripped'], backpack['subpath_results'], verbose, safe=False)
            subpath_results = backpack['subpath_results']
            cache_filepath = backpack['cache_filepath']
            # Save the results to cache
            if use_cache:
                self.save_path_results(result, backpack['subpath_files'], cache_filepath)
        else:
            result = backpack['result']
            subpath_results = backpack['subpath_results']
            cache_filepath = backpack['cache_filepath']
        # Ponder the path results
        if ponder:
            self.paths[path_name].ponder(result, subpath_results)
        return result, subpath_results, cache_filepath

    def get_cached_env_names(self, path_name: str, batch_envs: JBatch|None=None):
        '''Gets the set of environments that are used in the path and its subpaths. Excludes batched environments, unless the same environment is used by a different path.
        Args:
            path_name (str): The name of the path to get the cached environments for.
            batch (JBatch|None): The batch that will be run for the path_name.
        Returns:
            cached_env_names (set[str]): The set of environments that are used in the path and its subpaths.
        '''
        if batch_envs is None:
            cached_env_names = SortedSet(self.paths[path_name].env_names)
        else:
            cached_env_names = SortedSet([name for name in self.paths[path_name].env_names if name not in batch_envs])
        for subpath_name in self.paths[path_name].subpaths:
            if self.paths[path_name].subpath_batch_envs is not None:
                subpath_batch_envs = self.paths[path_name].subpath_batch_envs.get(subpath_name, None)
            else:
                subpath_batch_envs = None
            cached_env_names.update(self.get_cached_env_names(subpath_name, subpath_batch_envs))
        return cached_env_names
        

    def pack_for_path(self, path_name: str, force_run: bool=False, use_cache: bool=True, verbose: bool=False, force_subpath_run: bool=False):
        '''Packs your backpack to set out on a path! Returns whatever you need to run the path, but if you've done it already, returns the results of the path.
        Note that this will run subpaths required by the path, including batched subpaths.
        Args:
            path_name (str): The name of the path to run.
            force_run (bool): Whether to force the path to run even if the environments match a cached result.
            use_cache (bool): If false, the results will not be loaded or saved to the cache.
            verbose (bool): Whether to print verbose output.
            force_subpath_run (bool): Whether to force the subpaths to run even if their results match a cached result.
        Returns:
            backpack (dict[str, Any]): A dictionary containing whatever you need from the path.
            If the path has been run, the items are:
                'result': The result of the path.
                'subpath_results': The results of the subpaths.
                'cache_filepath': The filepath to the cache file.
            If the path has not been run, or use_cache is False, or force_run is True, the items are:
                'safe_envs_stripped': The environments needed by the path.
                'subpath_results': The results of the subpaths. For each subpath run with a batch, the value will be a nested dictionary of the results for each batch id.
                'subpath_files': The file paths that contain the results of the subpaths. For each subpath run with a batch, the value will be a nested dictionary of the file paths for each batch id.
                'cache_filepath': The filepath to the cache file.
        '''
        if path_name not in self.paths:
            raise ValueError(f"Path {path_name} not found in Journey {self.name}")
        self.validate_path(path_name, error=True)
        
        # Get the environments that are used in the path and its subpaths
        if use_cache:
            cache_dir = self.parent_cache_dir / path_name
            cache_dir.mkdir(parents=True, exist_ok=True)

            cached_env_names = self.get_cached_env_names(path_name)
            cached_envs = {name: env for name, env in self.envs.items() if name in cached_env_names}
            cache_filepath = get_cache_filepath(cached_envs, cache_dir)

            # Load the results from cache if possible
            result, subpath_results = (None, None) if force_run else self.load_path_results(cache_filepath, subpath_exists_error=False)
        else:
            result, subpath_results = (None, None)
            cache_filepath = None

        if result is not None:
            # Pack the backpack!
            backpack = {
                'result': result,
                'subpath_results': subpath_results,
                'cache_filepath': cache_filepath
            }
            print(f"Loaded previous result for {path_name} from: {cache_filepath}")
            return backpack
        else:
            subpath_results = {}
            subpath_files = {}
            
            # Run the subpaths, and retrieve the files their results are stored in
            for subpath_name in self.paths[path_name].subpaths:
                # Only run the subpaths that are not batched
                if not (self.paths[path_name].subpath_batch_envs and subpath_name in self.paths[path_name].subpath_batch_envs):
                    subpath_result, _, subpath_filename = self.run_path(subpath_name, force_run=force_subpath_run, use_cache=use_cache, ponder=False, verbose=True, force_subpath_run=force_subpath_run)
                    subpath_results[subpath_name] = subpath_result
                    subpath_files[subpath_name] = subpath_filename

            # Collect the safe environments
            safe_envs_stripped = {name: env.get_values() for name, env in self.envs.items() if name in self.paths[path_name].env_names}

            # Run the subpaths that are batched, and retrieve the files their results are stored in
            if self.paths[path_name].subpath_batch_envs is not None:
                subpath_results_singles = copy.copy(subpath_results)
                subpath_batches = self.paths[path_name].subpath_batches(safe_envs_stripped, subpath_results_singles)
                for subpath_name, sp_batch in subpath_batches.items():
                    subpath_results[subpath_name] = {}
                    subpath_files[subpath_name] = {}
                    for batch_id, batch_envs in sp_batch.items():
                        try:
                            self.update_envs(batch_envs)
                            spb_result, _, spb_filename = self.run_path(subpath_name, force_run=force_subpath_run, use_cache=use_cache, ponder=False, verbose=True, force_subpath_run=force_subpath_run)
                            subpath_results[subpath_name][batch_id] = spb_result
                            subpath_files[subpath_name][batch_id] = spb_filename
                        finally:
                            self.revert_envs()


            # Pack the backpack!
            backpack = {
                'safe_envs_stripped': safe_envs_stripped,
                'subpath_results': subpath_results,
                'subpath_files': subpath_files,
                'cache_filepath': cache_filepath
            }
            print(f"Packed for new {path_name} run to: {cache_filepath}")
            return backpack


    def update_envs(self, new_envs: dict[str, JEnv]|list[JEnv]):
        '''Updates the environments in the journey. Saves the old environment references to last_envs.
        Args:
            new_envs (dict[str, JEnv]|list[JEnv]): The new environments to update the journey with.
        '''
        # print(self.envs.keys())
        self.last_envs = copy.deepcopy(self.envs)
        if isinstance(new_envs, dict):
            self.envs.update(new_envs)
        elif isinstance(new_envs, list):
            self.envs.update({env.name: env for env in new_envs})
    
    def revert_envs(self):
        '''Reverts the environments to the last saved state. Returns True if the environments were reverted, False if no last state was found.'''
        if self.last_envs is not None:
            self.envs = self.last_envs
            self.last_envs = None
            return True
        return False

    def load_path_results(self, cache_filepath: Path, subpath_exists_error: bool=False):
        '''Loads the results of a path from cache.
        Args:
            cache_filepath (Path): The filepath to the cache file.
            subpath_exists_error (bool): Whether to raise an error if a subpath file is not found.
        Returns:
            result: The results of the path. None if the path is not in the cache, or if a subpath file is not found.
            subpath_results: A dictionary of the results of the subpaths. None if the path is not in the cache, or if a subpath file is not found.
        '''
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
        '''Saves the results of a path to cache.
        Args:
            result: The results of the path.
            subpath_files: A dictionary of the files that contain the results of the subpaths. If a subpath is run with a batch, the value will be a nested dictionary of the files for each batch id.
            cache_filepath: The filepath to the cache file.
            verbose: Whether to print verbose output.
        '''
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
        string += f"Environments:\n"
        for env_name, env in self.envs.items():
            string += f"   {env_name}:\n      "
            type_list = [param.type.name for param in env.values()]
            string += "\n      ".join([f'{name} ({type})' for name, type in zip(list(env.get_params().keys()), type_list)])
            string += "\n"
        string += f"Paths:\n"
        for path_name, path in self.paths.items():
            string += f"   {path_name}: {str(path.env_names)[1:-1]}\n"
        return string

    def __repr__(self):
        return self.get_str()
    def __str__(self):
        return self.get_str()