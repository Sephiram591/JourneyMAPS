from typing import Union
from jmaps.config import PATH
from jmaps.journey.path import JPath
from jmaps.journey.environment import JEnv

class Journey:
    def __init__(self, name, envs: Union[dict[str, JEnv], list[JEnv]] | None = None, paths: Union[dict[str, JPath], list[JPath]] | None = None):
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
        self.parent_cache_dir = PATH.cache / name
        self.parent_cache_dir.mkdir(parents=True, exist_ok=True)

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
        '''Adds a path, raising an error if a path with the same name already exists.'''
        if path.name in self.paths:
            raise ValueError(f"Path {path.name} already exists in Journey {self.name}")
        self.paths[path.name] = path
        if validate:
            self.validate_paths(error=False)
    def add_paths(self, new_paths: list[JPath]):
        '''Adds a list of paths, raising an error if a path with the same name already exists.'''
        for path in new_paths:
            self.add_path(path, validate=False)
        self.validate_paths(error=False)
    def get_path(self, name):
        '''Returns the path with the given name.'''
        return self.paths[name]
    def get_paths(self):
        '''Returns a copy of the paths.'''
        return self.paths.copy()

    def validate_paths(self, error: bool=True):
        '''Validates the paths by checking that all environments used in the paths are defined in the journey.'''
        invalid_paths = []
        missing_envs_list = []
        for path_name, path in self.paths.items():
            missing_envs = []
            for env_name in path.env_names:
                if env_name not in self.envs:
                    missing_envs.append(env_name)
            if len(missing_envs) > 0:
                invalid_paths.append(path_name)
                missing_envs_list.append(missing_envs)
        if len(invalid_paths) > 0:
            error_string = f"Invalid paths"
            for path_name, missing_envs in zip(invalid_paths, missing_envs_list):
                error_string += f"\n{path_name} is missing {', '.join(missing_envs)}"
            if error:
                raise ValueError(error_string)
            else:
                print(error_string)

    
    def run_path(self, path_name: str, force_run: bool=False, verbose: bool=False, use_cache: bool=True):
        '''Runs a path, raising an error if the path is not found in the journey.
        Args:
            path_name (str): The name of the path to run.
            force_run (bool): Whether to force the path to run even if the environments match a cached result.
            verbose (bool): Whether to print verbose output.
            use_cache (bool): If false, the results will not be loaded or saved to the cache.
        '''
        if path_name not in self.paths:
            raise ValueError(f"Path {path_name} not found in Journey {self.name}")
        if use_cache:
            cache_dir = self.parent_cache_dir / path_name
            cache_dir.mkdir(parents=True, exist_ok=True)
        else:
            cache_dir = None
        return self.paths[path_name].run(self.envs, cache_dir, force_run, verbose)


    # Overrides
    def get_str(self):
        '''Returns a string representation of the journey.'''
        string = f"Journey({self.name})\n"
        string += f"Environments:\n"
        for env_name, env in self.envs.items():
            string += f"   {env_name}:\n      "
            type_list = [param.type.name for param in env.get_params().values()]
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
    # def __len__(self):
    #     return len(self.paths)
    # def __getitem__(self, key):
    #     return self.paths[key]
    # def __setitem__(self, key, value):
    #     self.paths[key] = value
    # def __delitem__(self, key):
    #     del self.paths[key]
    # def __iter__(self):
    #     return iter(self.paths)
    # def __contains__(self, key):
    #     return key in self.paths
    # def __len__(self):
    #     return len(self.paths)