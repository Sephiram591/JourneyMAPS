from typing import Union
from jmaps.config import PATH
from jmaps.journey.path import JPath
from jmaps.journey.environment import JEnv

class Journey:
    def __init__(self, name, envs: Union[dict[str, JEnv], list[JEnv]] | None = None, paths: Union[dict[str, JPath], list[JPath]] | None = None):
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
        self.envs[env.name] = env
    def add_envs(self, envs: list[JEnv]):
        for env in envs:
            self.add_env(env)
    def get_env(self, name):
        return self.envs[name]
    def get_envs(self):
        return self.envs
    def add_path(self, path: JPath, validate: bool=True):
        self.paths[path.name] = path
        if validate:
            self.validate_paths(error=False)
    def add_paths(self, new_paths: list[JPath]):
        for path in new_paths:
            self.add_path(path, validate=False)
        self.validate_paths(error=False)
    def get_path(self, name):
        return self.paths[name]
    def get_paths(self):
        return self.paths

    def validate_paths(self, error: bool=True):
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