from abc import ABC, abstractmethod
from jmaps.journey.environment import JEnv
from pathlib import Path
import hashlib
import dill

class JPath(ABC):
    '''JPath is an abstract class that represents a Path in a journey.
    It contains a name, a list of required environment names, and a method to run the Path.
    In order to create a Journey, you must subclass JPath for your own process/simulation/experiment and override the _run method.
    If you want to view the results of the Path conveniently, you should override the ponder method.
    If you want to optimize the environments of the Path using JMaps, you must override the evaluate method.
    '''
    @property
    @abstractmethod
    def name(self) -> str:
        """Unique name of the path"""
        pass

    @property
    @abstractmethod
    def env_names(self) -> list[str]:
        """List of required environment names for the path to run."""
        pass
    
    @abstractmethod
    def _run(self, envs: dict[str, JEnv], verbose: bool=False):
        '''This method must be overridden to provide functionality for the path. This might include simulations, computations, data collection, etc.
        If you wish there was another argument to the _run method, it probably should actually be an environment parameter.
        Args:
            envs (dict[str, JEnv]): The environments of parameters to run the path with.
            verbose (bool): Whether to print verbose output.
        '''
        pass

    def ponder(self, envs: dict[str, JEnv], data):
        '''Ponders the path results. This is a placeholder for a method that can be overridden to perform and view analysis (plots, tables, etc.) on the path results.
        Args:
            envs (dict[str, JEnv]): The environments of parameters to ponder the path results with.
            data: The results of the path run given the environments.
        '''
        pass

    def evaluate(self, envs: dict[str, JEnv], data) -> float:
        '''Evaluates the path results. This is a placeholder for a method that can be overridden to evaluate the path results.
        Args:
            envs (dict[str, JEnv]): The environments of parameters to evaluate the path results with.
            data: The results of the path run given the environments.
        Returns:
            float: The figure of merit of the path results.
        '''
        raise NotImplementedError("evaluate method must be overridden if you want to optimize the environments of the path.")

    def run(self, envs: dict[str, JEnv], cache_dir: Path=None, force_run: bool=False, verbose: bool=False, ponder: bool=True):
        '''Loads path results from cache if it exists, otherwise runs the path and saves to cache.
        Args:
            envs (dict[str, JEnv]): The environments of parameters to run the path with.
            cache_dir (Path): The directory to save the cache to. If None, the results will not be saved to or loaded from the cache.
            force_run (bool): Whether to force the path to run even if the environments match a cached result.
            verbose (bool): Whether to print verbose output.
            ponder (bool): Whether to ponder the path results.
        Returns:
            data: The results of the path.
        '''
        # Enforce that the env_names fully represent the environments used in the path by cropping the envs to the env_names
        safe_envs = {name: env for name, env in envs.items() if name in self.env_names}

        if cache_dir:
            cache_filepath = self.get_cache_filepath(safe_envs, cache_dir)
            if cache_filepath.exists() and not force_run:
                with open(cache_filepath, "rb") as f:
                    if verbose:
                        print(f"Loaded path results from cache: {cache_filepath}")
                    data = dill.load(f)
            else:
                data = self._run(safe_envs, verbose)
                with open(cache_filepath, "wb") as f:
                    dill.dump(data, f)
                if verbose:
                    print(f"Saved path results to cache: {cache_filepath}")
        else:
            data = self._run(safe_envs, verbose)

        if ponder:
            self.ponder(safe_envs, data)
        return data

    def get_cache_filepath(self, envs: dict[str, JEnv], cache_dir: Path):
        '''Gets the cache filepath for a given set of JEnvs. Crops the envs to the env_names of the Path.
        Args:
            envs: dict[str, JEnv]
            cache_dir: Path
        Returns:
            cache_filepath: Path
        '''
        hashable = {name: env.get_hashable_params() for name, env in envs.items() if name in self.env_names}
        dumped = dill.dumps(hashable, protocol=dill.HIGHEST_PROTOCOL)
        key = hashlib.sha256(dumped).hexdigest()
        cache_filepath = cache_dir / f"{key}.dill"
        return cache_filepath
