from abc import ABC, abstractmethod
from jmaps.journey.environment import JEnv
from pathlib import Path
import hashlib
import dill

class JPath(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        """Unique name of the path"""
        pass

    @property
    @abstractmethod
    def env_names(self) -> list[str]:
        """List of required environment names"""
        pass
    
    @abstractmethod
    def _run(self, envs: dict[str, JEnv], verbose: bool=False, **kwargs):
        pass

    def ponder(self, envs: dict[str, JEnv], data, **kwargs):
        pass

    def run(self, envs: dict[str, JEnv], cache_dir: Path=None, force_run: bool=False, verbose: bool=False, ponder: bool=True):
        '''Loads path results from cache if it exists, otherwise runs the path and saves to cache.
        Args:
            envs: dict[str, JEnv]
            cache_dir: Path
        Returns:
            data
        '''
        if cache_dir:
            cache_filepath = self.get_cache_filepath(envs, cache_dir)
            if cache_filepath.exists() and not force_run:
                with open(cache_filepath, "rb") as f:
                    if verbose:
                        print(f"Loaded path results from cache: {cache_filepath}")
                    data = dill.load(f)
            else:
                data = self._run(envs, verbose)
                with open(cache_filepath, "wb") as f:
                    dill.dump(data, f)
                if verbose:
                    print(f"Saved path results to cache: {cache_filepath}")
        else:
            data = self._run(envs, verbose)

        if ponder:
            self.ponder(envs, data)
        return data

    def get_cache_filepath(self, envs: dict[str, JEnv], cache_dir: Path):
        '''Gets the cache filepath for a given set of JEnvs.
        Args:
            envs: dict[str, JEnv]
            cache_dir: Path
        Returns:
            cache_filepath: Path
        '''
        hashable = {env.name: env.get_hashable_params() for env in envs.values() if env.name in self.env_names}
        dumped = dill.dumps(hashable, protocol=dill.HIGHEST_PROTOCOL)
        key = hashlib.sha256(dumped).hexdigest()
        cache_filepath = cache_dir / f"{key}.dill"
        return cache_filepath
