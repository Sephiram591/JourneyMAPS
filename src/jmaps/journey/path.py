"""Abstract path definitions for journeys.

Defines `JPath` and batching helpers used by the `Journey` execution engine.
"""
from abc import ABC, abstractmethod
from jmaps.journey.environment import JEnv
from typing import Any

class JBatch(dict[str, list[JEnv]]):
    """A collection of named runs for batched subpath execution.

    Keys are batch IDs; values are lists of `JEnv` required to run a subpath
    once. A shared schema is validated across runs to ensure type consistency.
    """
    def __init__(self, runs: dict[str, list[JEnv]] | None = None, env_schema: dict[str, dict[str, type]] | None = None):
        """Initializes a JBatch.
        
        Args:
            runs (dict[str, list[JEnv]]): The runs to add to the batch.
        """
        self.env_schema = env_schema
        super().__init__()
        if runs is not None:
            for batch_id, envs in runs.items():
                self.add_run(batch_id, envs)
    def validate_run(self, envs: list[JEnv], error: bool=True):
        """Validates that every env matches the types of the envs in the list.
        
        Args:
            envs (list[JEnv]): The environments to validate.
            error (bool): Whether to raise an error if the run is invalid.
        """
        env_types = {env.name: env.get_types() for env in envs}
        error_string = ''
        invalid_run = False
        for schema_name, schema_type in self.env_schema.items():
            if schema_name not in env_types:
                error_string += f"Batch is missing env {schema_name}\n"
                invalid_run = True
            if schema_type != env_types[schema_name]:
                error_string += f"Batch has env {schema_name} with different types. \n\tSchema type: {schema_type}\nDoes not match \n\tEnv type: {env_types[schema_name]}\n"
                invalid_run = True
        if invalid_run:
            if error:
                raise ValueError(error_string)
            else:
                print(error_string)
    def add_run(self, batch_id: str, envs: list[JEnv]):
        """Adds a run to the batch. If the run is invalid (the involved envs and the types of their parameters don't match the schema), an error will be raised. The first call to add_run will define the schema.
        
        Args:
            batch_id (str): The id of the run.
            envs (list[JEnv]): The environments to add to the run.
        """
        if self.env_schema is None:
            self.env_schema = {env.name: env.get_types() for env in envs}
        else:
            self.validate_run(envs)
        self[batch_id] = envs


class JPath(ABC):
    '''JPath is an abstract class that represents a Path in a journey.
    It contains a name, a list of required environment names, and a method to run the Path.
    In order to create a Journey, you must subclass JPath for your own process/simulation/experiment and override the _run method, and the env_names property.
    If this Path requires the results of other Paths, you can override the subpaths property to return a set of subpath names.
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
    def env_names(self) -> set[str]:
        """Set of required environment names for the path to run."""
        pass

    @property
    def subpaths(self) -> set[str]:
        """Set of subpaths names which feed results into the path."""
        return set()

    @abstractmethod
    def _run(self, envs: dict[str, JEnv], subpath_results: dict[str, Any], verbose: bool=False):
        '''This method must be overridden to provide functionality for the path. This might include simulations, computations, data collection, etc.
        If you wish there was another argument to the _run method, it probably should actually be an environment parameter.
        Args:
            envs (dict[str, JEnv]): The environments of parameters to run the path with.
            subpath_results (dict[str, Any]): The results of the subpaths.
            verbose (bool): Whether to print verbose output.
        '''
        pass

    def ponder(self, result: Any, subpath_results: dict[str, Any]):
        """Ponders the path results. This is a placeholder for a method that can be overridden to perform and view analysis (plots, tables, etc.) on the path results.
        
        Args:
            envs (dict[str, JEnv]): The environments of parameters to ponder the path results with.
            result: The results of the path run given the environments.
        """
        pass

    def evaluate(self, result: Any, subpath_results: dict[str, Any]) -> float:
        """Evaluates the path results. This is a placeholder for a method that can be overridden to evaluate the path results.
        
        Args:
            envs (dict[str, JEnv]): The environments of parameters to evaluate the path results with.
            result: The results of the path run given the environments.
        
        Returns:
            float: The figure of merit of the path results.
        """
        raise NotImplementedError("evaluate method must be overridden if you want to optimize the environments of the path.")

    def run(self, envs: dict[str, JEnv], subpath_results: dict[str, Any], verbose: bool=False, safe: bool=True):
        """Loads path results from cache if it exists, otherwise runs the path and saves to cache.
        
        Args:
            envs (dict[str, JEnv]): The environments of parameters to run the path with.
            subpath_results (dict[str, Any]): The results of the subpaths.
            cache_dir (Path): The directory to save the cache to. If None, the results will not be saved to or loaded from the cache.
            force_run (bool): Whether to force the path to run even if the environments match a cached result.
            verbose (bool): Whether to print verbose output.
        
        Returns:
            result: The results of the path.
            cache_filepath: The filepath to the cache. None if the cache was not used.
        """
        # Enforce that the env_names fully represent the environments used in the path by cropping the envs to the env_names
        if safe:
            safe_envs = {name: env for name, env in envs.items() if name in self.env_names}
        else:
            safe_envs = envs
        result = self._run(safe_envs, subpath_results, verbose)
        return result

    def subpath_batches(self, envs: dict[str, JEnv], subpath_results_singles: dict[str, Any]) -> dict[str, JBatch] | None:
        """If this path requires multiple runs of a subpath, this method should be overridden to return a dictionary describing how to update the environments for each time the subpath is run.
        Note that the version of subpath_results_singles WILL NOT contain any results from batch runs.
        
        Args:
            envs (dict[str, JEnv]): The environments of parameters to run the path with.
            subpath_results_singles (dict[str, Any]): The results of the subpaths.
        
        Returns:
            batches (dict[subpath name, JBatch]): JBatch for each subpath that requires a batch run.
        """
        return None

    @property
    def subpath_batch_envs(self) -> dict[str, set[str]] | None:
        """If this path requires multiple runs of a subpath, this method should be overridden to return a dictionary describing which environments are used in the batch runs of the subpath.
        
        Returns:
            batch_envs (dict[subpath name, set[str]]): The environments that are used for each subpath that has batch runs.
        """
        return None
        