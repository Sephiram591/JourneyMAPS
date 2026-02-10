"""Abstract path definitions for journeys.

Defines `JPath` and batching helpers used by the `Journey` execution engine.
"""
from abc import ABC, abstractmethod
from jmaps.journey.param import JDict, wrap_jparam
from jmaps.journey.jmalc import get_sql_schema
from jmaps.journey.io import read, write
from pathlib import Path
from typing import Any, Dict
from deepdiff import DeepDiff
from pydantic import BaseModel, Field

class PathResult(BaseModel):
    sql: Dict[str, Any] | None = Field(None, description="Results that are saved to the sql database.")
    file: Dict[str, Any] | None = Field(None, description="Results that are saved to a file using the IO registry.")
    def __getitem__(self, key):
        try:
            return self.sql[key]
        except:
            return self.file[key]
    def to_file(self, file_path: Path):
        if self.file is None:
            return None
        file_schema = {}
        for k, v in self.file:
            file_schema[k] = write(v, file_path.with_name(file_path.name+"_" + k))
        return file_schema
    def from_file(self, file_path, file_schema):
        if file_schema is None:
            self.file = None
            return
        self.file = {}
        for k, v in file_schema:
            self.file[k] = read(v, file_path.with_name(file_path.name+"_" + k))

class JBatch(dict[str, JDict]):
    """A collection of named runs for batched subpath execution.

    Keys are batch IDs; values are lists of `JDict` environments required to run a subpath
    once. A shared schema is validated across runs to ensure type consistency.
    """
    def __init__(self, runs: dict[str, JDict] | None = None, param_schema: dict[str, type] | None = None):
        """Initializes a JBatch.
        
        Args:
            runs (dict[str, list[JDict]]): The runs to add to the batch.
        """
        self.param_schema = param_schema
        super().__init__()
        if runs is not None:
            for batch_id, envs in runs.items():
                self.add_run(batch_id, envs)
    def validate_run(self, env: JDict, error: bool=True):
        """Validates that every env matches the types of the envs in the list.
        Args:
            envs (list[JDict]): The environments to validate.
            error (bool): Whether to raise an error if the run is invalid.
        """
        differences = DeepDiff(self.param_schema, get_sql_schema(env.get_sql_data(show_unused=True, show_invisible=True)))
        if differences:
            if error:
                raise ValueError(differences)
            else:
                print(differences)
    def add_run(self, batch_id: str, env: JDict):
        """Adds a run to the batch. If the run is invalid (the involved envs and the types of their parameters don't match the schema), an error will be raised. The first call to add_run will define the schema.
        
        Args:
            batch_id (str): The id of the run.
            envs (list[JDict]): The environments to add to the run.
        """
        env = wrap_jparam(env)
        if self.param_schema is None:
            self.param_schema = get_sql_schema(env.get_sql_data(show_unused=True, show_invisible=True))
        else:
            self.validate_run(env)
        self[batch_id] = env


class JPath(ABC, BaseModel):
    '''JPath is an abstract class that represents a Path in a journey.
    It contains a name, a list of required environment names, and a method to run the Path.
    In order to create a Journey, you must subclass JPath for your own process/simulation/experiment and override the _run method, and the env_names property.
    If this Path requires the results of other Paths, you can override the subpaths property to return a set of subpath names.
    If you want to view the results of the Path conveniently, you should override the plot method.
    If you want to optimize the environments of the Path using JMaps, you must override the evaluate method.
    '''
    name: str = Field(..., description="Unique name of the path")
    changelog: str|None = Field(None, description="Description of the difference between the current path version and the last one. If this is the first version, simply describes the path.")
    save_datetime: bool = Field(False, description="Whether or not to save the time when the path is completed to the database.")
    subpaths: list[str] = Field(default_factory=set, description="List of subpath names which feed results into the path. Run in order.")
    batched_subpaths: set[str] = Field(default_factory=set, description="Set of subpath names have custom environments defined by this parent path.")

    @abstractmethod
    def _run(self, env: JDict, subpath_results: dict[str, Any], verbose: bool=False):
        '''This method must be overridden to provide functionality for the path. This might include simulations, computations, data collection, etc.
        If you wish there was another argument to the _run method, it probably should actually be an environment parameter.
        Args:
            env (JDict): The environment of parameters to run the path with.
            subpath_results (dict[str, Any]): The results of the subpaths.
            verbose (bool): Whether to print verbose output.
        '''
        pass

    def plot(self, result: Any, subpath_results: dict[str, Any]):
        """plots the path results. This is a placeholder for a method that can be overridden to perform and view analysis (plots, tables, etc.) on the path results.
        
        Args:
            env JDict: The environments of parameters to plot the path results with.
            result: The results of the path run given the environments.
        """
        pass

    def run(self, env: JDict, subpath_results: dict[str, Any], verbose: bool=False):
        """Loads path results from cache if it exists, otherwise runs the path and saves to cache.
        
        Args:
            env (JDict): The environments of parameters to run the path with.
            subpath_results (dict[str, Any]): The results of the subpaths.
            cache_dir (Path): The directory to save the cache to. If None, the results will not be saved to or loaded from the cache.
            force_run (bool): Whether to force the path to run even if the environments match a cached result.
            verbose (bool): Whether to print verbose output.
        
        Returns:
            result: The results of the path.
            cache_filepath: The filepath to the cache. None if the cache was not used.
        """
        try:
            env.lock()
            result = self._run(env, subpath_results, verbose)
        finally:
            env.unlock()

        return result

    def get_batches(self, subpath_name, env: JDict, previous_subpath_results: dict[str, Any]) -> JBatch | None:
        """If this path requires multiple runs of a subpath, this method should be overridden to return a dictionary describing how to update the environments for each time the subpath is run.
        Note that the version of subpath_results_singles WILL NOT contain any results from batch runs.
        
        Args:
            env (JDict): The environments of parameters to run the path with.
            previous_subpath_results (dict[str, Any]): The results of the subpaths run before this one.
        
        Returns:
            batches (dict[subpath name, JBatch]): JBatch for each subpath that requires a batch run.
        """
        return None

    def to_file(self, file_result, file_path):
        pass

    def from_file(self, file_path):
        pass