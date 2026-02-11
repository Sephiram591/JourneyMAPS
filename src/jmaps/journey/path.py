"""Abstract path definitions for journeys.

Defines :class:`JPath`, :class:`PathResult`, and batching helpers used by the
`Journey` execution engine.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict

from deepdiff import DeepDiff
from pydantic import BaseModel, Field

from jmaps.journey.io import read, write
from jmaps.journey.jmalc import get_sql_schema
from jmaps.journey.param import JDict, wrap_jparam


class PathResult(BaseModel):
    """Container for the results of a path execution.

    Attributes:
        sql: Mapping of values that are persisted in the SQL database.
        file: Mapping of values that are persisted on disk using the IO registry.
    """

    sql: Dict[str, Any] | None = Field(
        None, description="Results that are saved to the sql database."
    )
    file: Dict[str, Any] | None = Field(
        None, description="Results that are saved to a file using the IO registry."
    )

    def __getitem__(self, key: str) -> Any:
        """Return a result by key, preferring SQL-backed values."""
        try:
            return self.sql[key]
        except:
            return self.file[key]
    def to_file(self, file_path: Path):
        """Serialize file-backed results via the IO registry.

        Args:
            file_path: Base path to use for all file-backed results.

        Returns:
            dict[str, type] | None: Schema describing how each key was written,
            or ``None`` if there are no file-backed results.
        """
        if self.file is None:
            return None
        file_schema: dict[str, type] = {}
        for k, v in self.file.items():
            file_schema[k] = write(v, file_path.with_name(file_path.name + "_" + k))
        return file_schema

    def from_file(self, file_path: Path | None, file_schema):
        """Populate file-backed results from disk via the IO registry.

        Args:
            file_path: Base path used when the results were written. If ``None``,
                no file-backed results are loaded.
            file_schema: Schema describing how each key was written, or ``None``.
        """
        if file_schema is None or file_path is None:
            self.file = None
            return
        self.file = {}
        for k, v in file_schema.items():
            self.file[k] = read(v, file_path.with_name(file_path.name + "_" + k))


class JBatch(dict[str, JDict]):
    """A collection of named runs for batched subpath execution.

    Keys are batch IDs; values are :class:`JDict` environments required to run a
    subpath once. A shared schema is validated across runs to ensure type
    consistency.
    """

    def __init__(
        self,
        runs: dict[str, JDict] | None = None,
        param_schema: dict[str, type] | None = None,
    ):
        """Initialize a :class:`JBatch`.

        Args:
            runs: Optional mapping from batch ID to environment.
            param_schema: Optional pre-computed parameter schema. If omitted,
                the schema is inferred from the first added run.
        """
        self.param_schema = param_schema
        super().__init__()
        if runs is not None:
            for batch_id, env in runs.items():
                self.add_run(batch_id, env)

    def validate_run(self, env: JDict, error: bool = True):
        """Validate that an environment matches the batch schema.

        Args:
            env: Environment to validate.
            error: If ``True``, raise on mismatch; otherwise print differences.

        Raises:
            ValueError: If the schema differs and ``error`` is ``True``.
        """
        differences = DeepDiff(
            self.param_schema,
            get_sql_schema(env.get_sql_data(show_unused=True, show_invisible=True)),
        )
        if differences:
            if error:
                raise ValueError(differences)
            else:
                print(differences)

    def add_run(self, batch_id: str, env: JDict):
        """Add a run to the batch.

        The schema is inferred from the first run and enforced for subsequent
        additions.

        Args:
            batch_id: Identifier for this run.
            env: Environment to associate with ``batch_id``.
        """
        env = wrap_jparam(env)
        if self.param_schema is None:
            self.param_schema = get_sql_schema(
                env.get_sql_data(show_unused=True, show_invisible=True)
            )
        else:
            self.validate_run(env)
        self[batch_id] = env


class JPath(ABC, BaseModel):
    """Abstract base class representing a path in a Journey.

    Subclasses encapsulate a unit of work (e.g. simulation, analysis, or
    experiment) and define their dependencies on environments and subpaths.
    """

    name: str = Field(..., description="Unique name of the path.")
    changelog: str | None = Field(
        None,
        description=(
            "Description of what this path does and how it differs from "
            "previous versions."
        ),
    )
    save_datetime: bool = Field(
        False,
        description=(
            "Whether to save the completion time of the path in the database."
        ),
    )
    subpaths: list[str] = Field(
        default_factory=list,
        description=(
            "Ordered list of subpath names whose results are required by this path."
        ),
    )
    batched_subpaths: set[str] = Field(
        default_factory=set,
        description=(
            "Set of subpath names that are executed multiple times using custom "
            "batch environments defined by this parent path."
        ),
    )

    @abstractmethod
    def _run(self, env: JDict, subpath_results: dict[str, Any], verbose: bool = False) -> PathResult:
        """Implement the core logic for this path.

        This method must be overridden by subclasses to perform the actual work
        of the path (e.g. simulations, computations, data collection).

        Args:
            env: Environment of parameters used to run the path.
            subpath_results: Results from all subpaths listed in ``subpaths``.
            verbose: If ``True``, print additional diagnostic output.

        Returns:
            PathResult: Result object to be wrapped into a :class:`PathResult` by the
            caller.
        """
        raise NotImplementedError

    def plot(self, result: Any, subpath_results: dict[str, Any]):
        """Visualize or summarize the path results.

        Subclasses may override this method to produce plots, tables, or other
        analysis artifacts.

        Args:
            result: Result returned by :meth:`_run`.
            subpath_results: Results from subpaths.
        """
        pass

    def run(self, env: JDict, subpath_results: dict[str, Any], verbose: bool = False):
        """Execute the path under a locked environment.

        The environment is temporarily locked to prevent accidental parameter
        mutation while the path is running.

        Args:
            env: Environment of parameters to run the path with.
            subpath_results: Results of the subpaths.
            verbose: If ``True``, print additional diagnostic output.

        Returns:
            PathResult: The result of the path.
        """
        try:
            env.lock()
            result = self._run(env, subpath_results, verbose)
        finally:
            env.unlock()

        return result

    def get_batches(
        self, subpath_name: str, env: JDict, previous_subpath_results: dict[str, Any]
    ) -> JBatch | None:
        """Describe batched executions required for a subpath.

        Subclasses can override this to return a :class:`JBatch` describing how
        environments should be varied for multiple runs of ``subpath_name``.

        Args:
            subpath_name: Name of the subpath.
            env: Environment of parameters to run the parent path with.
            previous_subpath_results: Results of subpaths run before this one.

        Returns:
            JBatch | None: Batch description for the subpath, or ``None`` if
            only a single run is required.
        """
        return None

    def to_file(self, file_result, file_path: Path):
        """Optional hook to customize how path-level file results are written."""
        pass

    def from_file(self, file_path: Path):
        """Optional hook to customize how path-level file results are read."""
        pass