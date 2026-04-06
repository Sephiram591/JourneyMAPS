"""Parameter primitives used to define Journey environments.

This module defines a small parameter tree abstraction (:class:`JParam` and
its concrete subclasses) that tracks usage, supports locking, and can produce
SQL-friendly representations of the current environment state.
"""

from enum import Enum, auto
from inspect import signature, Parameter
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict

from pydantic import PrivateAttr, BaseModel, Field

from jmaps.journey.jmalc import get_sql_type, cast_sql_type, DBResult

REF_SEP = "."


class ResetCondition(Enum):
    """When a :class:`Buffer` should reset its cached value."""

    NEVER = auto()
    ALWAYS = auto()
    ON_RUN = auto()
    ON_RUN_IF_PARENT_PATH = auto()


class JParam(ABC, BaseModel):
    """Abstract base class for a Journey parameter node.

    Parameters are arranged in a tree, can be locked against user mutation, and
    track whether they have been used in a given run. Concrete subclasses must
    implement :meth:`_get_value`, :meth:`_get_children`, and :meth:`get_sql_data`.
    """

    _locked: bool = PrivateAttr(
        default=False
    )  # Whether the parameter is locked (cannot be changed)
    used: bool = False  # Whether the parameter has been used in the current run

    def lock(self):
        """Lock this parameter and all children against mutation."""
        self._locked = True
        for child in self._get_children():
            child.lock()

    def unlock(self):
        """Unlock this parameter and all children."""
        self._locked = False
        for child in self._get_children():
            child.unlock()

    def reset_usage(self):
        """Reset the ``used`` flag for this parameter and its children."""
        self.used = False
        for child in self._get_children():
            child.reset_usage()

    def set_usage(self, used: bool = True):
        """Mark this parameter and its children as used or unused."""
        self.used = used
        for child in self._get_children():
            child.set_usage(used)

    def init_run(self, is_parent_path: bool, parent_env=None):
        """Prepare this parameter tree for a new run.

        Args:
            is_parent_path: ``True`` if this parameter belongs to the top-level
                path being executed.
            parent_env: Root of the environment tree, used by some subclasses
                (e.g. :class:`Refer`) to resolve references.
        """
        parent_env = parent_env if parent_env is not None else self
        leaf_changed = False
        for child in self._get_children():
            new_change = child.init_run(is_parent_path, parent_env)
            leaf_changed = leaf_changed or new_change
        new_change = self._init_run(is_parent_path, parent_env, leaf_changed)
        leaf_changed = leaf_changed or new_change
        return leaf_changed
    def merge_usage(self, mirror_param: "JParam"):
        """Merge ``used`` flags from a mirror parameter tree."""
        self.used = self.used or mirror_param.used
        for self_child, mirror_child in zip(
            self._get_children(), mirror_param._get_children()
        ):
            self_child.merge_usage(mirror_child)


    def merge_dtypes(self, mirror_param: "JParam"):
        """Propagate dtype information from a mirror parameter tree. Mirror param overrides self dtype if it is not None."""
        if isinstance(self, JValue) and isinstance(mirror_param, JValue):
            self.dtype = mirror_param.dtype if mirror_param.dtype is not None else self.dtype
        elif isinstance(self, JDict):
            for k, v in self.data.items():
                if k in mirror_param.data:
                    v.merge_dtypes(mirror_param.data[k])
                else:
                    v.merge_dtypes(None)
        else:
            for self_child, mirror_child in zip(self._get_children(), mirror_param._get_children()):
                self_child.merge_dtypes(mirror_child)

    def _init_run(self, is_parent_path: bool, parent_env: "JParam", leaf_changed: bool):
        """Subclass hook invoked at the start of a run."""
        return False

    def get_value(self):
        """Return the evaluated value for this parameter and mark it as used."""
        self.used = True
        return self._get_value()

    @abstractmethod
    def _get_children(self):
        """Return an iterable of child :class:`JParam` instances."""

    @abstractmethod
    def _get_value(self):
        """Evaluate and return the value for this parameter."""

    @abstractmethod
    def get_sql_data(
        self, show_unused: bool = False, show_invisible: bool = False, return_schema: bool = False
    ):
        """Produce a serializable representation for SQL-like workflows.

        Args:
            show_unused: If ``True``, include parameters that have not been marked
                as used.
            show_invisible: If ``True``, expand :class:`InvisibleParam` nodes
                instead of omitting them.
            return_schema: If ``True``, return type names instead of values.

        Returns:
            dict | str | InvisibleParam: A serializable object suitable for building
            schemas or storing in the database.
        """


class JValue(JParam):
    """Leaf parameter holding a concrete Python value."""

    value: Any
    dtype: Callable | None

    def __init__(self, value: Any, dtype: Callable | None = None):
        """Initialize a :class:`JValue`.

        Args:
            value: Underlying Python value.
            dtype: Optional type used for casting/serialization. If omitted, the
                type is inferred from ``value``.
        """
        super().__init__(value=value, dtype=dtype)

    def _get_value(self):
        return self.value

    def get_sql_data(
        self, show_unused: bool = False, show_invisible: bool = False, return_schema: bool = False
    ):
        """Return a SQL-friendly representation of this value."""
        if return_schema:
            if self.dtype is not None:
                return self.dtype.__name__
            return get_sql_type(self.value)
        else:
            if self.dtype is not None:
                return self.dtype(self.value)
            return cast_sql_type(self.value)

    def _get_children(self):
        return []


class InvisibleParam(JParam):
    """Wrapper for a parameter that is hidden from SQL exports by default."""

    jparam: JParam

    def __init__(self, jparam: Any):
        """Initialize an :class:`InvisibleParam` around ``jparam``."""
        super().__init__(jparam=wrap_jparam(jparam))

    def _get_value(self):
        return self.jparam.get_value()

    def get_sql_data(
        self, show_unused: bool = False, show_invisible: bool = False, return_schema: bool = False
    ):
        """Return SQL data for the wrapped parameter or self if invisible."""
        return (
            self.jparam.get_sql_data(show_unused, show_invisible, return_schema)
            if show_invisible
            else self
        )

    def _get_children(self):
        return [self.jparam]

class JList(JParam):
    """Parameter representing an ordered list of child parameters."""

    data: list[JParam] = Field(default_factory=list)
    dtype: Callable = list
    def __init__(self, data: list[Any], **kwargs):
        """Initialize a :class:`JList` from a sequence of values."""
        data = [wrap_jparam(v) for v in data]
        super().__init__(data=data, **kwargs)

    # def __len__(self):
    #     return len(self.data)

    # def __iter__(self):
    #     """Iterate over evaluated values."""
    #     for v in self.data:
    #         yield v.get_value()

    # def append(self, value: Any):
    #     """Append a value to the list."""
    #     if self._locked:
    #         raise AttributeError(
    #             "JList is locked, parameters cannot be changed by the user."
    #         )
    #     self.data.append(wrap_jparam(value))

    # def extend(self, values: list[Any]):
    #     """Extend the list with multiple values."""
    #     if self._locked:
    #         raise AttributeError(
    #             "JList is locked, parameters cannot be changed by the user."
    #         )
    #     self.data.extend(wrap_jparam(v) for v in values)

    # def insert(self, index: int, value: Any):
    #     """Insert a value at a specific index."""
    #     if self._locked:
    #         raise AttributeError(
    #             "JList is locked, parameters cannot be changed by the user."
    #         )
    #     self.data.insert(index, wrap_jparam(value))

    # def __getitem__(self, index: int | str):
    #     if isinstance(index, str):
    #         if not index.isdigit():
    #             raise TypeError("JList indices must be integers or digit strings")
    #         index = int(index)
    #     return self.data[index].get_value()

    # def __setitem__(self, index: int | str, value: Any):
    #     if self._locked:
    #         raise AttributeError(
    #             "JList is locked, parameters cannot be changed by the user."
    #         )
    #     if isinstance(index, str):
    #         if not index.isdigit():
    #             raise TypeError("JList indices must be integers or digit strings")
    #         index = int(index)

    #     if (
    #         isinstance(self.data[index], JValue)
    #         and not isinstance(value, JValue)
    #         and self.data[index].dtype is not None
    #     ):
    #         self.data[index].value = value
    #         self.data[index].used = False
    #     else:
    #         self.data[index] = wrap_jparam(value)

    # def items(self):
    #     """Iterate over ``(index, value)`` pairs using evaluated values."""
    #     for i, v in enumerate(self.data):
    #         yield (i, v.get_value())

    def _get_value(self):
        return [self.data[i].get_value() for i in range(len(self.data))]

    def _get_children(self):
        return self.data

    def get_sql_data(
        self,
        show_unused: bool = False,
        show_invisible: bool = False,
        return_schema: bool = False,
    ):
        """Return a flattened SQL-friendly representation of the parameter list."""
        sql_list: list[Any] = []
        for v in self.data:
            if v.used or show_unused:
                sql_data = v.get_sql_data(show_unused, show_invisible, return_schema)
                if not isinstance(sql_data, InvisibleParam):
                    sql_list.append(sql_data)
                elif isinstance(sql_data, InvisibleParam) and show_invisible:
                    sql_list.append(sql_data.jparam.get_sql_data(
                        show_unused, show_invisible, return_schema
                    ))
        return sql_list

    def merge_usage(self, mirror_param: "JParam"):
        """Merge ``used`` flags from a mirror parameter JList."""
        self.used = self.used or mirror_param.used
        self.set_usage(self.used)


    def merge_dtypes(self, mirror_param: "JParam"):
        """JLists are dynamically typed, so we do not merge dtypes."""
        pass

class JDict(JParam):
    """Parameter representing a dictionary of named child parameters."""

    data: Dict[str, JParam] = Field(default_factory=dict)

    def __init__(self, data: dict[str, Any], **kwargs):
        """Initialize a :class:`JDict` from a mapping of names to values."""
        for k, v in data.items():
            data[k] = wrap_jparam(v)
        super().__init__(data=data, **kwargs)

    def keys(self):
        """Return the parameter names contained in this dictionary."""
        return self.data.keys()

    def replace(self, other: "JDict | dict[str, Any]", merge_dtypes: bool = True, merge_usage: bool = True):
        """Merge values from another :class:`JDict` into this one.

        Dtypes from ``self`` are preserved in the final object

        Args:
            other: Source dictionary or :class:`JDict` whose entries are merged. The schema of the other dict must be a subset of the schema of this dict.
            merge_dtypes: If True, the dtypes of the other dict are merged into this dict.
            merge_usage: If True, the usage of the other dict is merged into this dict.
        Raises:
            TypeError: If ``other`` cannot be converted to :class:`JDict`.
        """
        if isinstance(self, Buffer):
            self.value = None
        other = wrap_jparam(other)
        if not isinstance(other, JDict):
            raise TypeError("Other is not a JDict, cannot replace.")
        for k, v in other.data.items():
            if isinstance(v, JDict):
                self.data[k].replace(v, merge_dtypes, merge_usage)
                if v.used and merge_usage:
                    self.data[k].used = True
            else:
                if merge_dtypes:
                    v.merge_dtypes(self.data[k])
                if merge_usage:
                    v.merge_usage(self.data[k])
                self.data[k] = v

    def __getitem__(self, key: str):
        return self.data[key].get_value()

    def __setitem__(self, key: str, value: Any):
        if self._locked:
            raise AttributeError(
                "JDict is locked, parameters cannot be changed by the user."
            )
        if not isinstance(key, str):
            raise TypeError("JDict keys must be strings")
        if (
            key in self.data
            and isinstance(self.data[key], JValue)
            and not isinstance(value, JValue)
            and self.data[key].dtype is not None
        ):
            self.data[key].value = value
            self.data[key].used = False
        else:
            self.data[key] = wrap_jparam(value)

    def __getattr__(self, key: str):
        try:
            return super().__getattr__(key)
        except AttributeError:
            return self.data[key].get_value()

    def __setattr__(self, key: str, value: Any):
        try:
            super().__setattr__(key, value)
        except ValueError:
            if self._locked:
                raise AttributeError(
                    "JDict is locked, parameters cannot be changed by the user."
                )
            if not isinstance(key, str):
                raise TypeError("JDict keys must be strings")
            if (
                key in self.data
                and isinstance(self.data[key], JValue)
                and not isinstance(value, JValue)
                and self.data[key].dtype is not None
            ):
                self.data[key].value = value
                self.data[key].used = False
            else:
                self.data[key] = wrap_jparam(value)

    def items(self):
        """Iterate over ``(name, value)`` pairs using evaluated values."""
        for k, v in self.data.items():
            yield (k, v.get_value())

    def _get_value(self):
        return self

    def _get_children(self):
        return self.data.values()

    def get_sql_data(
        self, show_unused: bool = False, show_invisible: bool = False, return_schema: bool = False
    ):
        """Return a flattened SQL-friendly representation of the parameter tree."""
        sql_dict: dict[str, Any] = {}
        for k, v in self.data.items():
            if v.used or show_unused:
                sql_data = v.get_sql_data(show_unused, show_invisible, return_schema)
                if isinstance(sql_data, dict):
                    for k2, v2 in sql_data.items():
                        sql_dict[k + REF_SEP + k2] = v2
                elif not isinstance(sql_data, InvisibleParam):
                    sql_dict[k] = sql_data
                elif isinstance(sql_data, InvisibleParam) and show_invisible:
                    sql_dict[k] = sql_data.jparam.get_sql_data(show_unused, show_invisible, return_schema)
        return sql_dict
    def load_from_db_result(self, db_result: DBResult):
        """Load the environment from a database result."""
        for k, v in db_result.environment.items():
            ref_list = k.split(REF_SEP)
            jparam = self
            for ref in ref_list[:-1]:
                jparam = jparam[ref]
            jparam[ref_list[-1]] = v


class Buffer(JDict):
    """Parameter node representing a callable and its arguments.

    A :class:`Buffer` caches the result of calling ``var`` with its arguments.
    The :class:`ResetCondition` controls when the cached value is cleared.
    """

    # jvar: Any
    reset_condition: ResetCondition
    value: Any | None = None

    def __init__(
        self, jvar, *args, reset_condition: ResetCondition = ResetCondition.NEVER, **kwargs
    ):
        """Initialize a :class:`Buffer`.

        Args:
            jvar: Function or callable object to invoke.
            *args: Positional arguments (possibly :class:`JParam` instances).
            reset_condition: Rule governing when to reevaluate the callable.
            **kwargs: Keyword arguments (possibly :class:`JParam` instances).
        """
        sig = signature(jvar)
        binding = sig.bind(*args, **kwargs)
        catchall_name = None
        for name, param in sig.parameters.items():
            if param.kind == Parameter.VAR_KEYWORD:
                catchall_name = name
        data = {}
        data['args_list'] = [k for k in binding.arguments if k not in binding.kwargs and k != catchall_name]
        data['kwargs_list'] = [k for k in binding.kwargs]
        data.update({k: binding.arguments[k] for k in data['args_list']})
        data.update({k: binding.kwargs[k] for k in data['kwargs_list']})
        data['jvar'] = InvisibleParam(jvar)
        super().__init__(reset_condition=reset_condition, data=data)

    def _get_value(self):
        if self.reset_condition == ResetCondition.ALWAYS:
            eval_kwargs = {k: self[k] for k in self['kwargs_list']}
            eval_args = [self[k] for k in self['args_list']]
            return self.jvar(*eval_args, **eval_kwargs)
        elif self.value is None:
            eval_kwargs = {k: self[k] for k in self['kwargs_list']}
            eval_args = [self[k] for k in self['args_list']]
            self.value = self.jvar(*eval_args, **eval_kwargs)
        else:
            self.set_usage(True)
        return self.value

    def _init_run(self, is_parent_path: bool, parent_env: JParam, leaf_changed: bool):
        # Reset value if condition is met
        if self.reset_condition == ResetCondition.ON_RUN or self.reset_condition == ResetCondition.ALWAYS or (
            is_parent_path and self.reset_condition == ResetCondition.ON_RUN_IF_PARENT_PATH
            or leaf_changed
        ):
            self.value = None
            return True
        return False
    def get_sql_data(
        self, show_unused: bool = False, show_invisible: bool = False, return_schema: bool = False
    ):
        """Return SQL data including the callable name."""
        sql_dict = super().get_sql_data(show_unused, show_invisible, return_schema)
        sql_dict["jvar"] = self.jvar.__module__ + "." + self.jvar.__qualname__
        return sql_dict


class YBuffer(Buffer):
    """'Dependent' buffer whose identity is defined entirely by its inputs.

    Only the inputs to a :class:`YBuffer` are saved to the database.
    """

    pass


class XBuffer(Buffer):
    """'Independent' buffer whose identity is defined by its output.

    Only the output of an :class:`XBuffer` is saved to the database. Inputs may
    include parameters, but :class:`Refer` is not supported as an input and
    inputs are not persisted.
    """

    dtype: Callable | None = None

    def __init__(
        self,
        jvar,
        *args,
        reset_condition: ResetCondition = ResetCondition.NEVER,
        dtype: Callable | None = None,
        **kwargs,
    ):
        """Initialize an :class:`XBuffer`."""
        super().__init__(jvar, *args, reset_condition=reset_condition, **kwargs)
        self.dtype = dtype

    def get_sql_data(
        self, show_unused: bool = False, show_invisible: bool = False, return_schema: bool = False
    ):
        """Return SQL data for the output value."""
        if return_schema:
            return get_sql_type(self._get_value()) if self.dtype is None else self.dtype.__name__
        else:
            return (
                cast_sql_type(self._get_value())
                if self.dtype is None
                else self.dtype(self._get_value())
            )


class Refer(JParam):
    """Parameter referencing another value in the environment.

    Args:
        reference_list: List of keys (or dotted string) walking down a JDict
            hierarchy in the environment.
    """

    reference_list: list[str]
    jparam: JParam | None = None

    def __init__(self, reference_list: str | list[str]):
        if isinstance(reference_list, str):
            reference_list = reference_list.split(REF_SEP)
        super().__init__(reference_list=reference_list)

    def get_name(self):
        """Return the dotted reference name."""
        return REF_SEP.join(self.reference_list)

    def _init_run(self, is_parent_path: bool, parent_env: JParam, leaf_changed: bool):
        jparam: JParam = parent_env
        for ref in self.reference_list:
            if isinstance(jparam, JList):
                jparam = jparam.data[int(ref)]
            else:   
                jparam = jparam.data[ref]
            if isinstance(jparam, InvisibleParam):
                jparam = jparam.jparam
        new_jparam = jparam.model_copy(deep=True)
        changed = new_jparam != self.jparam
        self.jparam = new_jparam
        self.jparam.init_run(is_parent_path, parent_env)
        return changed


    def _get_children(self):
        return [self.jparam] if self.jparam is not None else []

    def _get_value(self):
        return self.jparam.get_value()

    def get_sql_data(
        self, show_unused: bool = False, show_invisible: bool = False, return_schema: bool = False
    ):
        return self.jparam.get_sql_data(show_unused, show_invisible, return_schema)

def wrap_jparam(value: Any) -> JParam:
    """Wrap a raw value into an appropriate :class:`JParam` subclass."""
    if isinstance(value, JParam):
        return value
    if isinstance(value, dict):
        return JDict(data=value)
    if isinstance(value, list):
        return JList(data=value)
    if isinstance(value, tuple):
        return YBuffer(tuple, [wrap_jparam(v) for v in value])
    return JValue(value)


def evaluate_keys(env: JDict, keys: str|list[str]):
    if keys is None:
        return {}
    if isinstance(keys, str):
        return env[keys]
    result = env
    for key in keys:
        result = result[key]
    return result