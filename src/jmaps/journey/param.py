"""Parameter primitives used to define Journey environments.

This module defines a small parameter tree abstraction (:class:`JParam` and
its concrete subclasses) that tracks usage, supports locking, and can produce
SQL-friendly representations of the current environment state.
"""

from enum import Enum, auto
from inspect import signature
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict

from pydantic import PrivateAttr, BaseModel, Field

from jmaps.journey.jmalc import get_sql_type, cast_sql_type

REF_SEP = "."


class ResetCondition(Enum):
    """When a :class:`Buffer` should reset its cached value."""

    NEVER = auto()
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
        self._init_run(is_parent_path, parent_env)
        for child in self._get_children():
            child.init_run(is_parent_path, parent_env)

    def merge_usage(self, mirror_param: "JParam"):
        """Merge ``used`` flags from a mirror parameter tree."""
        self.used = self.used or mirror_param.used
        for self_child, mirror_child in zip(
            self._get_children(), mirror_param._get_children()
        ):
            self_child.merge_usage(mirror_child)

    def _init_run(self, is_parent_path: bool, parent_env: "JParam"):
        """Subclass hook invoked at the start of a run."""
        pass

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

    def replace(self, other: "JDict | dict[str, Any]"):
        """Merge values from another :class:`JDict` into this one.

        Dtypes from ``self`` are preserved in the final object

        Args:
            other: Source dictionary or :class:`JDict` whose entries are merged.

        Raises:
            TypeError: If ``other`` cannot be converted to :class:`JDict`.
        """
        other = wrap_jparam(other)
        if not isinstance(other, JDict):
            raise TypeError("Other is not a JDict, cannot replace.")
        other.merge_dtypes(self)
        self.data.update(other.data)

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
        return sql_dict

    def merge_dtypes(self, other: "JDict"):
        """Propagate dtype information from another :class:`JDict`."""
        for k, v in self.data.items():
            if k in other.data:
                if (
                    isinstance(v, JValue)
                    and isinstance(other.data[k], JValue)
                    and other.data[k].dtype is not None
                ):
                    v.dtype = other.data[k].dtype
                elif isinstance(v, JDict) and isinstance(other.data[k], JDict):
                    v.merge_dtypes(other.data[k])


class Buffer(JDict):
    """Parameter node representing a callable and its arguments.

    A :class:`Buffer` caches the result of calling ``var`` with its arguments.
    The :class:`ResetCondition` controls when the cached value is cleared.
    """

    var: Any
    reset_condition: ResetCondition
    value: Any | None = None

    def __init__(
        self, var, *args, reset_condition: ResetCondition = ResetCondition.NEVER, **kwargs
    ):
        """Initialize a :class:`Buffer`.

        Args:
            var: Function or callable object to invoke.
            *args: Positional arguments (possibly :class:`JParam` instances).
            reset_condition: Rule governing when to reevaluate the callable.
            **kwargs: Keyword arguments (possibly :class:`JParam` instances).
        """
        binding = signature(var).bind(*args, **kwargs)
        data = binding.arguments
        for k, v in data.items():
            data[k] = wrap_jparam(v)
        super().__init__(var=var, reset_condition=reset_condition, data=binding.arguments)

    def _get_value(self):
        if self.value is None:
            eval_arguments = {k: v.get_value() for k, v in self.data.items()}
            self.value = self.var(**eval_arguments)
        return self.value

    def _init_run(self, is_parent_path: bool, parent_env: JParam):
        # Reset value if condition is met
        if self.reset_condition == ResetCondition.ON_RUN or (
            is_parent_path and self.reset_condition == ResetCondition.ON_RUN_IF_PARENT_PATH
        ):
            self.value = None

    def get_sql_data(
        self, show_unused: bool = False, show_invisible: bool = False, return_schema: bool = False
    ):
        """Return SQL data including the callable name."""
        sql_dict = super().get_sql_data(show_unused, show_invisible, return_schema)
        sql_dict["var"] = self.var.__name__
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
        var,
        *args,
        reset_condition: ResetCondition = ResetCondition.NEVER,
        dtype: Callable | None = None,
        **kwargs,
    ):
        """Initialize an :class:`XBuffer`."""
        super().__init__(var, *args, reset_condition=reset_condition, **kwargs)
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

    def _init_run(self, is_parent_path: bool, parent_env: JParam):
        jparam: JParam = parent_env
        for ref in self.reference_list:
            jparam = jparam.data[ref]
            if isinstance(jparam, InvisibleParam):
                jparam = jparam.jparam
        self.jparam = jparam.model_copy(deep=True)

    def _get_children(self):
        return [self.jparam]

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
    return JValue(value)