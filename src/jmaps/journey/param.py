from ast import Call
from enum import Enum, auto
from inspect import signature
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict
from pydantic import PrivateAttr, BaseModel, Field
from jmaps.journey.jmalc import get_sql_type, cast_sql_type, get_sql_schema
import numpy as np
REF_SEP = '.'

class ResetCondition(Enum):
    """Enumeration for when a Buffer should reset its cached value."""
    NEVER = auto()
    ON_RUN = auto()
    ON_RUN_IF_PARENT_PATH = auto()

class JParam(ABC, BaseModel):
    """Abstract base class for a journey-maps parameter node.

    Subclasses must implement get_value and get_sql_data.
    """
    _locked: bool = PrivateAttr(default=False) # Whether the parameter is locked (cannot be changed)
    used: bool = False # Whether the parameter has been used in the current run
    # model_config = ConfigDict(
    #     validate_assignment=True
    # )
    def lock(self):
        self._locked = True
        for child in self._get_children():
            child.lock()
    def unlock(self):
        self._locked = False
        for child in self._get_children():
            child.unlock()
    def reset_usage(self):
        self.used=False
        for child in self._get_children():
            child.reset_usage()
    def set_usage(self, used=True):
        self.used = used
        for child in self._get_children():
            child.set_usage(used)
    def init_run(self, is_parent_path:bool, parent_env=None):
        parent_env = parent_env if parent_env is not None else self
        self._init_run(is_parent_path, parent_env)
        for child in self._get_children():
            child.init_run(is_parent_path, parent_env)
    def merge_usage(self, mirror_param):
        self.used = self.used or mirror_param.used
        for self_child, mirror_child in zip(self._get_children(), mirror_param._get_children()):
            self_child.merge_usage(mirror_child)
    def _init_run(self, is_parent_path:bool, parent_env):
        pass
    def get_value(self):
        self.used = True
        return self._get_value()
    @abstractmethod
    def _get_children(self):
        pass
    @abstractmethod
    def _get_value(self):
        """Evaluates and returns the value for this parameter.
        Returns:
            The result of evaluating this parameter.
        """
        pass
    @abstractmethod
    def get_sql_data(self, show_unused:bool=False, show_invisible:bool=False, return_schema:bool=False):
        """Produces a serializable representation for SQL-like workflows.

        Returns:
            A dictionary or string representation for storage or export.
        """
        pass
    
class JValue(JParam):
    value: Any
    dtype: Callable | None
    def __init__(self, value:Any, dtype:Callable|None=None):
        # get_sql_type(value) # Raises an error if the value is not a valid type
        super().__init__(value=value, dtype=dtype)
    def _get_value(self):
        return self.value
    def get_sql_data(self, show_unused:bool=False, show_invisible:bool=False, return_schema:bool=False):
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
    jparam: JParam
    def __init__(self, jparam:Any):
        super().__init__(jparam=wrap_jparam(jparam))
    def _get_value(self):
        return self.jparam.get_value()
    def get_sql_data(self, show_unused:bool=False, show_invisible:bool=False, return_schema:bool=False):
        return self.jparam.get_sql_data(show_unused, show_invisible, return_schema) if show_invisible else self
    def _get_children(self):
        return [self.jparam]

class JDict(JParam):
    """Base class representing a dictionary.

    Buffer caches its output for efficiency. The reset_condition property
    controls when this cache is cleared.

    Args:
        var: A function or class to invoke.
        *args: Positional arguments (possibly JParam).
        reset_condition: Rule governing cache invalidation.
        **kwargs: Keyword arguments (possibly JParam).
    """
    data: Dict[str, JParam] = Field(default_factory=dict)
    def __init__(self, data:dict[str, Any], **kwargs):
        for k, v in data.items():
            data[k] = wrap_jparam(v)
        super().__init__(data=data, **kwargs)
    def keys(self):
        return self.data.keys()
    def replace(self, other):
        other = wrap_jparam(other)
        if not isinstance(other, JDict):
            raise TypeError("Other is not a JDict, cannot replace.")
        other.merge_dtypes(self)
        self.data.update(other.data)
        # other_schema = get_sql_schema(other.get_sql_data(show_unused=True, show_invisible=True))
        # this_schema = get_sql_schema(self.get_sql_data(show_unused=True, show_invisible=True))
        # if other_schema.viewitems() <= this_schema.viewitems():
        #     self.data.update(other.data)
        # else:
        #     raise ValueError(f"Other schema is not a subset of this schema.")
    def __getitem__(self, key):
        return self.data[key].get_value()
    def __setitem__(self, key, value):
        if self._locked:
            raise AttributeError(f"JDict is locked, parameters cannot be changed by the user.")
        if not isinstance(key, str):
            raise TypeError("JDict keys must be strings")
        if key in self.data and isinstance(self.data[key], JValue) and not isinstance(value, JValue) and self.data[key].dtype is not None:
            self.data[key].value = value
        else:
            self.data[key] = wrap_jparam(value)
    
    def __getattr__(self, key):
        try:
            super().__getattr__(key)
        except AttributeError:
            return self.data[key].get_value()

    def __setattr__(self, key, value):
        try:
            super().__setattr__(key, value)
        except ValueError:
            if self._locked:
                raise AttributeError(f"JDict is locked, parameters cannot be changed by the user.")
            if not isinstance(key, str):
                raise TypeError("JDict keys must be strings")
            if key in self.data and isinstance(self.data[key], JValue) and not isinstance(value, JValue) and self.data[key].dtype is not None:
                self.data[key].value = value
            else:
                self.data[key] = wrap_jparam(value)
    def items(self):
        for k, v in self.data.items():
            yield (k, v.get_value()) 

    def _get_value(self):
        return self

    def _get_children(self):
        return self.data.values()
    
    def get_sql_data(self, show_unused:bool=False, show_invisible:bool=False, return_schema:bool=False):
        sql_dict = {}
        for k, v in self.data.items():
            if v.used or show_unused:
                sql_data = v.get_sql_data(show_unused, show_invisible, return_schema)
                if isinstance(sql_data, dict):
                    for k2, v2 in sql_data.items():
                        sql_dict[k + REF_SEP + k2] = v2
                elif not isinstance(sql_data, InvisibleParam):
                    sql_dict[k] = sql_data
        return sql_dict
    def merge_dtypes(self, other):
        for k, v in self.data.items():
            if k in other.data:
                if isinstance(v, JValue) and isinstance(other.data[k], JValue) and other.data[k].dtype is not None:
                    v.dtype = other.data[k].dtype
                elif isinstance(v, JDict) and isinstance(other.data[k], JDict):
                    v.merge_dtypes(other.data[k])

class Buffer(JDict):
    """Base class representing a composition of a callable and its arguments.

    Buffer caches its output for efficiency. The reset_condition property
    controls when this cache is cleared.

    Args:
        var: A function or class to invoke.
        *args: Positional arguments (possibly JParam).
        reset_condition: Rule governing cache invalidation.
        **kwargs: Keyword arguments (possibly JParam).
    """
    var: Any
    reset_condition: ResetCondition
    value: Any|None = None
    def __init__(self, var, *args, reset_condition: ResetCondition = ResetCondition.NEVER, **kwargs):
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

    def _init_run(self, is_parent_path:bool, parent_env:JParam):
        # Reset value if condition is met
        if self.reset_condition == ResetCondition.ON_RUN or \
           (is_parent_path and self.reset_condition == ResetCondition.ON_RUN_IF_PARENT_PATH):
            self.value = None
    def get_sql_data(self, show_unused:bool=False, show_invisible:bool=False, return_schema:bool=False):
        sql_dict = super().get_sql_data(show_unused, show_invisible, return_schema)
        sql_dict['var'] = self.var.__name__
        return sql_dict

class YBuffer(Buffer):
    """'Dependent' Buffer: Represents a composition of a callable and its arguments. 
    A YBuffer is fully defined by its inputs, and only its inputs are saved to the database.

    Buffer caches its output for efficiency. The reset_condition property
    controls when this cache is cleared.

    """
    pass
        
class XBuffer(Buffer):
    """'Independent' Buffer: Represents a composition of a callable and its arguments. 
    A XBuffer is fully defined by its output, and only its output is saved to the database.

    Buffer caches its output for efficiency. The reset_condition property
    controls when this cache is cleared.

    XBuffer values can have input arguments, however, the Refer object is not supported as a possible input. 
    These inputs are not saved to the database.

    Methods:
        get_sql_data(): Returns the output (evaluated via get_value).
    """
    dtype: Callable | None = None
    def __init__(self, var, *args, reset_condition: ResetCondition = ResetCondition.NEVER, dtype:Callable|None=None, **kwargs):
        super().__init__(var, *args, reset_condition=reset_condition, **kwargs)
        self.dtype=None
    def get_sql_data(self, show_unused:bool=False, show_invisible:bool=False, return_schema:bool=False):
        if return_schema:
            return get_sql_type(self._get_value()) if self.dtype is None else self.dtype.__name__
        else:
            return cast_sql_type(self._get_value()) if self.dtype is None else self.dtype(self._get_value())

class Refer(JParam):
    """Parameter referencing another value in the environment.

    Args:
        reference_list: List of keys walking down a dictionary in the environment.

    Resolves by successive __getitem__ calls using reference_list.
    """
    reference_list: list[str]
    jparam: JParam|None = None
    def __init__(self, reference_list:str|list[str]):
        if isinstance(reference_list, str):
            reference_list = reference_list.split(REF_SEP)
        super().__init__(reference_list=reference_list)
    def get_name(self):
        return REF_SEP.join(self.reference_list)
    def _init_run(self, is_parent_path:bool, parent_env:JParam):
        jparam = parent_env
        for ref in self.reference_list:
            jparam = jparam.data[ref]
            if isinstance(jparam, InvisibleParam):
                jparam = jparam.jparam
        self.jparam = jparam.model_copy(deep=True)
    def _get_children(self):
        return [self.jparam]
    def _get_value(self):
        return self.jparam.get_value()
    def get_sql_data(self, show_unused:bool=False, show_invisible:bool=False, return_schema:bool=False):
        return self.jparam.get_sql_data(show_unused, show_invisible, return_schema)


def wrap_jparam(value:Any) -> JParam:
    if isinstance(value, JParam):
        return value
    if isinstance(value, dict):
        return JDict(data=value)
    return JValue(value)