"""Environment containers for named parameter sets.

`JEnv` wraps a mapping of parameter names to parameters with helpers for
evaluating lambda functions and extracting hashable values used for caching.
"""
from typing import Any, Set, Dict, Optional, Union
from pydantic import BaseModel, PrivateAttr, ConfigDict
from jmaps.journey.serial import serialize_default
from genson import SchemaBuilder
import orjson
import copy

class JEnv(BaseModel):
    params: Dict[str, Any] = {}
    one_shot_fns: Set[str] = set()
    invisible_params: Set[str] = set()
    # Private attributes for runtime state
    _one_shot_values: Dict[str, Any] = PrivateAttr(default_factory=dict)
    _param_usage: Dict[str, bool] = PrivateAttr(default_factory=dict)
    _locked: bool = PrivateAttr(default=False)


    model_config = ConfigDict(
        extra='forbid',
        validate_assignment=True
    )

    def __init__(self, **data):
        super().__init__(**data)
        # Set up runtime state for triggers and usage
        self._one_shot_values = {k: None for k in (self.one_shot_fns or set())}
        self._param_usage = {k: False for k in self.params}

    def __getitem__(self, key):
        if key in self.one_shot_fns:
            return self.get_one_shot_value(key)
        else:
            self._param_usage[key] = True
            # Try model attribute, fallback to dict
            return self.params[key]

    def __setitem__(self, key, value) -> None:
        if self._locked:
            raise AttributeError(f"Environment is locked, parameters cannot be changed by the user.")
        if key in self.one_shot_fns:
            self._one_shot_values[key] = None
        # Add/replace attribute and params
        self.params[key] = value

    def reset_param_usage(self):
        if self._locked:
            raise AttributeError(f"Environment is locked, parameters cannot be changed by the user.")
        self._param_usage = {k: False for k in self.params}
    def get_usage_schema(self):
        
    def get_values(self):
        """Returns a dictionary of all values, evaluating any triggered lambda functions"""
        values = {}
        for name, param in self.params.items():
            if name in self.one_shot_fns:
                param = self.get_one_shot_value(name)
            values[name] = param
        return values

    def get_visible_values(self):
        """Returns a dictionary of all visible parameters. Used for saving and loading Path results."""
        values = {}
        for name, param in self.params.items():
            if name not in self.invisible_params:
                if name in self.one_shot_fns:
                    param = self.get_one_shot_value(name)
                values[name] = param
        return values

    def get_one_shot_value(self, one_shot_key: str):
        if self._one_shot_values[one_shot_key] is None:
            # Lambdas stored in params
            one_shot_func = self.params[one_shot_key]
            self._one_shot_values[one_shot_key] = one_shot_func()
        return self._one_shot_values[one_shot_key]

    def reset_one_shot_values(self):
        """Resets the values of all one shot lambda functions in the environment."""
        if self._locked:
            raise AttributeError(f"Environment is locked, parameters cannot be changed by the user.")
        for key in self.one_shot_fns:
            self._one_shot_values[key] = None

    def update(self, other: Union['JEnv', Dict[str, Any]]):
        if self._locked:
            raise AttributeError(f"Environment is locked, parameters cannot be changed by the user.")
        if isinstance(other, JEnv):
            self.one_shot_fns.update(other.one_shot_fns)
            self.invisible_params.update(other.invisible_params)
            self._param_usage.update(other._param_usage)
            self._one_shot_values.update(other._one_shot_values)
            self.params.update(other.params)
        elif isinstance(other, dict):
            self.params.update(other)
        else:
            raise TypeError(f"Unsupported type for update: {type(other)}")
    def copy(self):
        JEnv().update(self)
    def lock(self):
        self._locked=True
    def unlock(self):
        self._locked=False