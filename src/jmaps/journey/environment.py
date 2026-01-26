"""Environment containers for named parameter sets.

`JEnv` wraps a mapping of parameter names to parameters with helpers for
evaluating lambda functions and extracting hashable values used for caching.
"""
from typing import Any
class JEnv(dict[str, Any]):
    def __init__(self, name, params: dict[str, Any]|None=None, triggered_lambdas:dict[str, Any]|None=None, invisible_params:set[str]|None=None):
        """JEnv is a class that represents an environment used in a journey.
        It wraps a dictionary that is enforced to contain only JParams and a name for the environment.
        
        Args:
            name (str): The name of the environment.
            params (dict[str, JParam]): A dictionary of JParams, where the key is the name of the parameter.
        """
        super().__init__()
        self.name = name
        self.update(params)
        self.triggered_lambdas = triggered_lambdas if triggered_lambdas is not None else {}
        self.invisible_params = invisible_params if invisible_params is not None else []
    def get_values(self):
        """Returns a dictionary of all values, evaluating any triggered lambda functions"""
        values = {}
        for name, param in self.items():
            if name in self.triggered_lambdas:
                param = self.evaluate_lambda(name, param)
            values[name] = param
        return values
    def get_visible_values(self):
        """Returns a dictionary of all visible parameters. Used for saving and loading Path results."""
        values = {}
        for name, param in self.items():
            if name not in self.invisible_params:
                if name in self.triggered_lambdas:
                    param = self.evaluate_lambda(name, param)
                values[name] = param
        return values
    def evaluate_lambda(self, lambda_id:str, lambda_val):
        if self.triggered_lambdas[lambda_id] is None:
            self.triggered_lambdas[lambda_id] = lambda_val()
        return self.triggered_lambdas[lambda_id]
    def reset_lambda_triggers(self):
        """Resets the values of all lambda triggers in the environment."""
        for key in self.triggered_lambdas.keys():
            self.triggered_lambdas[key] = None
    def update(self, other):
        if isinstance(other, JEnv):
            self.triggered_lambdas.update(other.triggered_lambdas)
            self.invisible_params.update(other.invisible_params)
        super().update(other)
