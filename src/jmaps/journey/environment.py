"""Environment containers for named parameter sets.

`JEnv` wraps a mapping of parameter names to `JParam` with helpers for
filtering by semantic type and extracting hashable values used for caching.
"""
from jmaps.journey.param import JParam, ParamType

class JEnv(dict[str, JParam]):
    def __init__(self, name, params: dict[str, JParam]|None=None):
        """JEnv is a class that represents an environment used in a journey.
        It wraps a dictionary that is enforced to contain only JParams and a name for the environment.
        
        Args:
            name (str): The name of the environment.
            params (dict[str, JParam]): A dictionary of JParams, where the key is the name of the parameter.
        """
        super().__init__()
        self.name = name
        self.update(params)
    def get_params(self, types: list[ParamType]=[ParamType.SET, ParamType.VAR, ParamType.OPT, ParamType.LAMBDA]):
        '''Returns a dictionary of all JParams matching the given types.'''
        return {name: param for name, param in self.items() if param.type in types}
    def get_values(self, types: list[ParamType]=[ParamType.SET, ParamType.VAR, ParamType.OPT, ParamType.LAMBDA]):
        '''Returns a dictionary of all JParam.value objects matching the given types.'''
        return {name: param.value for name, param in self.items() if param.type in types}
    def get_hashable_values(self):
        '''Returns a dictionary of all JParam.value objects that affect the results of the journey. Used for saving and loading Path results.'''
        return self.get_values(types=[ParamType.SET, ParamType.VAR, ParamType.LAMBDA])
    def get_types(self, types: list[ParamType]=[ParamType.SET, ParamType.VAR, ParamType.OPT, ParamType.LAMBDA]):
        '''Returns a dictionary of all JParam.type objects.'''
        return {name: type(param) for name, param in self.get_values(types=types).items()}