from jmaps.journey.param import JParam, PARAM_TYPES

class JEnv:
    def __init__(self, name, params: dict[str, JParam]=None):
        '''
        JEnv is a class that represents an environment used in a journey.
        It contains a dictionary of JParams and a name for the environment.
        Args:
            name (str): The name of the environment.
            params (dict[str, JParam]): A dictionary of JParams, where the key is the name of the parameter.
        '''
        self.name = name
        if params is None:
            params = {}
        self.params = params

    # Getters and setters
    def add_param(self, name: str, param: JParam):
        '''Adds a parameter, raising an error if a parameter with the same name already exists.'''
        if name in self.params:
            raise ValueError(f"Parameter {name} already exists in JEnv {self.name}")
        self.params[name] = param
    def add_params(self, params: dict[str, JParam]):
        '''Adds the parameters, raising an error if a parameter with the same name already exists.'''
        for name, param in params.items():
            self.add_param(name, param)
    def get_param(self, name):
        '''Returns the parameter with the given name.'''
        return self.params[name]
    def get_params(self):
        '''Returns a copy of the parameters.'''
        return self.params.copy()
    def get_stripped_params(self, types: list[PARAM_TYPES]=[PARAM_TYPES.SET, PARAM_TYPES.VAR, PARAM_TYPES.OPT]):
        '''Returns a dictionary of all parameters with the given types.'''
        return {name: param.value for name, param in self.params.items() if param.type in types}
    def get_hashable_params(self):
        '''Returns a dictionary of all parameters that affect the results of the journey. Used for saving and loading Path results.'''
        return self.get_stripped_params(types=[PARAM_TYPES.SET, PARAM_TYPES.VAR])
    def update_param(self, name: str, param: JParam):
        '''Updates the parameter with the given name, raising an error if the parameter does not exist.'''
        if name not in self.params:
            raise ValueError(f"Parameter {name} does not exist in JEnv {self.name}")
        self.params[name] = param
    def update_params(self, params: dict[str, JParam]):
        '''Updates the parameters, raising an error if a parameter with the same name does not exist.'''
        for name, param in params.items():
            self.update_param(name, param)
    def __len__(self):
        return len(self.params)
    def __getitem__(self, key):
        return self.params[key]
    def __setitem__(self, key, value):
        self.params[key] = value
    def __delitem__(self, key):
        del self.params[key]
    def __iter__(self):
        return iter(self.params)
    def __contains__(self, key):
        return key in self.params
    def __len__(self):
        return len(self.params)