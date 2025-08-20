from jmaps.journey.param import JParam, PARAM_TYPES

class JEnv:
    def __init__(self, name, params: dict[str, JParam]=None):
        self.name = name
        if params is None:
            params = {}
        self.params = params

    # Getters and setters
    def add_param(self, param: JParam):
        if param.name in self.params:
            raise ValueError(f"Parameter {param.name} already exists in JEnv {self.name}")
        self.params[param.name] = param
    def add_params(self, params: list[JParam]):
        for param in params:
            self.add_param(param)
    def get_param(self, name):
        return self.params[name]
    def get_params(self):
        return self.params
    def get_stripped_params(self, types: list[PARAM_TYPES]=[PARAM_TYPES.SET, PARAM_TYPES.VAR, PARAM_TYPES.OPT]):
        return {name: param.value for name, param in self.params.items() if param.type in types}
    def get_hashable_params(self):
        return self.get_stripped_params(types=[PARAM_TYPES.SET, PARAM_TYPES.VAR])
    def update_param(self, param: JParam):
        if param.name not in self.params:
            raise ValueError(f"Parameter {param.name} does not exist in JEnv {self.name}")
        self.params[param.name] = param
    def update_params(self, params: list[JParam]):
        for param in params:
            self.update_param(param)
            
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