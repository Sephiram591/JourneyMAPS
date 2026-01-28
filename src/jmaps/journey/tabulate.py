
class Tab():
    def __init__(self, var, *args, **kwargs):
        self.var = var
        self.args = args
        self.kwargs = kwargs
        self.name = var.__name__
    def get_value():
        