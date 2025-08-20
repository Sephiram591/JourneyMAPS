from enum import Enum

PARAM_TYPES = Enum('ParamType', ['VAR', 'SET', 'OPT'])

class JParam:
    def __init__(self, value, type: PARAM_TYPES):
        self.value = value
        self.type = type
    def __getattr__(self, name):
        return getattr(self.value, name)

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return f"JParam(value={self.value}, type={self.type})"
    # Equality & comparison
    def __eq__(self, other):
        return self.value == (other.value if isinstance(other, JParam) else other)

    def __ne__(self, other):
        return self.value != (other.value if isinstance(other, JParam) else other)

    def __lt__(self, other):
        return self.value < (other.value if isinstance(other, JParam) else other)

    def __le__(self, other):
        return self.value <= (other.value if isinstance(other, JParam) else other)

    def __gt__(self, other):
        return self.value > (other.value if isinstance(other, JParam) else other)

    def __ge__(self, other):
        return self.value >= (other.value if isinstance(other, JParam) else other)

    # Arithmetic operators
    def __add__(self, other):
        return self.value + (other.value if isinstance(other, JParam) else other)

    def __radd__(self, other):
        return (other.value if isinstance(other, JParam) else other) + self.value

    def __sub__(self, other):
        return self.value - (other.value if isinstance(other, JParam) else other)

    def __rsub__(self, other):
        return (other.value if isinstance(other, JParam) else other) - self.value

    def __mul__(self, other):
        return self.value * (other.value if isinstance(other, JParam) else other)

    def __rmul__(self, other):
        return (other.value if isinstance(other, JParam) else other) * self.value

    def __truediv__(self, other):
        return self.value / (other.value if isinstance(other, JParam) else other)

    def __rtruediv__(self, other):
        return (other.value if isinstance(other, JParam) else other) / self.value

    def __floordiv__(self, other):
        return self.value // (other.value if isinstance(other, JParam) else other)

    def __rfloordiv__(self, other):
        return (other.value if isinstance(other, JParam) else other) // self.value

    def __mod__(self, other):
        return self.value % (other.value if isinstance(other, JParam) else other)

    def __rmod__(self, other):
        return (other.value if isinstance(other, JParam) else other) % self.value

    def __pow__(self, other):
        return self.value ** (other.value if isinstance(other, JParam) else other)

    def __rpow__(self, other):
        return (other.value if isinstance(other, JParam) else other) ** self.value

    # Negation and unary operators
    def __neg__(self):
        return -self.value

    def __pos__(self):
        return +self.value

    def __abs__(self):
        return abs(self.value)

    # Length (for sequences)
    def __len__(self):
        return len(self.value)

    # Iteration support
    def __iter__(self):
        return iter(self.value)

    # Indexing
    def __getitem__(self, key):
        return self.value[key]

    def __setitem__(self, key, val):
        self.value[key] = val

    # Hash support (optional, only if value is hashable)
    def __hash__(self):
        return hash(self.value)
        
class JVar(JParam):
    def __init__(self, value):
        super().__init__(value, PARAM_TYPES.VAR)
class JSet(JParam):
    def __init__(self, value):
        super().__init__(value, PARAM_TYPES.SET)
class JOpt(JParam):
    def __init__(self, value):
        super().__init__(value, PARAM_TYPES.OPT)