"""Parameter abstractions for journeys.

Defines `JParam` and typed wrappers used to distinguish how values participate
in optimization and configuration.
"""
from enum import Enum
import copy

class ParamType(Enum):
    VAR = 'VAR'
    SET = 'SET'
    OPT = 'OPT'
    LAMBDA = 'LAMBDA'
class JParam:
    """Lightweight wrapper adding type semantics to parameter values.

    Operators delegate to the underlying `value` to behave like the wrapped
    object, while retaining metadata in `type`.
    """
    def __init__(self, value, jtype: ParamType):
        '''
        JParam is a class that represents a parameter used in a journey.
        It contains a value and a type. The value can be any type, and the operators on JParam are overloaded to use the value's operators.
        Args:
            value (any): The value of the parameter.
            jtype (ParamType): The type of the parameter.
        '''
        self.value = value
        self.type = jtype
    def __getattr__(self, name):
        return getattr(self.value, name)

    def __deepcopy__(self, memo):
        # Create a new JParam with deepcopied value and type
        new_value = copy.deepcopy(self.value, memo)
        new_type = self.type  # Enum is immutable, no need to deepcopy
        new_obj = self.__class__.__new__(self.__class__)  # Avoid __init__ recursion
        new_obj.value = new_value
        new_obj.type = new_type
        memo[id(self)] = new_obj
        return new_obj
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
        
    # Boolean support
    def __bool__(self):
        return bool(self.value)
        
class JVar(JParam):
    def __init__(self, value):
        super().__init__(value, ParamType.VAR)
class JSet(JParam):
    def __init__(self, value):
        super().__init__(value, ParamType.SET)
class JOpt(JParam):
    def __init__(self, value):
        super().__init__(value, ParamType.OPT)
class JLambda(JParam):
    def __init__(self, fn):
        self.fn = fn
        self.type = ParamType.LAMBDA

    @property
    def value(self):
        return self.fn()