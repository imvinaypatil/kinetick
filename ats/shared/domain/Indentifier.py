from typing import TypeVar, Generic

T = TypeVar('T')


class Identifier(Generic[T]):
    def __init__(self, value: T):
        self.value: T = value

    def toString(self):
        return str(self.value)
