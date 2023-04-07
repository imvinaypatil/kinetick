from typing import TypeVar, Optional, Generic

from ats.shared.domain.UniqueEntityId import UniqueEntityId

T = TypeVar('T')


class Entity(Generic[T]):
    def __init__(self, props: T, id: Optional[UniqueEntityId] = None):
        self.id = id or UniqueEntityId()
        self.props: T = props
