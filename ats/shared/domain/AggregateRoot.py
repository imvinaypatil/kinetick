from typing import TypeVar, Generic
from ats.shared.domain.Entity import Entity
from ats.shared.domain.UniqueEntityId import UniqueEntityId

T = TypeVar('T')


class AggregateRoot(Entity[Generic[T]]):
    @property
    def uniqueEntityId(self) -> UniqueEntityId:
        return self.id
