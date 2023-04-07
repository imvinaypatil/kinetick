import abc
from typing import Generic, TypeVar

from mongoengine import Document


EntityType = TypeVar("EntityType")


class BaseRepo(metaclass=abc.ABCMeta, Generic[EntityType]):
    @abc.abstractmethod
    def fromDomain(self, entity: EntityType) -> Document:
        pass

    @abc.abstractmethod
    def toDomain(self, document: Document) -> EntityType:
        pass

    def save(self, entity: EntityType) -> EntityType:
        doc: Document = self.fromDomain(entity)
        doc.save()
        return self.toDomain(doc)
