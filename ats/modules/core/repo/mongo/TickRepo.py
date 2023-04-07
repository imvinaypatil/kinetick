from datetime import datetime

from mongoengine import Document, ObjectIdField, StringField, DateTimeField, FloatField, IntField

from ats.modules.core.aggregates.Tick import Tick, TickProps
from ats.modules.core.repo.BaseRepo import BaseRepo
from ats.shared.domain.UniqueEntityId import UniqueEntityId


class TickModel(Document):
    _id: ObjectIdField()
    symbol: StringField(required=True)
    timestamp: DateTimeField(required=True, default=datetime.utcnow)
    tickerId: StringField()
    lastPrice: FloatField()
    lastSize: IntField(default=0)


class TickRepo(BaseRepo[Tick]):

    def fromDomain(self, entity: Tick) -> Document:
        doc = TickModel(_id=entity.uniqueEntityId.toString(),
                        symbol=entity.symbol, timestamp=entity.timestamp,
                        tickerId=entity.tickerId, lastPrice=entity.lastPrice,
                        lastSize=entity.lastSize)
        return doc

    def toDomain(self, document: TickModel) -> Tick:
        tick = Tick(props=TickProps(
            symbol=document.symbol,
            tickerId=document.tickerId,
            timestamp=document.timestamp,
            lastPrice=document.lastPrice,
            lastSize=document.lastSize
        ), id=UniqueEntityId(document.id))
        return tick
