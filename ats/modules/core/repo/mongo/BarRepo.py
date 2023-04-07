from datetime import datetime

from mongoengine import Document, ObjectIdField, StringField, DateTimeField, FloatField, IntField

from ats.modules.core.aggregates.Bar import Bar, BarProps
from ats.modules.core.repo.BaseRepo import BaseRepo
from ats.shared.domain.UniqueEntityId import UniqueEntityId


class BarModel(Document):
    _id: ObjectIdField()
    symbol = StringField(required=True)
    timestamp = DateTimeField(required=True, default=datetime.utcnow)
    tickerId = StringField()
    open = FloatField()
    close = FloatField()
    high = FloatField()
    low = FloatField()
    volume = IntField()
    interval = StringField(unique_with=['symbol', 'timestamp'])


class BarRepo(BaseRepo[Bar]):

    def fromDomain(self, entity: Bar) -> Document:
        doc = BarModel(_id=entity.uniqueEntityId.toString(),
                       symbol=entity.symbol,
                       timestamp=entity.timestamp,
                       tickerId=entity.tickerId,
                       open=entity.open,
                       close=entity.close,
                       high=entity.high,
                       low=entity.low,
                       volume=entity.volume,
                       interval=entity.interval)
        return doc

    def toDomain(self, document: BarModel) -> Bar:
        tick = Bar(props=BarProps(
            symbol=document.symbol,
            timestamp=document.timestamp,
            tickerId=document.tickerId,
            open=document.open,
            close=document.close,
            high=document.high,
            low=document.low,
            volume=document.volume,
            interval=document.interval
        ), id=UniqueEntityId(document.id))
        return tick
