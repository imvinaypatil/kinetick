from mongoengine import FloatField, StringField, DateTimeField, IntField, DynamicDocument
import datetime

from kinetick.enums import Timeframes

RESOLUTION = Timeframes.tuple()


class OHLC(DynamicDocument):
    tickerId = StringField(max_length=50, required=True)
    symbol = StringField(max_length=50, required=True)
    datetime = DateTimeField(required=True, default=datetime.datetime.utcnow)
    open = FloatField(required=True)
    high = FloatField(required=True)
    low = FloatField(required=True)
    close = FloatField(required=True)
    volume = IntField(required=True)
    vwap = FloatField()
    resolution = StringField(choices=RESOLUTION, default=Timeframes.MINUTE_T, unique_with=['tickerId', 'datetime'])
