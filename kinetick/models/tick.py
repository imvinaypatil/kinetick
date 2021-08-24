from mongoengine import StringField, DateTimeField, FloatField, IntField, DynamicDocument
import datetime

tick_types = ('TICK', 'QUOTE')


class Tick(DynamicDocument):
    tickerId = StringField(max_length=50, required=True)
    symbol = StringField(max_length=50, required=False)
    datetime = DateTimeField(required=True, default=datetime.datetime.utcnow)
    open = FloatField(required=False)
    high = FloatField(required=False)
    low = FloatField(required=False)
    close = FloatField(required=False)
    volume = IntField(required=False)
    vwap = FloatField()
    buy = FloatField()
    buysize = IntField()
    sell = FloatField()
    sellsize = IntField()
    last = FloatField()
    lastSize = IntField()
    ask = FloatField()
    asksize = IntField()
    bid = FloatField()
    bidSize = IntField()
    kind = StringField(default="TICK", choices=tick_types)
    meta = {
        'indexes': ['tickerId', 'datetime', 'kind']
    }
