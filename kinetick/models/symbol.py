from mongoengine import Document, StringField

CURRENCY = ('INR', 'USD')


class Symbol(Document):
    tickerId = StringField(max_length=50, required=True, primary_key=True)
    symbol = StringField(max_length=50, required=True, unique=True)
    sec_type = StringField(max_length=50, required=False)
    exchange = StringField(max_length=50, required=False)
    currency = StringField(max_length=3, choices=CURRENCY)
