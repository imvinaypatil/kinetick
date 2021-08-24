from mongoengine import StringField, DateTimeField, IntField, FloatField, BooleanField, DynamicDocument
from datetime import datetime


# note: position / order / trade are used interchangeably through the app.
class Position(DynamicDocument):
    """ Position Data Model.
    holds information relating to either trade/order/position.
    """
    _tickerId = StringField(max_length=50, required=True, db_field="tickerId")
    _symbol = StringField(max_length=50, required=False, db_field="symbol")
    datetime = DateTimeField(required=True, default=datetime.utcnow)
    algo = StringField(max_length=100)
    direction = StringField(max_length=20, choices=('LONG', 'SHORT'))
    quantity = IntField(default=0)
    entry_time = DateTimeField()
    exit_time = DateTimeField()
    exit_reason = StringField()
    order_type = StringField()
    market_price = FloatField()
    target = FloatField(default=0.0)
    stop = FloatField(default=0.0)
    entry_price = FloatField(default=0.0)
    exit_price = FloatField(default=0.0)
    realized_pnl = FloatField(default=0.0)
    active = BooleanField(default=False)
    opt_ticker = StringField(max_length=50, required=False)
    opt_strike = FloatField(required=False)
    opt_type = StringField(require=False),
    opt_expiry = StringField(required=False),
    sec_type = StringField(default='cash')  # TODO add enum
    underlying = StringField(required=False)

    def open_position(self):
        if self.direction is None:
            raise Exception("no direction provided")
        if self.quantity is None:
            raise Exception("no quantity provided")
        if self.entry_time is None:
            self.entry_time = datetime.now()
        self.active = True

    def close_position(self):
        if self.direction is None:
            raise Exception("no direction provided")
        if self.quantity is None:
            raise Exception("no quantity provided")
        if self.exit_time is None:
            self.exit_time = datetime.now()
        self.active = False

    def pnl(self):
        pnl = abs(self.exit_price - self.entry_price)

        sl_hit = False
        if self.exit_price <= self.entry_price and self.direction == "LONG":
            sl_hit = True
        elif self.exit_price >= self.entry_price and self.direction == "SHORT":
            sl_hit = True

        pnl = -pnl if sl_hit else pnl
        pnl = pnl * self.quantity
        return pnl

    @property
    def status(self):
        return self.active
