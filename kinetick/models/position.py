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
    _direction = StringField(max_length=20, choices=('LONG', 'SHORT'), db_field="direction")
    _quantity = IntField(default=0, db_field="quantity")
    entry_time = DateTimeField()
    exit_time = DateTimeField()
    exit_reason = StringField()
    order_type = StringField()  # LIMIT/MARKET
    _broker_order_id = StringField(db_field="broker_order_id")
    _variety = StringField(db_field="variety")
    market_price = FloatField()
    target = FloatField(default=0.0)
    stop = FloatField(default=0.0)
    entry_price = FloatField(default=0.0)
    exit_price = FloatField(default=0.0)
    realized_pnl = FloatField(default=0.0)
    _active = BooleanField(default=False, db_field="active")
    opt_ticker = StringField(max_length=50, required=False)
    opt_strike = FloatField(required=False)
    opt_type = StringField(require=False),
    opt_expiry = StringField(required=False),
    sec_type = StringField(default='STK')  # TODO add enum
    underlying = StringField(required=False)

    def open_position(self):
        if self._direction is None:
            raise Exception("no direction provided")
        if self._quantity is None:
            raise Exception("no quantity provided")
        if self.entry_time is None:
            self.entry_time = datetime.now()
        self._active = True

    def close_position(self):
        if self._direction is None:
            raise Exception("no direction provided")
        if self._quantity is None:
            raise Exception("no quantity provided")
        if self.exit_time is None:
            self.exit_time = datetime.now()
        self._active = False

    def pnl(self):
        pnl = abs(self.exit_price - self.entry_price)

        sl_hit = False
        if self.exit_price <= self.entry_price and self._direction == "LONG":
            sl_hit = True
        elif self.exit_price >= self.entry_price and self._direction == "SHORT":
            sl_hit = True

        pnl = -pnl if sl_hit else pnl
        pnl = pnl * self._quantity
        return pnl

    @property
    def active(self):
        return self._active

    @property
    def ticker_id(self):
        return self._tickerId

    @property
    def symbol(self):
        return self._symbol

    @property
    def direction(self):
        return self._direction

    @property
    def quantity(self):
        return self._quantity

    @property
    def broker_order_id(self):
        return self._broker_order_id

    @property
    def variety(self):
        return self._variety
