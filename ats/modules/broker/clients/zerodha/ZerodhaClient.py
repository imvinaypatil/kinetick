from ats.bin.Zerodha import Zerodha
from ats.modules.broker.clients.Broker import Broker, TransactionTypeEnum
from ats.modules.position.aggregates.Position import RawBrokerOrder, PositionValidityEnum, PositionStateEnum
from ats.shared.types.enums import InstrumentTypeEnum, ExchangeEnum, BrokerEnum, OrderTypeEnum


class ZerodhaClient(Broker):

    def __init__(self, user_id, password, pin):
        self.user_id = user_id
        self.password = password
        self.pin = pin
        self.zerodha = Zerodha(user_id, password, pin,
                               debug=False)

    def connect(self):
        self.zerodha.login()

    def orderStatus(self, rawOrder: RawBrokerOrder) -> PositionStateEnum:
        # COMPLETE, REJECTED, CANCELLED, and OPEN
        order = self.zerodha.order_by_id(rawOrder['order_id'])
        if order['status'] is 'COMPLETE':
            return PositionStateEnum.FILLED
        elif order['status'] is 'OPEN':
            return PositionStateEnum.OPEN
        return PositionStateEnum.EXITED

    def createOrderDraft(self, tradingsymbol: str, transaction_type: TransactionTypeEnum,
                         instrumentType: InstrumentTypeEnum, quantity: int, exchange: ExchangeEnum, stoploss: int,
                         validity: PositionValidityEnum, price: float, orderType: OrderTypeEnum) -> RawBrokerOrder:
        order_type = 'LIMIT' if orderType == OrderTypeEnum.LIMIT else 'MARKET'
        pos_type = 'CNC' if stoploss is None else 'CO'
        pos_type = 'MIS' if validity == PositionValidityEnum.INTRADAY and pos_type is not 'CO' else pos_type

        variety, product = self.zerodha.get_order_variety(instrumentType, pos_type)

        orderdraft = {
                     "variety": variety,
                     "exchange": exchange,
                     "tradingsymbol": tradingsymbol,
                     "order_type": order_type,
                     "transaction_type": transaction_type,
                     "product": product,
                     "quantity": quantity,
                     "price": price,
                     "trigger_price": stoploss,
                     "tag": 'kinetick',
                 },
        return RawBrokerOrder(None, BrokerEnum.ZERODHA).__dict__.update(orderdraft)

    def getOrder(self, orderId) -> RawBrokerOrder:
        order = self.zerodha.order_by_id(order_id=orderId)
        rawOrder = RawBrokerOrder(orderId, BrokerEnum.ZERODHA)
        rawOrder.__dict__ = order
        return rawOrder

    def placeOrder(self, rawOrder: RawBrokerOrder) -> RawBrokerOrder:
        order_id = self.zerodha.place_order(
            variety=rawOrder['variety'],
            tradingsymbol=rawOrder['tradingsymbol'],
            transaction_type=rawOrder['transaction_type'],
            quantity=rawOrder['quantity'],
            product=rawOrder['product'],
            order_type=rawOrder['order_type'],
            exchange=rawOrder['exchange'],
        )
        return self.getOrder(order_id)

    def cancelOrder(self, rawOrder: RawBrokerOrder) -> RawBrokerOrder:
        order_id = self.zerodha.exit_order(rawOrder['order_id'])
        return self.getOrder(order_id)
