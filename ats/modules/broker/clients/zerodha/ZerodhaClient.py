from ats.modules.broker.clients.BrokerClient import BrokerClient, TransactionTypeEnum
from ats.modules.position.aggregates.Position import RawBrokerOrder, PositionValidityEnum, PositionStatusEnum
from ats.shared.types.enums import InstrumentTypeEnum, ExchangeEnum


class ZerodhaClient(BrokerClient):

    def __init__(self, user_id, password, pin):
        self.user_id = user_id
        self.password = password
        self.pin = pin

    def connect(self):
        pass

    def placeOrder(self, tradingsymbol: str, transaction_type: TransactionTypeEnum, instrumentType: InstrumentTypeEnum,
                   quantity: int, exchange: ExchangeEnum, stoploss: int, validity: PositionValidityEnum,
                   price: float) -> RawBrokerOrder:
        pass

    def exitOrder(self, order: RawBrokerOrder) -> RawBrokerOrder:
        pass

    def orderStatus(self, order: RawBrokerOrder) -> PositionStatusEnum:
        # COMPLETE, REJECTED, CANCELLED, and OPEN
        if order.status is 'COMPLETE':
            return PositionStatusEnum.FILLED
        elif order.status is 'OPEN':
            return PositionStatusEnum.OPEN
        return  PositionStatusEnum.EXITED
