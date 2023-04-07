import abc

from ats.modules.position.aggregates.Position import PositionValidityEnum, RawBrokerOrder, PositionStateEnum
from ats.shared.types.enums import InstrumentTypeEnum, ExchangeEnum, TransactionTypeEnum, OrderTypeEnum


class Broker(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def connect(self):
        pass

    @abc.abstractmethod
    def createOrderDraft(self,
                         tradingsymbol: str,
                         transaction_type: TransactionTypeEnum,
                         instrumentType: InstrumentTypeEnum,
                         quantity: int,
                         exchange: ExchangeEnum,
                         stoploss: int,
                         validity: PositionValidityEnum,
                         price: float,
                         orderType: OrderTypeEnum,
                         ) -> RawBrokerOrder:
        pass

    @abc.abstractmethod
    def placeOrder(self, rawOrder: RawBrokerOrder) -> RawBrokerOrder:
        pass

    @abc.abstractmethod
    def cancelOrder(self, rawOrder: RawBrokerOrder) -> RawBrokerOrder:
        pass

    @abc.abstractmethod
    def orderStatus(self, rawOrder: RawBrokerOrder) -> PositionStateEnum:
        pass
