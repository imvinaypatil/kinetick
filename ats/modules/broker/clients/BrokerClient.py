import abc
import enum

from ats.modules.position.aggregates.Position import PositionValidityEnum, RawBrokerOrder, PositionStatusEnum
from ats.shared.types.enums import InstrumentTypeEnum, ExchangeEnum, TransactionTypeEnum


class BrokerClient(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def connect(self):
        pass

    def createOrderDraft(self, tradingsymbol: str,
                         transaction_type: TransactionTypeEnum,
                         instrumentType: InstrumentTypeEnum,
                         quantity: int,
                         exchange: ExchangeEnum,
                         stoploss: int,
                         validity: PositionValidityEnum,
                         price: float) -> RawBrokerOrder:
        pass

    @abc.abstractmethod
    def placeOrder(self, order: RawBrokerOrder) -> RawBrokerOrder:
        pass

    @abc.abstractmethod
    def cancelOrder(self, order: RawBrokerOrder) -> RawBrokerOrder:
        pass

    @abc.abstractmethod
    def orderStatus(self, order: RawBrokerOrder) -> PositionStatusEnum:
        pass
