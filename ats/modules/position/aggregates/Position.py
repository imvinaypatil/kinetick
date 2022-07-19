import enum

from ats.shared.types.enums import InstrumentTypeEnum, BrokerEnum


class PositionValidityEnum(enum.Enum):
    INTRADAY = 'INTRADAY'
    NORMAL = 'NORMAL'


class PositionStatusEnum(enum.Enum):
    CREATED = 'CREATED'
    OPEN = 'OPEN'
    FILLED = 'FILLED'
    EXITED = 'EXITED'
    # state transition: CREATED -> OPEN -> FILLED -> EXITED


class PositionDirectionEnum(enum.Enum):
    LONG = 'LONG'
    SHORT = 'SHORT'
    NEUTRAL = 'NEUTRAL'


class RawBrokerOrder:
    def __init__(self, orderId: str, brokerRef: BrokerEnum):
        self.orderId = orderId
        self.brokerRef = brokerRef


class Position:
    ticker = ""
    tickerId = ""
    instrumentType = ""  # { instrumentType: OPT / FUT / STOCK / INDEX }
    exchange = ""
    strategyName = ""  # str
    entryPrice = 0
    buyPrice = 0
    sellPrice = 0
    quantity = 0
    pnl = 0
    expiryTimerInSeconds = 0
    validityTimestamp = ""
    validityType = PositionValidityEnum.NORMAL
    direction = PositionDirectionEnum.NEUTRAL

    createdAt = ""
    filledAt = ""
    exitedAt = ""
    stoploss = ""
    target = 0
    status = PositionStatusEnum.CREATED
    rawBrokerOrder = None

    def __init__(self, ticker: str, tickerId: str, exchange: str, entryPrice: float, direction):
        self.ticker = ticker
        self.tickerId = tickerId
        self.exchange = exchange
        self.entryPrice = entryPrice
        self.direction = direction

    def setInstrumentType(self, instrumentType: InstrumentTypeEnum) -> None:
        self.instrumentType = instrumentType

    def setStrategyName(self, strategyName: str) -> None:
        self.strategyName = strategyName

    def setBrokerOrder(self, brokerOrder: RawBrokerOrder) -> None:
        self.rawBrokerOrder = brokerOrder

    def setValidityType(self, validity: PositionValidityEnum):
        self.validityType = validity

    def isActive(self):
        return self.status is not PositionStatusEnum.EXITED

    def setStatus(self, status: PositionStatusEnum):
        self.status = status

    def getBrokerOrder(self) -> RawBrokerOrder:
        return self.rawBrokerOrder

    def getStatus(self) -> PositionStatusEnum:
        return self.status

    def setDirection(self, direction: PositionDirectionEnum):
        self.direction = direction
        return self.direction
