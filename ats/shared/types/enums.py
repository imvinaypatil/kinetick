import enum


class ExchangeEnum(enum.Enum):
    SANDBOX = 'Sandbox'
    BSE = 'BSE'
    NSE = 'NSE'
    NFO = 'NFO'


class InstrumentTypeEnum(enum.Enum):
    OPTION = 'OPTION'
    FUTURE = 'FUTURE'
    STOCK = 'STOCK'
    INDEX = 'INDEX'


class BrokerEnum(enum.Enum):
    ZERODHA = 'ZERODHA'


class OrderTypeEnum(enum.Enum):
    MARKET = 'MARKET'
    LIMIT = 'LIMIT'


class TransactionTypeEnum(enum.Enum):
    BUY = 'BUY'
    SELL = 'SELL'
