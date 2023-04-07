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


class DateTimeFormatEnum(enum.Enum):
    DATE_FORMAT = "%Y%m%d"
    DATE_TIME_FORMAT = "%Y%m%d %H:%M:%S"
    DATE_TIME_FORMAT_LONG = "%Y-%m-%d %H:%M:%S"
    DATE_TIME_FORMAT_LONG_MILLISECS = "%Y-%m-%d %H:%M:%S.%f"
    DATE_TIME_FORMAT_HISTORY = "%Y%m%d %H:%M:%S"
    DATE_FORMAT_HISTORY = "%Y-%m-%d"


class 