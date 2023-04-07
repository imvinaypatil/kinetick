from datetime import datetime
from typing import Optional, NamedTuple, Dict, Any

from ats.shared.core.Guard import GuardArgument, Guard
from ats.shared.core.Result import Result, ResultError
from ats.shared.domain.AggregateRoot import AggregateRoot
from ats.shared.domain.ErrorCodes import ErrorCodes
from ats.shared.domain.UniqueEntityId import UniqueEntityId
from ats.shared.infra.logging.logger import logger
from ats.shared.types.timeframe import Timeframe


class BarProps(NamedTuple):
    symbol: str
    timestamp: datetime
    tickerId: str = None

    open: Optional[float] = None
    close: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    volume: Optional[int] = None
    interval: Optional[Timeframe] = None


class Bar(AggregateRoot[BarProps]):
    def serialize(self) -> Dict[str, Any]:
        return {
            'id': self.uniqueEntityId.toString(),
            'symbol': self.props.symbol,
            'tickerId': self.props.tickerId,
            'timestamp': self.props.timestamp.isoformat(),
            'open': self.props.open,
            'close': self.props.close,
            'high': self.props.high,
            'low': self.props.low,
            'volume': self.props.volume,
            'interval': self.props.interval
        }

    def __str__(self):
        return str(self.serialize())

    @classmethod
    def createFromDict(cls, dictionary: Dict[str, Any]) -> Result['Bar']:
        nullChecklist = ['symbol', 'timestamp']
        guardResult = Guard.againstNullKeys(nullChecklist, dictionary=dictionary)
        if not guardResult.succeeded:
            return Result.fail(guardResult.error)
        Guard.setNoneIfKeyNotExists(['tickerId', 'open', 'close', 'high', 'low', 'volume', 'interval'],
                                    dictionary=dictionary)

        id = dictionary['id'] if 'id' in dictionary else UniqueEntityId()

        try:
            ohlc: Bar = Bar(
                BarProps(
                    symbol=dictionary['symbol'],
                    tickerId=dictionary['tickerId'],
                    timestamp=dictionary['timestamp'],
                    open=dictionary['open'],
                    close=dictionary['close'],
                    high=dictionary['high'],
                    low=dictionary['low'],
                    volume=dictionary['volume'],
                    interval=dictionary['interval']
                ),
                id=id
            )

            return Result.ok(ohlc)
        except Exception as e:
            logger.error(e)
            return Result.fail(ResultError(
                code=ErrorCodes.VALIDATION_FAILURE.value,
                message=str(e),
            ))

    @property
    def timestamp(self):
        return self.props.timestamp

    @property
    def symbol(self):
        return self.props.symbol

    @property
    def tickerId(self):
        return self.props.tickerId

    @property
    def open(self):
        return self.props.open

    @property
    def close(self):
        return self.props.close

    @property
    def high(self):
        return self.props.high

    @property
    def low(self):
        return self.props.low

    @property
    def volume(self):
        return self.props.volume

    @property
    def interval(self):
        return self.props.interval


if __name__ == '__main__':
    ohlc = Bar(props=BarProps(symbol="ABC", tickerId="tid", timestamp=datetime.now(), interval=Timeframe.DAY_1),
               id=UniqueEntityId("id"))
    print(ohlc)
    print(ohlc.serialize())

    ohlcResult = Bar.createFromDict({
        'symbol': 'symbol',
        'timestamp': datetime.now()
    })

    print(ohlcResult.isSuccess)
    print(ohlcResult.getValue())
