import datetime
from typing import NamedTuple, Optional, Dict, Any

from ats.shared.domain.AggregateRoot import AggregateRoot
from ats.shared.domain.UniqueEntityId import UniqueEntityId


class TickProps(NamedTuple):
    symbol: str
    timestamp: datetime.datetime
    tickerId: str = None
    lastPrice: Optional[float] = None
    lastSize: Optional[int] = None


class Tick(AggregateRoot[TickProps]):
    def serialize(self) -> Dict[str, Any]:
        return {
            'id': self.uniqueEntityId.toString(),
            'symbol': self.props.symbol,
            'tickerId': self.props.tickerId,
            'timestamp': self.props.timestamp.isoformat(),
            'lastPrice': self.props.lastPrice,
            'lastSize': self.props.lastSize,
        }

    @property
    def symbol(self):
        return self.props.symbol

    @property
    def timestamp(self):
        return self.props.timestamp

    @property
    def lastPrice(self):
        return self.props.lastPrice

    @property
    def lastSize(self):
        return self.props.lastSize

    @property
    def tickerId(self):
        return self.props.tickerId


if __name__ == '__main__':
    tick = Tick(props=TickProps(symbol="ABC", tickerId="tid", timestamp=datetime.datetime.now()),
                id=UniqueEntityId("id"))
    print(tick)
    print(tick.serialize())
    print(tick.tickerId)
