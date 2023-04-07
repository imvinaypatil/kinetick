import datetime
from typing import NamedTuple, Optional, Dict, Any

from ats.shared.domain.AggregateRoot import AggregateRoot
from ats.shared.domain.UniqueEntityId import UniqueEntityId


class QuoteProps(NamedTuple):
    symbol: str
    timestamp: datetime.datetime
    tickerId: str = None

    lastPrice: Optional[float] = None
    lastSize: Optional[int] = None
    askPrice: Optional[float] = None
    askSize: Optional[int] = None
    bidPrice: Optional[float] = None
    bidSize: Optional[int] = None


class Quote(AggregateRoot[QuoteProps]):
    def serialize(self) -> Dict[str, Any]:
        return {
            'id': self.uniqueEntityId.toString(),
            'symbol': self.props.symbol,
            'tickerId': self.props.tickerId,
            'timestamp': self.props.timestamp.isoformat(),

            'lastPrice': self.props.lastPrice,
            'lastSize': self.props.lastSize,
            'askPrice': self.props.askPrice,
            'askSize': self.props.askSize,
            'bidPrice': self.props.bidPrice,
            'bidSize': self.props.bidSize
        }


if __name__ == '__main__':
    quote = Quote(props=QuoteProps(symbol="ABC", tickerId="tid", timestamp=datetime.datetime.now()),
                  id=UniqueEntityId("id"))
    print(quote)
    print(quote.serialize())
