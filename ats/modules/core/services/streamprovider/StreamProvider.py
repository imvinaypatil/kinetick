import abc
from typing import Callable

from ats.modules.core.aggregates.Bar import Bar
from ats.modules.core.aggregates.Quote import Quote
from ats.modules.core.aggregates.Tick import Tick
from ats.shared.types.timeframe import Timeframe
from kinetick.instrument import Instrument
from pandas import DataFrame


class StreamProvider(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def stream(self):
        pass

    @abc.abstractmethod
    def subscribe(self, instrument: Instrument, orderbook: bool = None, tick: bool = None, quote: bool = None):
        pass

    @abc.abstractmethod
    def addTickHandler(self, handler: Callable[[Instrument, Tick], None]) -> None:
        pass

    @abc.abstractmethod
    def addBarHandler(self, handler: Callable[[Instrument, Bar], None]) -> None:
        pass

    @abc.abstractmethod
    def addQuoteHandler(self, handler: Callable[[Instrument, Quote], None]) -> None:
        pass

    @abc.abstractmethod
    def getBars(self, instrument: Instrument, interval: Timeframe, window: int) -> DataFrame:
        # return  df: open, close, high, low, volume
        pass
