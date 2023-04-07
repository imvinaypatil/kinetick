import enum
import time
from datetime import datetime
from typing import Callable, List

from dateutil.parser import parse as parse_date

import pandas as pd
from pandas import DataFrame

from ats.modules.core.services.streamprovider.StreamProvider import StreamProvider
from ats.modules.core.services.streamprovider.WebullClient import WebullClient
from ats.modules.core.aggregates.Bar import Bar
from ats.modules.core.aggregates.Quote import Quote
from ats.modules.core.aggregates.Tick import Tick
from ats.shared.config.Appconfig import Appconfig
from ats.shared.infra.logging.logger import logger
from ats.shared.types.enums import DateTimeFormatEnum
from ats.shared.utils import asynctools
from kinetick.instrument import Instrument

from ats.shared.types.timeframe import Timeframe


class BarTypeEnum(enum.Enum):
    OHLC = 'OHLC'


class BarTimeTypeEnum(enum.Enum):
    DATE = 'DATE'
    TIME = 'TIME'
    DATE_TIME_LOCAL = 'DATETIMELOCAL'
    DATE_TIME_UTC = 'DATETIMEUTC'


class Blotter(StreamProvider):
    def stream(self):
        self.streamProvider.stream()

    def subscribe(self, instrument: Instrument, orderbook: bool = None, tick: bool = None, quote: bool = None):
        self.streamProvider.subscribe(instrument, orderbook, tick, quote)

    def addTickHandler(self, handler: Callable[[Instrument, Tick], None]) -> None:
        self.tickCallbacks.append(handler)

    def addBarHandler(self, handler: Callable[[Instrument, Bar], None]) -> None:
        self.barCallbacks.append(handler)

    def addQuoteHandler(self, handler: Callable[[Instrument, Quote], None]) -> None:
        self.streamProvider.addQuoteHandler(handler)

    def getBars(self, instrument: Instrument, interval: Timeframe, window: int) -> DataFrame:
        return self.streamProvider.getBars(instrument, interval, window)

    @asynctools.multitasking.task
    def _call_tick_handlers(self, instrument: Instrument, tick: Tick):
        for callback in self.tickCallbacks:
            try:
                callback(instrument, tick)
            except Exception as e:
                logger.error(f"Blotter::_call_tick_handlers: Caught error calling ${callback}", e)

    @asynctools.multitasking.task
    def _call_bar_handlers(self, instrument: Instrument, bar: Bar):
        for callback in self.barCallbacks:
            try:
                callback(instrument, bar)
            except Exception as e:
                logger.error(f"Blotter::_call_bar_handlers: Caught error calling ${callback}", e)

    def _on_tick_handler(self, instrument: Instrument, tick: Tick):
        logger.info(f'{instrument}: {tick}')
        self._call_tick_handlers(instrument, tick)

        symbol = instrument

        timestamp = datetime.strptime(tick.timestamp,
                                      DateTimeFormatEnum.DATE_TIME_FORMAT_LONG_MILLISECS.value)

        try:
            timestamp = parse_date(timestamp)
        except Exception as e:
            logger.warn("Blotter::on_tick_handler caught exception while parsing tick timestamp", e)

        if symbol not in self._raw_bars:
            self._raw_bars[symbol] = self._raw_bars['~']

        if symbol not in self._bars:
            self._bars[symbol] = self._bars['~']

        # add tick to raw self._bars
        tick_data = pd.DataFrame(index=['timestamp'],
                                 data={'timestamp': timestamp,
                                       'last': tick.lastPrice,
                                       'volume': tick.lastSize})
        tick_data.set_index(['timestamp'], inplace=True)
        _raw_bars = self._raw_bars[symbol].copy()
        _raw_bars = _raw_bars.append(tick_data)

        # add utils.resampled raw to self._bars
        ohlc = _raw_bars['last'].resample('1T').ohlc()
        vol = _raw_bars['volume'].resample('1T').sum()

        opened_bar = ohlc
        opened_bar['volume'] = vol

        # add bar to self._bars object
        previous_bar_count = len(self._bars[symbol])
        self._bars[symbol] = self._bars[symbol].append(opened_bar)
        self._bars[symbol] = self._bars[symbol].groupby(
            self._bars[symbol].index).last()

        if len(self._bars[symbol].index) > previous_bar_count:
            logger.debug(f"__Bars__ {self._bars[symbol].to_dict(orient='records')[0]} \
            +{datetime.fromtimestamp(time.time())}")

            bar = self._bars[symbol].to_dict(orient='records')[0]
            bar["symbol"] = symbol
            bar["timestamp"] = self._bars[symbol].index[0].strftime(
                DateTimeFormatEnum.DATE_TIME_FORMAT_LONG.value)
            bar["tickerId"] = tick.tickerId

            ohlcResult = Bar.createFromDict(**bar)
            if ohlcResult.isFailure:
                logger.error("Blotter::_on_tick_handler: Error creating OHLC from raw bar", ohlcResult.error)
            self._call_bar_handlers(instrument, ohlcResult.getValue())

            self._bars[symbol] = self._bars[symbol][-1:]
            _raw_bars.drop(_raw_bars.index[:], inplace=True)
        self._raw_bars[symbol] = _raw_bars

    def __init__(self, streamProvider: StreamProvider, barTimeZone: str):
        self.barTimeZone = barTimeZone
        self.streamProvider = streamProvider

        self.tickCallbacks: List[Callable[[Instrument, Tick], None]] = list()
        self.barCallbacks: List[Callable[[Instrument, Bar], None]] = list()

        self._bars = pd.DataFrame(
            columns=['open', 'high', 'low', 'close', 'volume'])
        self._bars.index.names = ['datetime']
        self._bars.index = pd.to_datetime(self._bars.index, utc=True)
        # self._bars.index = self._bars.index.tz_convert(settings['timezone'])
        self._bars = {"~": self._bars}

        self._raw_bars = pd.DataFrame(columns=['last', 'volume'])
        self._raw_bars.index.names = ['datetime']
        self._raw_bars.index = pd.to_datetime(self._raw_bars.index, utc=True)
        self._raw_bars = {"~": self._raw_bars}

        self.streamProvider.addTickHandler(self._on_tick_handler)

if __name__ == '__main__':
    Appconfig.LOGLEVEL = 'debug'
    streamprovider = WebullClient()
    blotter = Blotter(streamProvider=streamprovider, barTimeZone='Asia/kolkata')
    sym = Instrument('AAPL')
    blotter.stream()
    blotter.subscribe(instrument=sym, tick=True)



