from typing import Callable, List
from dateutil.parser import parse as parse_date
from ats.bin.Webull import Webull, Contract
from ats.modules.core.aggregates.Bar import Bar
from ats.modules.core.aggregates.Quote import Quote
from ats.modules.core.aggregates.Tick import Tick, TickProps
from pandas import DataFrame

from ats.modules.core.services.streamprovider.StreamProvider import StreamProvider
from ats.shared.infra.logging.logger import logger
from ats.shared.types.timeframe import Timeframe
from ats.shared.utils import utils, asynctools
from kinetick.instrument import Instrument


class WebullClient(StreamProvider):

    def stream(self):
        logger.info("WebullClient: connecting..")
        # if self.webull.connected:
        #     self.webull.disconnect()
        #     self.webull.connect(stream=True)
        self.webull.stream()

    def subscribe(self, instrument: Instrument, orderbook: bool = None, tick: bool = None, quote: bool = None):
        if tick:
            tickerId = self.webull.tickerId(instrument)
            contract = Contract(symbol=instrument, tickerId=tickerId)
            self.webull.subscribeMarketData(contracts=contract)
        if orderbook:
            self.webull.subscribeMarketDepth(contracts=instrument)

        if instrument not in self.instruments:
            self.instruments[instrument.symbol] = instrument

    def addTickHandler(self, handler: Callable[[Instrument, Tick], None]) -> None:
        self.tickCallbacks.append(handler)

    def addBarHandler(self, handler: Callable[[Instrument, Bar], None]) -> None:
        pass

    def addQuoteHandler(self, handler: Callable[[Instrument, Quote], None]) -> None:
        pass

    def getBars(self, instrument: Instrument, interval: Timeframe, window: int) -> DataFrame:
        pass

    @asynctools.multitasking.task
    def _on_ohlc_received(self, msg, kwargs):
        symbol = self.webull.tickerSymbol(kwargs['tickerId'])

        msg['datetime'] = msg.index
        data = msg.to_dict(orient='records')

        for row in data:
            params = {"tickerId": str(kwargs['tickerId']), "symbol": symbol,
                      # "symbol_group": utils.gen_symbol_group(symbol), "asset_class": utils.gen_asset_class(
                      # symbol),
                      "timestamp": utils.datetime_to_timezone(
                          parse_date(str(row['datetime'])), tz="UTC"
                      ).strftime("%Y-%m-%d %H:%M:%S"), "open": utils.to_decimal(row['open']),
                      "high": utils.to_decimal(row['high']), "low": utils.to_decimal(row['low']),
                      "close": utils.to_decimal(row['close']), "volume": int(row['volume']),
                      "vwap": utils.to_decimal(row['vwap']), "interval": self.backfill_resolution}

            ohlcResult = Bar.createFromDict(params)

            if ohlcResult.isFailure:
                logger.error(f"WebullClient.onOhlcReceived: Failed to create OHLC. {ohlcResult.error.message}",
                             ohlcResult)

    @asynctools.multitasking.task
    def _call_tick_handlers(self, instrument: Instrument, tick: Tick):
        for callback in self.tickCallbacks:
            try:
                callback(instrument, tick)
            except Exception as e:
                logger.error(f"WebullClient::_call_tick_handler: Error executing {callback}", e)

    @asynctools.multitasking.task
    def on_tick_string_received(self, tickerId, kwargs):
        data = None
        symbol = self.webull.tickerSymbol(tickerId)

        if "tick" in kwargs:
            data = {
                "symbol": symbol,
                "timestamp": kwargs['tick']['time'],
                "lastPrice": utils.to_decimal(kwargs['tick']['last']),
                "lastSize": int(kwargs['tick']['size']),
            }

        # proceed if data exists
        if data is not None and symbol in self.instruments:
            instrument = self.instruments[symbol]
            tick = Tick(props=TickProps(symbol=symbol, tickerId=tickerId,
                                        timestamp=data['timestamp'], lastPrice=data['lastPrice'],
                                        lastSize=data['lastSize']))
            self._call_tick_handlers(instrument, tick)

    def callbacks(self, caller, msg, **kwargs):
        logger.debug(f"WebullClient.callbacks: {caller} {msg}")

        if caller == "handleConnectionClosed":
            logger.info("WebullClient.callbacks: Lost connection to Broker...")
            # TODO reconnect

        elif caller == "handleHistoricalData":
            self._on_ohlc_received(msg, kwargs)

        elif caller == "handleMarketQuote":
            pass

        elif caller == "handleTickPrice" or caller == "handleTickSize" or caller == "handleTickString":
            tickerId = msg['tickerId']
            self.on_tick_string_received(tickerId, kwargs)

        elif caller == "handleMarketDepth":
            pass

        elif caller == "handleError":
            logger.error(f'WebullClient.callbacks: Caught error. caller {caller}', {
                'msg': msg,
                **kwargs
            })

    def __init__(self):
        self.tickHandler = None
        self.barHandler = None
        self.quoteHandler = None
        self.orderBookHandler = None
        self.webull = Webull(debugMode=True)
        self.webull.callbacks = self.callbacks
        self.instruments = {}
        self.tickCallbacks: List[Callable[[Instrument, Tick], None]] = list()
        self.webull.connect(stream=True)


if __name__ == '__main__':
    wb = WebullClient()
    wb.subscribe(Instrument("HDFC"), tick=True)
    wb.stream()
