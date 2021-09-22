#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Modified version from QTPyLib: Quantitative Trading Python Library
# https://github.com/ranaroussi/qtpylib
# Copyright 2016-2018 Ran Aroussi
#
# Modified by vin8tech
# Copyright 2019-2021 vin8tech, vinay patil
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import argparse
import inspect
import sys
import logging
import os

from datetime import datetime
from abc import ABCMeta, abstractmethod

import warnings

import pandas as pd
from kinetick.models import Position
from numpy import nan

from kinetick.utils import utils, asynctools
from kinetick.broker import Broker
from kinetick.enums import Timeframes, PositionType
from kinetick.risk_assessor import RiskAssessor
from kinetick.utils.utils import create_logger
from kinetick.workflow import validate_columns as validate_csv_columns
from kinetick.blotter import prepare_history

# =============================================
warnings.simplefilter(action='ignore', category=FutureWarning)

# check min, python version
if sys.version_info < (3, 4):
    raise SystemError("Kinetick requires Python version >= 3.4")

# =============================================
# Configure logging
create_logger(__name__, level=os.getenv('LOGLEVEL') or logging.INFO)

# =============================================
# set up threading pool
__threads__ = utils.read_single_argv("--threads")
__threads__ = int(__threads__) if utils.is_number(__threads__) else None
asynctools.multitasking.createPool(__name__, __threads__)


# =============================================


class Algo(Broker):
    """Algo class initializer (sub-class of Broker)

    :Parameters:

        instruments : list
            List of contract tuples. Default is empty list
        resolution : str
            Desired bar resolution (using pandas resolution: 1T, 1H, etc).
            Use K for tick bars. Default is 1T (1min)
        tick_window : int
            Length of tick lookback window to keep. Defaults to 1
        bar_window : int
            Length of bar lookback window to keep. Defaults to 100
        timezone : str
            Convert broker timestamps to this timezone.
            Defaults to UTC
        preload : str
            Preload history when starting algo (Pandas resolution: 1H, 1D, etc)
            Use K for tick bars.
        continuous : bool
            Tells preloader to construct continuous Futures contracts
            (default is True)
        blotter : str
            Log trades to this Blotter's Datastore (default is "auto detect")
        log: str
            Path to store trade data (default: None)
        backtest: bool
            Whether to operate in Backtest mode (default: False)
        start: str
            Backtest start date (YYYY-MM-DD [HH:MM:SS[.MS]). Default is None
        end: str
            Backtest end date (YYYY-MM-DD [HH:MM:SS[.MS]). Default is None
        data : str
            Path to the directory with Kinetick-compatible CSV files (Backtest)
        output: str
            Path to save the recorded data (default: None)
        risk_assessor: RiskAssessor
            All instances of Algo will use same global risk assessor if provided.
        name: String
            Strategy name. Please provide unique identifier to your algo if running multiple algos.
    """

    __metaclass__ = ABCMeta

    def __init__(self, instruments, risk_assessor: RiskAssessor = None, resolution="1m",
                 tick_window=1, bar_window=100, timezone="UTC", preload=None,
                 continuous=True, blotter=None, sms=None, log=None,
                 backtest=False, start=None, end=None, data=None, output=None,
                 backfill=False, name=None, preload_positions=None, **kwargs):

        # detect algo name
        self.name = name or str(self.__class__).split('.')[-1].split("'")[0]

        # initialize algo logger
        self.log_algo = logging.getLogger(__name__)

        # initialize strategy logger
        utils.create_logger(self.name, level=logging.INFO)
        self.log = logging.getLogger(self.name)

        # override args with any (non-default) command-line args
        self.args = {arg: val for arg, val in locals().items(
        ) if arg not in ('__class__', 'self', 'kwargs')}
        self.args.update(kwargs)
        self.args.update(self.load_cli_args())

        # -----------------------------------
        # assign algo params
        self.bars = pd.DataFrame(columns=["symbol", "symbol_group"])
        self.ticks = pd.DataFrame(columns=["symbol", "symbol_group"])
        self.quotes = {}
        self.books = {}
        self.tick_count = 0
        self.tick_bar_count = 0
        self.bar_count = 0
        self.bar_hashes = {}

        self.tick_window = tick_window if tick_window > 0 else 1
        if "V" in resolution:
            self.tick_window = 1000
        self.bar_window = bar_window if bar_window > 0 else 100
        self.resolution = Timeframes.timeframe_to_resolution(resolution)
        self.timezone = timezone
        self.preload = preload
        self.preload_positions = self.args['preload_positions']
        self.backfill = self.args["backfill"]
        self.continuous = continuous

        # -----------------------------------
        # backtest info
        self.backtest = self.args["backtest"]
        self.backtest_start = self.args["start"]
        self.backtest_end = self.args["end"]
        self.backtest_csv = self.args["data"]

        # -----------------------------------
        self.sms_numbers = self.args["sms"]
        self.trade_log_dir = self.args["log"]
        self.blotter_name = self.args["blotter"]
        self.record_output = self.args["output"]

        self.risk_assessor = risk_assessor if risk_assessor is not None else RiskAssessor(**self.args)

        # ---------------------------------------
        # sanity checks for backtesting mode
        if self.backtest:
            if self.record_output is None:
                self.log_algo.error(
                    "Must provide an output file for Backtest mode")
                sys.exit(0)
            if self.backtest_start is None:
                self.log_algo.error(
                    "Must provide start date for Backtest mode")
                sys.exit(0)
            if self.backtest_end is None:
                self.backtest_end = datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S.%f')
            if self.backtest_csv is not None:
                if not os.path.exists(self.backtest_csv):
                    self.log_algo.error(
                        "CSV directory cannot be found (%s)",
                        self.backtest_csv)
                    sys.exit(0)
                elif self.backtest_csv.endswith("/"):
                    self.backtest_csv = self.backtest_csv[:-1]

        else:
            self.backtest_start = None
            self.backtest_end = None
            self.backtest_csv = None

        # -----------------------------------
        # initiate broker/order manager
        super().__init__(instruments,
                         zerodha_user=self.args['zerodha_user'],
                         zerodha_password=self.args['zerodha_password'],
                         zerodha_pin=self.args['zerodha_pin'])

        # -----------------------------------
        # signal collector
        self.signals = {}
        for sym in self.symbols:
            self.signals[sym] = pd.DataFrame()

        # -----------------------------------
        # initialize output file
        self.record_ts = None
        if self.record_output:
            self.datastore = utils.DataStore(self.args["output"])

        # ---------------------------------------
        # add stale ticks for more accurate time--based bars
        if not self.backtest and self.resolution[-1] not in ("S", "K", "V"):
            self.bar_timer = asynctools.RecurringTask(
                self.add_stale_tick, interval_sec=1, init_sec=1, daemon=True)

        # ---------------------------------------
        # be aware of thread count
        self.threads = asynctools.multitasking.getPool(__name__)['threads']

    # ---------------------------------------
    def add_stale_tick(self):
        ticks = self.ticks.copy()
        if self.ticks.empty:
            return

        last_tick_sec = float(utils.datetime64_to_datetime(
            ticks.index.values[-1]).strftime('%M.%S'))

        for sym in list(self.ticks["symbol"].unique()):
            tick = ticks[ticks['symbol'] ==
                         sym][-5:].to_dict(orient='records')[-1]
            tick['timestamp'] = datetime.utcnow()

            if last_tick_sec != float(tick['timestamp'].strftime("%M.%S")):
                tick = pd.DataFrame(index=[0], data=tick)
                tick.set_index('timestamp', inplace=True)
                tick = utils.set_timezone(tick, tz=self.timezone)
                tick.loc[:, 'lastsize'] = 0  # no real size

                self._tick_handler(tick, stale_tick=True)

    # ---------------------------------------
    def load_cli_args(self):
        """
        Parse command line arguments and return only the non-default ones

        :Retruns: dict
            a dict of any non-default args passed on the command-line.
        """
        parser = argparse.ArgumentParser(
            description='Kinetick Algo',
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument('--sms', default=self.args["sms"],
                            help='Numbers to text orders', nargs='+')
        parser.add_argument('--log', default=self.args["log"],
                            help='Path to store trade data')
        parser.add_argument('--backtest', default=self.args["backtest"],
                            help='Work in Backtest mode (flag)',
                            action='store_true')
        parser.add_argument('--backfill', default=self.args["backfill"],
                            help='backfill bar data (flag)',
                            action='store_true')
        parser.add_argument('--start', default=self.args["start"],
                            help='Backtest start date')
        parser.add_argument('--end', default=self.args["end"],
                            help='Backtest end date')
        parser.add_argument('--data', default=self.args["data"],
                            help='Path to backtester CSV files')
        parser.add_argument('--output', default=self.args["output"],
                            help='Path to save the recorded data')
        parser.add_argument('--blotter',
                            help='Log trades to this Blotter\'s Datastore')
        parser.add_argument('--continuous', default=self.args["continuous"],
                            help='Use continuous Futures contracts (flag)',
                            action='store_true')
        parser.add_argument('--zerodha_user', default=os.getenv("zerodha_user"),
                            help='Zerodha Username', required=False)
        parser.add_argument('--zerodha_password', default=os.getenv("zerodha_password"),
                            help='Zerodha Password', required=False)
        parser.add_argument('--zerodha_pin', default=os.getenv("zerodha_pin"),
                            help='Zerodha PIN', required=False)
        parser.add_argument('--resolution', default=os.getenv("resolution") or Timeframes.MINUTE_1,
                            help='Bar interval in terms of resolution (default=1m). '
                                 'ex. 1m, 3m, 5m, 15m, 30m, 1h, 2h, 3h, 4h, 6h, 8h, 1D',
                            required=False)
        parser.add_argument('--max_trades', default=os.getenv("max_trades") or 1, type=int,
                            help='Max Active Concurrent Trades (default=1). ex. 4', required=False)
        parser.add_argument('--initial_capital', default=os.getenv("initial_capital") or 10000, type=float,
                            help='Initial Capital (default=10000). ex. 1200000', required=False)
        parser.add_argument('--initial_margin', default=os.getenv("initial_margin") or 1000, type=float,
                            help='Initial Margin (default=1000). ex. 10000', required=False)
        parser.add_argument('--risk2reward', default=os.getenv("risk2reward") or 1, type=float,
                            help='Risk to Reward (default=1). ex. 1.2', required=False)
        parser.add_argument('--risk_per_trade', default=os.getenv("risk_per_trade") or 100, type=float,
                            help='Risk per Trade (default=100), ex. 100', required=False)
        parser.add_argument('--preload_positions',
                            default=os.getenv("preload_positions") or self.args["preload_positions"]
                            if 'preload_positions' in self.args else None,
                            help='Preload positions. Only available in live trade. ex. 4D, 1H', required=False)

        cmd_args, _ = parser.parse_known_args()
        args = {arg: val for arg, val in vars(
            cmd_args).items()}
        return args

    # ---------------------------------------
    def run(self):
        """Starts the algo

        Connects to the Blotter, processes market data and passes
        tick data to the ``on_tick`` function and bar data to the
        ``on_bar`` methods.
        """

        history = pd.DataFrame()

        # get history from csv dir
        if self.backtest and self.backtest_csv:
            kind = "TICK" if self.resolution[-1] in ("S", "K", "V") else "BAR"
            dfs = []
            for symbol in self.symbols:
                file = "%s/%s.%s.csv" % (self.backtest_csv, symbol, kind)
                if not os.path.exists(file):
                    self.log_algo.error(
                        "Can't load data for %s (%s doesn't exist)",
                        symbol, file)
                    sys.exit(0)
                try:
                    df = pd.read_csv(file)
                    if "expiry" not in df.columns:
                        df.loc[:, "expiry"] = nan

                    if not validate_csv_columns(df, kind, raise_errors=False):
                        self.log_algo.error(
                            "%s isn't a Kinetick-compatible format", file)
                        sys.exit(0)

                    if df['symbol'].values[-1] != symbol:
                        self.log_algo.error(
                            "%s Doesn't content data for %s", file, symbol)
                        sys.exit(0)

                    dfs.append(df)

                except Exception as e:
                    self.log_algo.error(
                        "Error reading data for %s (%s)", symbol, file)
                    sys.exit(0)

            history = prepare_history(
                data=pd.concat(dfs, sort=True),
                resolution=self.resolution,
                tz=self.timezone,
                continuous=self.continuous
            )
            history = history[history.index >= self.backtest_start]

        elif not self.blotter_args["dbskip"] and (
                self.backtest or self.preload):

            start = self.backtest_start if self.backtest else utils.backdate(
                self.preload)
            end = self.backtest_end if self.backtest else None

            history = self.blotter.history(
                symbols=self.symbols,
                start=start,
                end=end,
                resolution=Timeframes.to_timeframe(self.resolution),
                tz=self.timezone,
                continuous=self.continuous
            )

            # history needs backfilling?
            # self.blotter.backfilled = True
            if not self.blotter.backfilled and self.backfill:
                # "loan" Blotter this stream provider
                self.blotter.connection = self.broker

                # call the back fill
                self.blotter.backfill(data=history,
                                      resolution=Timeframes.to_timeframe(self.resolution),
                                      start=start, end=end)

                # re-get history from db
                history = self.blotter.history(
                    symbols=self.symbols,
                    start=start,
                    end=end,
                    resolution=Timeframes.to_timeframe(self.resolution),
                    tz=self.timezone,
                    continuous=self.continuous
                )

                # take our connection back :)
                self.blotter.connection = None

        # optimize pandas
        if not history.empty:
            history['symbol'] = history['symbol'].astype('category')
            history['symbol_group'] = history['symbol_group'].astype('category')
            history['asset_class'] = history['asset_class'].astype('category')
            history = history.loc[history['volume'] > 0]

        if self.backtest:
            # initiate strategy
            self.on_start()

            # drip history
            drip_handler = self._tick_handler if self.resolution[-1] in (
                "S", "K", "V") else self._bar_handler
            self.blotter.drip(history, drip_handler)

        else:
            if not self.blotter_args["dbskip"] and self.preload_positions:
                start = utils.backdate(self.preload_positions)
                self.load_positions(start)
            # place history self.bars
            self.bars = history

            # add instruments to blotter in case they do not exist
            self.blotter.register(self.instruments)

            # initiate strategy
            self.on_start()

            # listen for RT data
            self.blotter.stream(
                symbols=self.symbols,
                tz=self.timezone,
                quote_handler=self._quote_handler,
                tick_handler=self._tick_handler,
                bar_handler=self._bar_handler,
                book_handler=self._book_handler
            )

    # ---------------------------------------
    @abstractmethod
    def on_start(self):
        """
        Invoked once when algo starts. Used for when the strategy
        needs to initialize parameters upon starting.

        """
        # raise NotImplementedError("Should implement on_start()")
        pass

    # ---------------------------------------
    @abstractmethod
    def on_quote(self, instrument):
        """
        Invoked on every quote captured for the selected instrument.
        This is where you'll write your strategy logic for quote events.

        :Parameters:

            symbol : string
                `Instruments Object <#instrument-api>`_

        """
        # raise NotImplementedError("Should implement on_quote()")
        pass

    # ---------------------------------------
    @abstractmethod
    def on_tick(self, instrument):
        """
        Invoked on every tick captured for the selected instrument.
        This is where you'll write your strategy logic for tick events.

        :Parameters:

            symbol : string
                `Instruments Object <#instrument-api>`_

        """
        # raise NotImplementedError("Should implement on_tick()")
        pass

    # ---------------------------------------
    @abstractmethod
    def on_bar(self, instrument):
        """
        Invoked on every tick captured for the selected instrument.
        This is where you'll write your strategy logic for tick events.

        :Parameters:

            instrument : object
                `Instruments Object <#instrument-api>`_

        """
        # raise NotImplementedError("Should implement on_bar()")
        pass

    # ---------------------------------------
    @abstractmethod
    def on_orderbook(self, instrument):
        """
        Invoked on every change to the orderbook for the selected instrument.
        This is where you'll write your strategy logic for orderbook events.

        :Parameters:

            symbol : string
                `Instruments Object <#instrument-api>`_

        """
        # raise NotImplementedError("Should implement on_orderbook()")
        pass

    # ---------------------------------------
    @abstractmethod
    def on_fill(self, instrument, order):
        """
        Invoked on every order fill for the selected instrument.
        This is where you'll write your strategy logic for fill events.

        :Parameters:

            instrument : object
                `Instruments Object <#instrument-api>`_
            order : object
                Filled order data

        """
        # raise NotImplementedError("Should implement on_fill()")
        pass

    # ---------------------------------------
    def get_history(self, symbols, start, end=None, resolution="1T", tz="UTC"):
        """Get historical market data.
        Connects to Blotter and gets historical data from storage

        :Parameters:
            symbols : list
                List of symbols to fetch history for
            start : datetime / string
                History time period start date
                datetime or YYYY-MM-DD[ HH:MM[:SS]] string)

        :Optional:
            end : datetime / string
                History time period end date
                (datetime or YYYY-MM-DD[ HH:MM[:SS]] string)
            resolution : string
                History resolution (Pandas resample, defaults to 1T/1min)
            tz : string
                History timezone (defaults to UTC)

        :Returns:
            history : pd.DataFrame
                Pandas DataFrame object with historical data for all symbols
        """
        return self.blotter.history(symbols, start, end, resolution, tz)

    # ---------------------------------------
    # shortcuts to broker._create_order
    # ---------------------------------------
    def order(self, txn_type, symbol, quantity=0, **kwargs):
        """ Send an order for the selected instrument

        :Parameters:

            txn_type : string
                Order Type (BUY/SELL, EXIT/FLATTEN)
            symbol : string
                instrument symbol
            quantity : int
                Order quantiry

        :Optional:

            limit_price : float
                In case of a LIMIT order, this is the LIMIT PRICE
            expiry : int
                Cancel this order if not filled after *n* seconds
                (default 60 seconds)
            order_type : string
                Type of order: Market (default),
                LIMIT (default when limit_price is passed),
                MODIFY (required passing or orderId)
            orderId : int
                If modifying an order, the order id of the modified order
            target : float
                Target (exit) price
            initial_stop : float
                Price to set hard stop
            stop_limit: bool
                Flag to indicate if the stop should be STOP or STOP LIMIT.
                Default is ``False`` (STOP)
            trail_stop_at : float
                Price at which to start trailing the stop
            trail_stop_type : string
                Type of traiing stop offset (amount, percent).
                Default is ``percent``
            trail_stop_by : float
                Offset of trailing stop distance from current price
            fillorkill: bool
                Fill entire quantiry or none at all
            iceberg: bool
                Is this an iceberg (hidden) order
            tif: str
                Time in force (DAY, GTC, IOC, GTD). default is ``DAY``
        """
        self.log_algo.info('ORDER: %s %4d %s %s', txn_type,
                           quantity, symbol, kwargs)
        if txn_type.upper() == "EXIT" or txn_type.upper() == "FLATTEN":
            position = self.get_positions(symbol)
            if position['position'] == 0:
                return

            kwargs['symbol'] = symbol
            kwargs['quantity'] = abs(position['position'])
            kwargs['direction'] = "BUY" if position['position'] < 0 else "SELL"

            # print("EXIT", kwargs)

            try:
                self.record({symbol + '_POSITION': 0}, **kwargs)
            except Exception as e:
                pass

            if not self.backtest:
                self._create_order(**kwargs)

        else:
            if quantity == 0:
                return

            # buy/sell at best price
            limit_price = kwargs['limit_price'] if 'limit_price' in kwargs else 0
            best_price = kwargs['best_price'] if 'best_price' in kwargs else False

            if best_price is True and limit_price > 0:
                instrument = self.get_instrument(symbol)
                order_book = instrument.get_orderbook()
                running_price = float(order_book['bid'][0]) if txn_type.upper() != "BUY" else float(
                    order_book['ask'][0])
                running_size = float(order_book['bidsize'][0]) if txn_type.upper() != "BUY" else float(
                    order_book['asksize'][0])

                if running_price > 0 and running_size >= abs(quantity):
                    if txn_type.upper() != "BUY":
                        kwargs['limit_price'] = running_price if running_price > limit_price else limit_price
                    else:
                        kwargs['limit_price'] = running_price if running_price < limit_price else limit_price

            kwargs['symbol'] = symbol
            kwargs['quantity'] = abs(quantity)
            kwargs['direction'] = txn_type.upper()
            # kwargs['trigger_price'] = running_price if running_price > 0 else limit_price

            try:
                quantity = abs(quantity)
                if kwargs['direction'] != "BUY":
                    quantity = -quantity
                self.record({'POSITION': quantity}, **kwargs)
            except Exception as e:
                pass

            if not self.backtest:
                return self._create_order(**kwargs)

    # ---------------------------------------
    def cancel_order(self, order_id):
        """ Cancels a un-filled order

        Parameters:
            order_id : int
                Order ID
        """
        if not self.backtest:
            return self._cancel_order(order_id)

    # ---------------------------------------
    def record(self, *args, **kwargs):
        """Records data for later analysis.
        Values will be logged to the file specified via
        ``--output [file]`` (along with bar data) as
        csv/pickle/h5 file.

        Call from within your strategy:
        ``self.record(key=value)``

        :Parameters:
            ** kwargs : mixed
                The names and values to record

        """
        if self.record_output:
            try:
                self.datastore.record(self.record_ts, *args, **kwargs)
            except Exception as e:
                self.log_algo.error('Error Recording', e)

    # ---------------------------------------
    @staticmethod
    def _caller(caller):
        stack = [x[3] for x in inspect.stack()][1:-1]
        return caller in stack

    # ---------------------------------------
    @asynctools.multitasking.task
    def _book_handler(self, book):
        symbol = book['symbol']
        del book['symbol']
        del book['kind']

        self.books[symbol] = book
        self.on_orderbook(self.get_instrument(symbol))

    # ---------------------------------------
    @asynctools.multitasking.task
    def _quote_handler(self, quote):
        del quote['kind']
        self.quotes[quote['symbol']] = quote
        self.on_quote(self.get_instrument(quote))

    # ---------------------------------------
    @staticmethod
    def _get_window_per_symbol(df, window):
        # truncate tick window per symbol
        dfs = []
        for sym in list(df["symbol"].unique()):
            dfs.append(df[df['symbol'] == sym][-window:])
        return pd.concat(dfs, sort=True).sort_index()

    # ---------------------------------------
    @staticmethod
    def _thread_safe_merge(symbol, basedata, newdata):
        data = newdata
        if "symbol" in basedata.columns:
            data = pd.concat(
                [basedata[basedata['symbol'] != symbol], data], sort=True)

        data.loc[:, '_idx_'] = data.index
        data = data.drop_duplicates(
            subset=['_idx_', 'symbol', 'symbol_group', 'asset_class'],
            keep='last')
        data = data.drop('_idx_', axis=1)
        data = data.sort_index()

        try:
            return data.dropna(subset=[
                'open', 'high', 'low', 'close', 'volume'])
        except Exception as e:
            return data

    # ---------------------------------------
    @asynctools.multitasking.task
    def _tick_handler(self, tick, stale_tick=False):
        # self._cancel_expired_pending_orders() TODO

        # tick symbol
        symbol = tick['symbol'].values
        if len(symbol) == 0:
            return
        symbol = symbol[0]
        self.last_price[symbol] = float(tick['last'].values[0])

        # work on copy
        self_ticks = self.ticks.copy()

        # initial value
        if self.record_ts is None:
            self.record_ts = tick.index[0]

        if self.resolution[-1] not in ("S", "K", "V"):
            if self.threads == 0:
                self.ticks = self._update_window(
                    self.ticks, tick, window=self.tick_window)
            else:
                self_ticks = self._update_window(
                    self_ticks, tick, window=self.tick_window)
                self.ticks = self._thread_safe_merge(
                    symbol, self.ticks, self_ticks)  # assign back
        else:
            self.ticks = self._update_window(self.ticks, tick)
            # bars = utils.resample(self.ticks, self.resolution)
            bars = utils.resample(
                self.ticks, self.resolution, tz=self.timezone)

            if len(bars.index) > self.tick_bar_count > 0 or stale_tick:
                self.record_ts = tick.index[0]
                self._base_bar_handler(bars[bars['symbol'] == symbol][-1:])

                window = int(
                    "".join([s for s in self.resolution if s.isdigit()]))
                if self.threads == 0:
                    self.ticks = self._get_window_per_symbol(
                        self.ticks, window)
                else:
                    self_ticks = self._get_window_per_symbol(
                        self_ticks, window)
                    self.ticks = self._thread_safe_merge(
                        symbol, self.ticks, self_ticks)  # assign back

            self.tick_bar_count = len(bars.index)

            # record non time-based bars
            self.record(bars[-1:])

        if not stale_tick:
            if self.ticks[(self.ticks['symbol'] == symbol) | (
                    self.ticks['symbol_group'] == symbol)].empty:
                return
            tick_instrument = self.get_instrument(tick)
            if tick_instrument:
                self.on_tick(tick_instrument)

    # ---------------------------------------
    def _base_bar_handler(self, bar):
        """ non threaded bar handler (called by threaded _tick_handler) """
        # bar symbol
        symbol = bar['symbol'].values
        if len(symbol) == 0:
            return
        symbol = symbol[0]
        self_bars = self.bars.copy()  # work on copy

        is_tick_or_volume_bar = False
        handle_bar = True

        if self.resolution[-1] in ("S", "K", "V"):
            is_tick_or_volume_bar = True
            handle_bar = self._caller("_tick_handler")

        # drip is also ok
        handle_bar = handle_bar or self._caller("drip")

        if is_tick_or_volume_bar:
            # just add a bar (used by tick bar bandler)
            if self.threads == 0:
                self.bars = self._update_window(self.bars, bar,
                                                window=self.bar_window)
            else:
                self_bars = self._update_window(self_bars, bar,
                                                window=self.bar_window)
        else:
            # add the bar and resample to resolution
            if self.threads == 0:
                self.bars = self._update_window(self.bars, bar,
                                                window=self.bar_window,
                                                resolution=self.resolution)
            else:
                self_bars = self._update_window(self_bars, bar,
                                                window=self.bar_window,
                                                resolution=self.resolution)

        # assign new data to self.bars if threaded
        if self.threads > 0:
            self.bars = self._thread_safe_merge(symbol, self.bars, self_bars)

        # optimize pandas
        if len(self.bars) == 1:
            self.bars['symbol'] = self.bars['symbol'].astype('category')
            self.bars['symbol_group'] = self.bars['symbol_group'].astype('category')
            self.bars['asset_class'] = self.bars['asset_class'].astype('category')

        # new bar?
        hash_string = bar[:1]['symbol'].to_string().translate(
            str.maketrans({key: None for key in "\n -:+"}))
        this_bar_hash = abs(hash(hash_string)) % (10 ** 8)

        newbar = True
        if symbol in self.bar_hashes.keys():
            newbar = self.bar_hashes[symbol] != this_bar_hash
        self.bar_hashes[symbol] = this_bar_hash

        if newbar and handle_bar:
            if self.bars[(self.bars['symbol'] == symbol) | (
                    self.bars['symbol_group'] == symbol)].empty:
                return
            bar_instrument = self.get_instrument(symbol)
            if bar_instrument:
                self.record_ts = bar.index[0]
                self.on_bar(bar_instrument)
                # if self.resolution[-1] not in ("S", "K", "V"):
                # self.record(bar)

    # ---------------------------------------
    @asynctools.multitasking.task
    def _bar_handler(self, bar):
        """ threaded version of _base_bar_handler (called by blotter's) """
        self._base_bar_handler(bar)

    # ---------------------------------------
    def _update_window(self, df, data, window=None, resolution=None):
        if df is None:
            df = data
        else:
            df = df.append(data, sort=True)
            df.loc[:, '_idx_'] = df.index
            df.drop_duplicates(
                subset=['_idx_', 'symbol', 'symbol_group', 'asset_class'],
                keep='last', inplace=True)
            df.drop('_idx_', axis=1, inplace=True)

        # resample
        if resolution:
            tz = str(df.index.tz)
            # try:
            #     tz = str(df.index.tz)
            # except Exception as e:
            #     tz = None
            df = utils.resample(df, resolution=resolution, tz=tz, sync_last_timestamp=False)

        else:
            # remove duplicates rows
            # (handled by resample if resolution is provided)
            df.loc[:, '_idx_'] = df.index
            df.drop_duplicates(
                subset=['_idx_', 'symbol', 'symbol_group', 'asset_class'],
                keep='last', inplace=True)
            df.drop('_idx_', axis=1, inplace=True)

        # return
        if window is None:
            return df

        # return df[-window:]
        return self._get_window_per_symbol(df, window)

    # ---------------------------------------
    # signal logging methods
    # ---------------------------------------
    def _add_signal_history(self, df, symbol):
        """ Initilize signal history """
        if symbol not in self.signals.keys() or len(self.signals[symbol]) == 0:
            self.signals[symbol] = [nan] * len(df.index)
        else:
            self.signals[symbol].append(nan)

        self.signals[symbol] = self.signals[symbol][-len(df.index):]
        signal_count = len(self.signals[symbol])
        df.loc[-signal_count:, 'signal'] = self.signals[symbol][-signal_count:]

        return df

    def _log_signal(self, symbol, signal):
        """ Log signal

        :Parameters:
            symbol : string
                instruments symbol
            signal : integer
                signal identifier (1, 0, -1)

        """
        self.signals[symbol][-1] = signal

    def sync_bars(self, symbol, lookback=800):
        """
        sync the bars with server
        :param symbol:
        :param lookback:
        :return:
        """
        bars = self.broker.get_bars(
            tickerId=self.broker.tickerId(symbol),
            lookback=lookback,
            interval='m' + str(Timeframes.timeframe_to_minutes(Timeframes.to_timeframe(self.resolution)))
        )
        bars['symbol'] = symbol
        bars["symbol_group"] = utils.gen_symbol_group(symbol)
        bars["asset_class"] = utils.gen_asset_class(symbol)

        bars["datetime"] = bars.index

        bars.set_index('datetime', inplace=True)
        bars.index = pd.to_datetime(bars.index, utc=True)

        try:
            bars.index = bars.index.tz_convert(tz=self.timezone)
        except Exception as e:
            bars.index = bars.index.tz_localize('UTC').tz_convert(tz=self.timezone)

        # add options columns
        df = utils.force_options_columns(bars)
        self._bar_handler(df)
        return bars

    def load_positions(self, start):
        for pos in Position.find(algo=self.name, _active=True, datetime__gt=start):
            if not PositionType.is_overnight_position(pos.variety):
                continue
            instrument = self.get_instrument(pos.symbol)
            instrument.set_position(pos)
            self.add_instruments([instrument])
            try:
                self.risk_assessor.enter_position(pos)
            except Exception as e:
                self.log_algo.warning(
                    "Preloaded %s position exceed current risk parameters. %s", pos.symbol, e)
