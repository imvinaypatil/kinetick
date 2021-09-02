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
import atexit
import json
import logging
import os
import pickle

import sys
import tempfile
import time
import glob
import subprocess

from datetime import datetime
from abc import ABCMeta
from math import ceil

import zmq
import pandas as pd
from dateutil.parser import parse as parse_date

from numpy import (
    isnan as np_isnan,
    nan as np_nan,
    int64 as np_int64
)

from kinetick import (
    path,
)
from kinetick.utils import utils, asynctools
from mongoengine import connect as mongo_connect, NotUniqueError, Q
from kinetick.enums import Timeframes, COMMON_TYPES
from kinetick.lib.brokers import Webull
from kinetick.models import Tick, OHLC
from kinetick.utils.utils import (
    read_single_argv,
    is_number,
    create_logger, force_options_columns, datetime64_to_datetime, wb_lookback_str, chmod, resample,
    create_continuous_contract, gen_symbol_group, gen_asset_class
)

# =============================================
# Configure logging
create_logger(__name__, os.getenv('LOGLEVEL') or logging.INFO)
logging.getLogger('zmq').setLevel(os.getenv('LOGLEVEL') or logging.INFO)

# =============================================
# set up threading pool
__threads__ = read_single_argv("--threads")
__threads__ = __threads__ if is_number(__threads__) else None
asynctools.multitasking.createPool(__name__, __threads__)

# =============================================

cash_ticks = {}


class Blotter():
    """Broker class initilizer

    :Optional:

        name : string
            name of the blotter (used by other modules)
        symbols : str
            contracts CSV database (default: ./symbols.csv)
        zmqport : str
            ZeroMQ Port to use (default: 12345)
        zmqtopic : str
            ZeroMQ string to use (default: _kinetick_BLOTTERNAME_)
        orderbook : str
            Get Order Book (Market Depth) data (default: False)
        dbhost : str
            MySQL server hostname (default: localhost)
        dbport : str
            MySQL server port (default: 3306)
        dbname : str
            MySQL server database (default: kinetick)
        dbuser : str
            MySQL server username (default: root)
        dbpass : str
            MySQL server password (default: none)
        dbskip : str
            Skip MySQL logging (default: True)
    """

    __metaclass__ = ABCMeta

    def __init__(self, name=None, symbols="symbols.csv",
                 dbhost="localhost", dbport="27017", dbname="kinetick",
                 dbuser="root", dbpass="", dbskip=True, orderbook=False,
                 zmqport="12345", zmqtopic=None, **kwargs):

        # whats my name?
        self.name = str(self.__class__).split('.')[-1].split("'")[0].lower()
        if name is not None:
            self.name = name

        # initialize class logger
        self.log_blotter = logging.getLogger(__name__)

        # do not act on first tick (timezone is incorrect)
        self.first_tick = True

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

        # global objects
        self.db_connection = None
        self.context = None
        self.socket = None
        self.connection = None  # stream provider

        self.symbol_ids = {}  # cache
        self.cash_ticks = cash_ticks  # outside cache
        self.rtvolume = set()  # has RTVOLUME?

        # override args with any (non-default) command-line args
        self.args = {arg: val for arg, val in locals().items(
        ) if arg not in ('__class__', 'self', 'kwargs')}
        self.args.update(kwargs)
        self.args.update(self.load_cli_args())

        # -------------------------------
        # work default values
        # -------------------------------
        if zmqtopic is None:
            zmqtopic = "_kinetick_" + str(self.name.lower()) + "_"
            if self.args['zmqtopic'] is None:
                self.args['zmqtopic'] = zmqtopic

        # if no path given for symbols' csv, use same dir
        if symbols == "symbols.csv":
            symbols = path['caller'] + '/' + symbols
        # -------------------------------

        # read cached args to detect duplicate blotters
        self.duplicate_run = False
        self.cached_args = {}
        self.args_cache_file = "%s/%s.kinetick" % (
            tempfile.gettempdir(), self.name)
        if os.path.exists(self.args_cache_file):
            self.cached_args = self._read_cached_args()

        # don't display connection errors on ctrl+c
        self.quitting = False

        # do stuff on exit
        atexit.register(self._on_exit)

        # track historical data download status
        self.backfilled = False
        self.backfilled_symbols = []
        self.backfill_resolution = Timeframes.MINUTE_1  # default to 1 min

        # be aware of thread count
        self.threads = asynctools.multitasking.getPool(__name__)['threads']

    # -------------------------------------------
    def _on_exit(self, terminate=True):
        if "as_client" in self.args:
            return

        self.log_blotter.info("Blotter stopped...")

        if hasattr(self, 'wb'):
            self.log_blotter.info("Cancel market data...")
            self.connection.cancelMarketDataSubscription()

            self.log_blotter.info("Disconnecting...")
            self.connection.disconnect()

        if not self.duplicate_run:
            self.log_blotter.info("Deleting runtime args...")
            self._remove_cached_args()

        if not self.args['dbskip']:
            self.log_blotter.info("Disconnecting from DB...")
            try:
                self.db_connection.close()
            except Exception as e:
                pass

        if terminate:
            sys.exit(0)

    # -------------------------------------------
    @staticmethod
    def _detect_running_blotter(name):
        return name

    # -------------------------------------------
    @staticmethod
    def _blotter_file_running():
        try:
            # not sure how this works on windows...
            command = 'pgrep -f ' + sys.argv[0]
            process = subprocess.Popen(
                command, shell=True, stdout=subprocess.PIPE)
            stdout_list = process.communicate()[0].decode('utf-8').split("\n")
            stdout_list = list(filter(None, stdout_list))
            return len(stdout_list) > 0
        except Exception as e:
            return False

    # -------------------------------------------
    def _check_unique_blotter(self):
        if os.path.exists(self.args_cache_file):
            # temp file found - check if really running
            # or if this file wasn't deleted due to crash
            if not self._blotter_file_running():
                # print("REMOVING OLD TEMP")
                self._remove_cached_args()
            else:
                self.duplicate_run = True
                self.log_blotter.error("Blotter is already running...")
                # sys.exit(1)

        self._write_cached_args()

    # -------------------------------------------
    def _remove_cached_args(self):
        if os.path.exists(self.args_cache_file):
            os.remove(self.args_cache_file)

    def _read_cached_args(self):
        if os.path.exists(self.args_cache_file):
            return pickle.load(open(self.args_cache_file, "rb"))
        return {}

    def _write_cached_args(self):
        pickle.dump(self.args, open(self.args_cache_file, "wb"))
        utils.chmod(self.args_cache_file)

    # -------------------------------------------
    def load_cli_args(self):
        def to_boolean(s):
            if s:
                return s.lower() in ['true', '1', 't', 'y', 'yes']
            else:
                return None

        parser = argparse.ArgumentParser(
            description='Kinetick Blotter',
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)

        parser.add_argument('--symbols', default=os.getenv("symbols") or self.args['symbols'],
                            help='contracts CSV database', required=False)
        parser.add_argument('--zmqport', default=os.getenv("zmqport") or self.args['zmqport'],
                            help='ZeroMQ Port to use', required=False)
        parser.add_argument('--zmqtopic', default=os.getenv("zmqtopic") or self.args['zmqtopic'],
                            help='ZeroMQ Port to use', required=False)
        parser.add_argument('--orderbook', action='store_true',
                            default=to_boolean(os.getenv("orderbook")) or self.args['orderbook'],
                            help='Get Order Book (Market Depth) data',
                            required=False)
        parser.add_argument('--dbhost', default=os.getenv("dbhost") or self.args['dbhost'],
                            help='MySQL server hostname', required=False)
        parser.add_argument('--dbport', default=os.getenv("dbport") or self.args['dbport'],
                            help='MySQL server port', required=False)
        parser.add_argument('--dbname', default=os.getenv("dbname") or self.args['dbname'],
                            help='MySQL server database', required=False)
        parser.add_argument('--dbuser', default=os.getenv("dbuser") or self.args['dbuser'],
                            help='MySQL server username', required=False)
        parser.add_argument('--dbpass', default=os.getenv("dbpass") or self.args['dbpass'],
                            help='MySQL server password', required=False)
        dbskip = to_boolean(os.getenv("dbskip")) if os.getenv("dbskip") is not None else self.args['dbskip']
        parser.add_argument('--dbskip', default=dbskip,
                            required=False, help='Skip DB logging (flag)',
                            action='store_true')

        # only return non-default cmd line args
        # (meaning only those actually given)
        cmd_args, _ = parser.parse_known_args()
        args = {arg: val for arg, val in vars(
            cmd_args).items()}
        return args

    # -------------------------------------------
    def callbacks(self, caller, msg, **kwargs):
        # self.log_blotter.debug("caller: [%s]", caller)
        # self.log_blotter.debug("Message Received: %s", msg)

        if caller == "handleConnectionClosed":
            self.log_blotter.info("Lost connection to Broker...")
            # let docker handle the restarts
            self._on_exit(terminate=True)
            # self.run()

        elif caller == "handleHistoricalData":
            self.on_ohlc_received(msg, kwargs)

        # elif caller == "handleTickString":
        #     self.on_tick_string_received(msg['tickerId'], kwargs)

        elif caller == "handleMarketQuote":
            self.on_quote_received(kwargs['tickerId'])

        elif caller == "handleTickPrice" or caller == "handleTickSize" or caller == "handleTickString":
            tickerId = msg['tickerId']
            self.on_tick_string_received(tickerId, kwargs)

        elif caller in "handleTickOptionComputation":
            self.on_option_computation_received(msg['tickerId'])

        elif caller == "handleMarketDepth":
            self.on_orderbook_received(msg['tickerId'])

        elif caller == "handleError":
            # don't display connection errors on ctrl+c
            if self.quitting:
                return

    # -------------------------------------------
    def on_ohlc_received(self, msg, kwargs):
        symbol = self.connection.tickerSymbol(kwargs['tickerId'])

        # print(msg)
        # msg.index.names = ['datetime']
        msg['datetime'] = msg.index
        data = msg.to_dict(orient='records')

        for row in data:
            params = {"tickerId": str(kwargs['tickerId']), "symbol": symbol,
                      # "symbol_group": utils.gen_symbol_group(symbol), "asset_class": utils.gen_asset_class(
                      # symbol),
                      "datetime": utils.datetime_to_timezone(
                          parse_date(str(row['datetime'])), tz="UTC"
                      ).strftime("%Y-%m-%d %H:%M:%S"), "open": utils.to_decimal(row['open']),
                      "high": utils.to_decimal(row['high']), "low": utils.to_decimal(row['low']),
                      "close": utils.to_decimal(row['close']), "volume": int(row['volume']),
                      "vwap": utils.to_decimal(row['vwap']), "resolution": self.backfill_resolution}

            ohlc = OHLC(**params)
            # store in db
            try:
                self.log2db(data=ohlc)
            except NotUniqueError:
                pass
                # self.log_blotter.info(f'duplicate row skipped. {row}')

        if kwargs["completed"]:
            self.backfilled_symbols.append(symbol)
            tickers = set(
                {v: k for k, v in self.connection.tickerIds.items() if v.upper() != "SYMBOL"}.keys())
            if tickers == set(self.backfilled_symbols):
                self.backfilled = True
        print(".")

    # -------------------------------------------
    @asynctools.multitasking.task
    def on_tick_string_received(self, tickerId, kwargs):

        # kwargs is empty
        # if not kwargs:
        #     return

        data = None
        symbol = self.connection.tickerSymbol(tickerId)

        # for instruments that receive RTVOLUME events
        if "tick" in kwargs:
            self.rtvolume.add(symbol)
            data = {
                # available data from ib
                "symbol": symbol,
                "symbol_group": utils.gen_symbol_group(symbol),  # ES_F, ...
                "asset_class": utils.gen_asset_class(symbol),
                "timestamp": kwargs['tick']['time'],
                "last": utils.to_decimal(kwargs['tick']['last']),
                "lastsize": int(kwargs['tick']['size']),
                "bid": utils.to_decimal(kwargs['tick']['bid']),
                "ask": utils.to_decimal(kwargs['tick']['ask']),
                "bidsize": int(kwargs['tick']['bidsize']),
                "asksize": int(kwargs['tick']['asksize']),
                # "wap":          kwargs['tick']['wap'],
            }

        # for instruments that DOESN'T receive RTVOLUME events (exclude options)
        elif symbol not in self.rtvolume and \
                self.connection.contracts[tickerId].sec_type not in ("OPT", "FOP"):

            tick = self.connection.marketData[tickerId]
            # print(tick)

            if not tick.empty and tick['last'].values[-1] > 0 < tick['lastsize'].values[-1]:
                data = {
                    # available data from ib
                    "symbol": symbol,
                    "tickerId": str(tickerId),
                    # ES_F, ...
                    "symbol_group": utils.gen_symbol_group(symbol),
                    "asset_class": utils.gen_asset_class(symbol),
                    "timestamp": tick.index.values[-1],
                    "datetime": tick.index.values[-1],
                    "last": utils.to_decimal(tick['last'].values[-1]),
                    "lastsize": int(tick['lastsize'].values[-1]),
                    "buy": utils.to_decimal(tick['buy'].values[-1]),
                    "sell": utils.to_decimal(tick['sell'].values[-1]),
                    "buysize": int(tick['buysize'].values[-1]),
                    "sellsize": int(tick['sellsize'].values[-1])
                    # "wap":          kwargs['tick']['wap'],
                }

        # proceed if data exists
        if data is not None:
            # cache last tick
            if symbol in self.cash_ticks.keys():
                if data == self.cash_ticks[symbol]:
                    return

            self.cash_ticks[symbol] = data

            # add options fields
            # data = utils.force_options_columns(data)

            # print('.', end="", flush=True)
            self.on_tick_received(data)

    # -------------------------------------------
    @asynctools.multitasking.task
    def on_quote_received(self, tickerId):
        try:
            symbol = self.connection.tickerSymbol(tickerId)

            if self.connection.contracts[tickerId].sec_type in ("OPT", "FOP"):
                quote = self.connection.optionsData[tickerId].to_dict(orient='records')[
                    0]
                quote['type'] = self.connection.contracts[tickerId].right
                quote['strike'] = utils.to_decimal(
                    self.connection.contracts[tickerId].strike)
                quote["symbol_group"] = self.connection.contracts[tickerId].symbol + \
                                        '_' + self.connection.contracts[tickerId].sec_type
                quote = utils.mark_options_values(quote)
            else:
                quote = self.connection.marketQuoteData[tickerId].to_dict(orient='records')[0]
                quote["symbol_group"] = utils.gen_symbol_group(symbol)

            quote["symbol"] = symbol
            quote["tickerId"] = str(tickerId)
            quote["asset_class"] = utils.gen_asset_class(symbol)
            quote['bid'] = utils.to_decimal(quote['bid'])
            quote['ask'] = utils.to_decimal(quote['ask'])
            # quote['last'] = utils.to_decimal(quote['last'])
            quote["kind"] = "QUOTE"

            # cash markets do not get RTVOLUME (handleTickString)
            if quote["asset_class"] == "CSH":
                quote['last'] = round(
                    float((quote['bid'] + quote['ask']) / 2), 5)
                quote['timestamp'] = datetime.utcnow(
                ).strftime("%Y-%m-%d %H:%M:%S.%f")
                quote['datetime'] = quote['timestamp']
                # create synthetic tick
                if symbol in self.cash_ticks.keys() and quote['last'] != self.cash_ticks[symbol]:
                    self.on_tick_received(quote)
                else:
                    self.broadcast(quote, "QUOTE")

                self.cash_ticks[symbol] = quote['last']
            else:
                quoteStore = Tick(**quote)
                # print(quoteStore.to_json())
                self.log2db(quoteStore)
                self.broadcast(quote, "QUOTE")

        except Exception as e:
            pass

    # -------------------------------------------
    @asynctools.multitasking.task
    def on_option_computation_received(self, tickerId):
        # try:
        symbol = self.connection.tickerSymbol(tickerId)

        tick = self.connection.optionsData[tickerId].to_dict(orient='records')[0]

        # must have values!
        for key in ('bid', 'ask', 'last', 'bidsize', 'asksize', 'lastsize',
                    'volume', 'delta', 'gamma', 'vega', 'theta'):
            if tick[key] == 0:
                return

        tick['type'] = self.connection.contracts[tickerId].right
        tick['strike'] = utils.to_decimal(
            self.connection.contracts[tickerId].strike)
        tick["symbol_group"] = self.connection.contracts[tickerId].symbol + \
                               '_' + self.connection.contracts[tickerId].sec_type
        tick['volume'] = int(tick['volume'])
        tick['bid'] = utils.to_decimal(tick['bid'])
        tick['bidsize'] = int(tick['bidsize'])
        tick['ask'] = utils.to_decimal(tick['ask'])
        tick['asksize'] = int(tick['asksize'])
        tick['last'] = utils.to_decimal(tick['last'])
        tick['lastsize'] = int(tick['lastsize'])

        tick['price'] = utils.to_decimal(tick['price'], 2)
        tick['underlying'] = utils.to_decimal(tick['underlying'])
        tick['dividend'] = utils.to_decimal(tick['dividend'])
        tick['volume'] = int(tick['volume'])
        tick['iv'] = utils.to_decimal(tick['iv'])
        tick['oi'] = int(tick['oi'])
        tick['delta'] = utils.to_decimal(tick['delta'])
        tick['gamma'] = utils.to_decimal(tick['gamma'])
        tick['vega'] = utils.to_decimal(tick['vega'])
        tick['theta'] = utils.to_decimal(tick['theta'])

        tick["symbol"] = symbol
        tick["symbol_group"] = utils.gen_symbol_group(symbol)
        tick["asset_class"] = utils.gen_asset_class(symbol)

        tick = utils.mark_options_values(tick)

        # is this a really new tick?
        prev_last = 0
        prev_lastsize = 0
        if symbol in self.cash_ticks.keys():
            prev_last = self.cash_ticks[symbol]['last']
            prev_lastsize = self.cash_ticks[symbol]['lastsize']
            if tick == self.cash_ticks[symbol]:
                return

        self.cash_ticks[symbol] = dict(tick)

        # assign timestamp
        tick['timestamp'] = self.connection.optionsData[tickerId].index[0]
        if tick['timestamp'] == 0:
            tick['timestamp'] = datetime.utcnow().strftime(
                COMMON_TYPES['DATE_TIME_FORMAT_LONG_MILLISECS'])

        # treat as tick if last/volume changed
        if tick['last'] != prev_last or tick['lastsize'] != prev_lastsize:
            tick["kind"] = "TICK"
            self.on_tick_received(tick)

        # otherwise treat as quote
        else:
            tick["kind"] = "QUOTE"
            self.broadcast(tick, "QUOTE")

        # except Exception as e:
        # pass

    # -------------------------------------------
    @asynctools.multitasking.task
    def on_orderbook_received(self, tickerId):
        orderbook = self.connection.marketDepthData[tickerId].dropna(
            subset=['bid', 'ask']).fillna(0).to_dict(orient='list')

        # add symbol data to list
        symbol = self.connection.tickerSymbol(tickerId)
        orderbook['symbol'] = symbol
        orderbook["symbol_group"] = gen_symbol_group(symbol)
        orderbook["asset_class"] = gen_asset_class(symbol)
        orderbook["kind"] = "ORDERBOOK"

        quoteStore = Tick(tickerId=str(tickerId), symbol=symbol, kind="QUOTE", bid=float(orderbook['bid'][0]),
                          bidsize=int(orderbook['bidsize'][0]), ask=float(orderbook['ask'][0]),
                          asksize=int(orderbook['asksize'][0]))
        # print(quoteStore.to_json())
        self.log2db(quoteStore)
        # broadcast
        self.broadcast(orderbook, "ORDERBOOK")

    # -------------------------------------------
    @asynctools.multitasking.task
    def on_tick_received(self, tick):
        # data
        symbol = tick['symbol']
        timestamp = datetime.strptime(tick['timestamp'],
                                      COMMON_TYPES["DATE_TIME_FORMAT_LONG_MILLISECS"])

        # do not act on first tick (timezone is incorrect)
        if self.first_tick:
            self.first_tick = False
            return

        try:
            timestamp = parse_date(timestamp)
        except Exception as e:
            pass

        # placeholders
        if symbol not in self._raw_bars:
            self._raw_bars[symbol] = self._raw_bars['~']

        if symbol not in self._bars:
            self._bars[symbol] = self._bars['~']

        # send tick to message self.broadcast
        tick["kind"] = "TICK"
        self.broadcast(tick, "TICK")
        tickStore = Tick(**tick)
        self.log2db(tickStore)

        # add tick to raw self._bars
        tick_data = pd.DataFrame(index=['timestamp'],
                                 data={'timestamp': timestamp,
                                       'last': tick['last'],
                                       'volume': tick['lastsize']})
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
            self.log_blotter.debug(f"__Bars__ {self._bars[symbol].to_dict(orient='records')[0]} \
            +{datetime.fromtimestamp(time.time())}")
            bar = self._bars[symbol].to_dict(orient='records')[0]
            bar["symbol"] = symbol
            bar["symbol_group"] = tick['symbol_group']
            bar["asset_class"] = tick['asset_class']
            bar["timestamp"] = self._bars[symbol].index[0].strftime(
                COMMON_TYPES["DATE_TIME_FORMAT_LONG"])

            bar["kind"] = "BAR"
            self.broadcast(bar, "BAR")
            barStore = OHLC(**bar, tickerId=tick['tickerId'])
            self.log2db(barStore)

            self._bars[symbol] = self._bars[symbol][-1:]
            _raw_bars.drop(_raw_bars.index[:], inplace=True)
        self._raw_bars[symbol] = _raw_bars

    # -------------------------------------------
    def broadcast(self, data, kind):
        def int64_handler(o):
            if isinstance(o, np_int64):
                try:
                    return pd.to_datetime(o, unit='ms').strftime(
                        COMMON_TYPES["DATE_TIME_FORMAT_LONG"])
                except Exception as e:
                    return int(o)
            raise TypeError

        string2send = "%s %s" % (
            self.args["zmqtopic"], json.dumps(data, default=int64_handler))

        # print(kind, string2send)
        try:
            self.socket.send_string(string2send)
        except Exception as e:
            pass

    # -------------------------------------------
    def log2db(self, data):
        try:
            if self.args['dbskip']:
                return
            if isinstance(data, Tick):
                return
            data.save()
        except Exception as e:
            self.log_blotter.error("Error inserting data into db %s", e)

    # -------------------------------------------
    def run(self):
        """Starts the blotter

        Connects to the TWS/GW, processes and logs market data,
        and broadcast it over TCP via ZeroMQ (which algo subscribe to)
        """

        self._check_unique_blotter()

        # connect to mysql
        self.db_connect()

        self.context = zmq.Context(zmq.REP)
        self.socket = self.context.socket(zmq.PUB)
        self.socket.bind("tcp://*:" + str(self.args['zmqport']))

        db_modified = 0
        contracts = []
        prev_contracts = []
        first_run = True

        self.log_blotter.info("Connecting to Broker...")

        self.connection = Webull()
        self.connection.callbacks = self.callbacks
        self.connection.connect(stream=True)

        self.log_blotter.info("Connection established...")

        try:
            while True:

                if not os.path.exists(self.args['symbols']):
                    pd.DataFrame(columns=['symbol', 'sec_type', 'exchange',
                                          'currency', 'expiry', 'strike', 'opt_type']
                                 ).to_csv(self.args['symbols'], header=True, index=False)
                    utils.chmod(self.args['symbols'])
                else:
                    time.sleep(0.1)

                    # read db properties
                    db_data = os.stat(self.args['symbols'])
                    db_size = db_data.st_size
                    db_last_modified = db_data.st_mtime

                    # empty file
                    if db_size == 0:
                        if prev_contracts:
                            self.log_blotter.info('Cancel market data...')
                            self.connection.cancelMarketDataSubscription()
                            time.sleep(0.1)
                            prev_contracts = []
                        continue

                    # modified?
                    if not first_run and db_last_modified == db_modified:
                        continue

                    # continue...
                    db_modified = db_last_modified

                    # read contructs db
                    df = pd.read_csv(self.args['symbols'], header=0)
                    if df.empty:
                        continue

                    # removed expired
                    df = df[(
                                    (df['expiry'] < 1000000) & (
                                    df['expiry'] >= int(datetime.now().strftime('%Y%m')))) | (
                                    (df['expiry'] >= 1000000) & (
                                    df['expiry'] >= int(datetime.now().strftime('%Y%m%d')))) |
                            np_isnan(df['expiry'])
                            ]

                    # fix expiry formatting (no floats)
                    df['expiry'] = df['expiry'].fillna(
                        0).astype(int).astype(str)
                    df.loc[df['expiry'] == "0", 'expiry'] = ""
                    df = df[df['sec_type'] != 'BAG']

                    df.fillna("", inplace=True)
                    df.to_csv(self.args['symbols'], header=True, index=False)
                    utils.chmod(self.args['symbols'])

                    # ignore comment
                    df = df[~df['symbol'].str.contains("#")]
                    contracts = [tuple(x) for x in df.values]

                    if first_run:
                        first_run = False

                    else:
                        if contracts != prev_contracts:
                            # cancel market data for removed contracts
                            for contract in prev_contracts:
                                if contract not in contracts:
                                    self.connection.cancelMarketDataSubscription(
                                        self.connection.createContract(contract))
                                    if self.args['orderbook']:
                                        self.connection.cancelMarketDepth(
                                            self.connection.createContract(contract))
                                    time.sleep(0.1)
                                    contract_string = self.connection.contractString(
                                        contract).split('_')[0]
                                    self.log_blotter.info(
                                        'Contract Removed [%s]', contract_string)

                    # request market data
                    for contract in contracts:
                        if contract not in prev_contracts:
                            self.connection.subscribeMarketData(
                                self.connection.createContract(contract))
                            if self.args['orderbook']:
                                self.connection.subscribeMarketDepth(
                                    self.connection.createContract(contract))
                            # time.sleep(0.1)
                            contract_string = self.connection.contractString(
                                contract).split('_')[0]
                            self.log_blotter.info(
                                'Contract Added [%s]', contract_string)

                    # update latest contracts
                    if prev_contracts != contracts:
                        if not self.connection.started:
                            self.connection.stream()

                    prev_contracts = contracts
                time.sleep(10)

        except (KeyboardInterrupt, SystemExit):
            self.quitting = True  # don't display connection errors on ctrl+c
            self.log_blotter.error(
                "\n\n>>> Interrupted with Ctrl-c...\n(waiting for running tasks to be completed)\n")
            # asynctools.multitasking.killall() # stop now
            asynctools.multitasking.wait_for_tasks()  # wait for threads to complete
            sys.exit(1)

    # -------------------------------------------
    # CLIENT / STATIC
    # -------------------------------------------
    def _fix_history_sequence(self, df, table):
        """ fix out-of-sequence ticks/bars """

        # remove "Unnamed: x" columns
        cols = df.columns[df.columns.str.startswith('Unnamed:')].tolist()
        df.drop(cols, axis=1, inplace=True)

        # remove future dates
        df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
        blacklist = df[df['datetime'] > pd.to_datetime('now', utc=True)]
        df = df.loc[set(df.index) - set(blacklist)]  # .tail()

        # loop through data, symbol by symbol
        dfs = []
        bad_ids = [blacklist['_id'].values.tolist()]

        for symbol_id in list(df['symbol'].unique()):

            data = df[df['symbol'] == symbol_id].copy()

            # sort by id
            data.sort_values('datetime', axis=0, ascending=True, inplace=False)

            # convert index to column
            # data.loc[:, "ix"] = data.index
            # data.reset_index(inplace=True)

            # find out of sequence ticks/bars
            malformed = data.shift(1)[(data['resolution'] != data['resolution'].shift(1)) & (
                    data['datetime'] < data['datetime'].shift(1))]

            # cleanup rows
            if malformed.empty:
                # if all rows are in sequence, just remove last row
                dfs.append(data)
            else:
                # remove out of sequence rows + last row from data
                index = [
                    x for x in data.index.values if x not in malformed['datetime'].values]
                dfs.append(data.loc[index])

                # add to bad id list (to remove from db)
                bad_ids.append(list(malformed['_id'].values))

        # combine all lists
        data = pd.concat(dfs, sort=True)

        # flatten bad ids
        bad_ids = sum(bad_ids, [])

        # remove bad ids from db
        if bad_ids:
            bad_ids = list(map(str, map(int, bad_ids)))
            self.log_blotter.warning("Bad Ids found", bad_ids)

        # return
        return data.drop(['_id'], axis=1)

    # -------------------------------------------
    def history(self, symbols, start, end=None, resolution="1T", tz="UTC", continuous=True):
        # load runtime/default data
        if isinstance(symbols, str):
            symbols = symbols.split(',')

        # work with symbol groups
        # symbols = list(map(utils.gen_symbol_group, symbols))
        symbol_groups = list(map(utils.gen_symbol_group, symbols))
        # print(symbols)

        # convert datetime to string for MySQL
        try:
            start = start.strftime(
                COMMON_TYPES["DATE_TIME_FORMAT_LONG_MILLISECS"])
        except Exception as e:
            pass

        if end is not None:
            try:
                end = end.strftime(
                    COMMON_TYPES["DATE_TIME_FORMAT_LONG_MILLISECS"])
            except Exception as e:
                pass

        # connect to mysql
        self.db_connect()

        # --- build query
        table = 'ticks' if resolution[-1] in ("K", "V", "S") else 'bars'

        query = {
            'resolution': resolution
        }
        if symbols[0].strip() != "*":
            query['symbol__in'] = symbols

        from_to_query = (Q(datetime__gte=start) & Q(datetime__lte=end)) if end is not None else (Q(datetime__gte=start))
        data = OHLC.objects(from_to_query, **query).order_by('datetime')  # pd.read_sql(query, self.db_connection)  # .dropna()
        df = pd.DataFrame.from_records([json.loads(d.to_json()) for d in data])
        # del df['_id']
        if df.empty:
            return df
        df['_id'] = df['_id'].apply(lambda _id: _id['$oid'])
        df['datetime'] = df['datetime'].apply(lambda date: datetime.utcfromtimestamp(int(date['$date'] / 1000)))

        # print(df)
        # clearup records that are out of sequence
        data = self._fix_history_sequence(df, table)
        # setup dataframe
        data = prepare_history(data=data, resolution=Timeframes.timeframe_to_resolution(resolution), tz=tz,
                               continuous=continuous)

        # del query['resolution']
        # tick_data = Tick.objects(from_to_query, **query).order_by('datetime')
        # tick_df = pd.DataFrame.from_records([json.loads(d.to_json()) for d in tick_data])
        # tick_df['datetime'] = tick_df['datetime'].apply(lambda date: datetime.utcfromtimestamp(int(date['$date'] / 1000)))
        # tick_df = prepare_history(data=tick_df, tz=tz)
        # data['kind'] = 'BAR'
        # data = data.append(tick_df)
        # data.sort_index(inplace=True)
        # del data['_id']
        return data

    # -------------------------------------------
    def stream(self, symbols, tick_handler=None, bar_handler=None,
               quote_handler=None, book_handler=None, tz="UTC"):
        # load runtime/default data
        if isinstance(symbols, str):
            symbols = symbols.split(',')
        symbols = list(map(str.strip, symbols))

        # connect to zeromq self.socket
        self.context = zmq.Context()
        sock = self.context.socket(zmq.SUB)
        sock.setsockopt_string(zmq.SUBSCRIBE, "")
        uri = 'tcp://127.0.0.1:' + str(self.args['zmqport'])
        if os.getenv("zmq_connection_string"):
            uri = os.getenv("zmq_connection_string")
        sock.connect(uri)

        try:
            while True:
                message = sock.recv_string()

                if self.args["zmqtopic"] in message:
                    message = message.split(self.args["zmqtopic"])[1].strip()
                    data = json.loads(message)

                    if data['symbol'] not in symbols:
                        continue

                    # convert None to np.nan !!
                    data.update((k, np_nan)
                                for k, v in data.items() if v is None)

                    # quote
                    if data['kind'] == "ORDERBOOK":
                        if book_handler is not None:
                            try:
                                book_handler(data)
                            except Exception as e:
                                self.log_blotter.error(e)
                            continue
                    # quote
                    if data['kind'] == "QUOTE":
                        if quote_handler is not None:
                            try:
                                quote_handler(data)
                            except Exception as e:
                                self.log_blotter.error(e)
                            continue

                    try:
                        data["datetime"] = parse_date(data["timestamp"])
                    except Exception as e:
                        pass

                    df = pd.DataFrame(index=[0], data=data)
                    df.set_index('datetime', inplace=True)
                    df.index = pd.to_datetime(df.index, utc=True)
                    df.drop(["timestamp", "kind"], axis=1, inplace=True)

                    try:
                        df.index = df.index.tz_convert(tz)
                    except Exception as e:
                        df.index = df.index.tz_localize('UTC').tz_convert(tz)

                    # add options columns
                    df = force_options_columns(df)

                    if data['kind'] == "TICK":
                        if tick_handler is not None:
                            try:
                                tick_handler(df)
                            except Exception as e:
                                self.log_blotter.error(e)
                    elif data['kind'] == "BAR":
                        if bar_handler is not None:
                            try:
                                bar_handler(df)
                            except Exception as e:
                                self.log_blotter.error(e)

        except (KeyboardInterrupt, SystemExit):
            print(
                "\n\n>>> Interrupted with Ctrl-c...\n(waiting for running tasks to be completed)\n")
            print(".\n.\n.\n")
            # asynctools.multitasking.killall() # stop now
            asynctools.multitasking.wait_for_tasks()  # wait for threads to complete
            sys.exit(1)

    # -------------------------------------------
    @staticmethod
    def drip(data, handler):
        try:
            for i in range(len(data)):
                handler(data.iloc[i:i + 1])
                # time.sleep(.1)

            asynctools.multitasking.wait_for_tasks()
            print("\n\n>>> Backtesting Completed.")

        except (KeyboardInterrupt, SystemExit):
            print(
                "\n\n>>> Interrupted with Ctrl-c...\n(waiting for running tasks to be completed)\n")
            print(".\n.\n.\n")
            # asynctools.multitasking.killall() # stop now
            asynctools.multitasking.wait_for_tasks()  # wait for threads to complete
            sys.exit(1)

    # ---------------------------------------
    def backfill(self, data, resolution, start, end=None, csv_path=None):
        """
        Backfills missing historical data

        :Optional:
            data : pd.DataFrame
                Minimum required bars for backfill attempt
            resolution : str
                Algo resolution
            start: datetime
                Backfill start date (YYYY-MM-DD [HH:MM:SS[.MS]).
            end: datetime
                Backfill end date (YYYY-MM-DD [HH:MM:SS[.MS]). Default is None
        :Returns:
            status : mixed
                False for "won't backfill" / True for "backfilling, please wait"
        """

        data.sort_index(inplace=True)

        # currenly only supporting minute-data
        if resolution[-1] in ("K", "V"):
            self.backfilled = True
            return None

        # missing history?
        start_date = parse_date(start)
        end_date = parse_date(end) if end else datetime.utcnow()

        if data.empty:
            first_date = datetime.utcnow()
            last_date = datetime.utcnow()
        else:
            first_date = datetime64_to_datetime(data.index.values[0])
            last_date = datetime64_to_datetime(data.index.values[-1])

        self.backfill_resolution = Timeframes.to_timeframe(resolution)

        interval = Timeframes.timeframe_to_minutes(self.backfill_resolution)

        wb_lookback = None
        if start_date < first_date:
            wb_lookback = wb_lookback_str(start_date, end_date, interval)
        elif end_date > last_date:
            wb_lookback = wb_lookback_str(last_date, end_date, interval)

        if not wb_lookback:
            self.backfilled = True
            return None

        self.log_blotter.info("Backfilling historical data from IB...")

        # request parameters
        params = {
            "lookback": ceil(wb_lookback),
            "resolution": interval,
            "end_datetime": int(end_date.timestamp()),
            "csv_path": csv_path
        }

        # if connection is active - request data
        try:
            self.connection.requestHistoricalData(**params)
        except Exception as e:
            self.log_blotter.error(e)

        # wait for backfill to complete
        if not self.backfilled:
            raise Exception("Backfill interrupted")

        # otherwise, pass the parameters to the caller
        return True

    # -------------------------------------------
    def register(self, instruments):

        if isinstance(instruments, dict):
            instruments = list(instruments.values())

        if not isinstance(instruments, list):
            return

        try:
            db = pd.read_csv(self.args['symbols'], header=0).fillna("")

            instruments = pd.DataFrame(instruments)
            instruments.columns = db.columns
            # instruments['expiry'] = instruments['expiry'].astype(int).astype(str)

            db = db.append(instruments)
            # db['expiry'] = db['expiry'].astype(int)
            db = db.drop_duplicates(keep="first")

            db.to_csv(self.args['symbols'], header=True, index=False)
            chmod(self.args['symbols'])
        except Exception as e:
            self.log_blotter.error("Skipping symbols file since it couldn't be found in the system", e)

    # -------------------------------------------

    def db_connect(self):
        # skip db connection
        if self.args['dbskip']:
            return

        # already connected?
        if self.db_connection is not None:
            return

        # connect to mongo
        params = {
            'host': str(self.args['dbhost']) if str(self.args['dbhost']) is not None else 'localhost',
            'port': int(self.args['dbport']) if self.args['dbport'] is not None else 27017,
            'username': str(self.args['dbuser']) if str(self.args['dbuser']) is not None else None,
            'password': str(self.args['dbpass']) if str(self.args['dbpass']) is not None else None,
            'db': str(self.args['dbname']) if str(self.args['dbname']) else 'kinetick'
        }
        self.db_connection = mongo_connect(**params)

    # ===========================================
    # Utility functions --->
    # ===========================================

    # -------------------------------------------


# -------------------------------------------
def load_blotter_args(blotter_name=None, logger=None):
    """ Load running blotter's settings (used by clients)

    :Parameters:
        blotter_name : str
            Running Blotter's name (defaults to "auto-detect")
        logger : object
            Logger to be use (defaults to Blotter's)

    :Returns:
        args : dict
            Running Blotter's arguments
    """
    if logger is None:
        logger = create_logger(__name__, logging.WARNING)

    # find specific name
    if blotter_name is not None:  # and blotter_name != 'auto-detect':
        args_cache_file = tempfile.gettempdir() + "/" + blotter_name.lower() + ".kinetick"
        if not os.path.exists(args_cache_file):
            # logger.critical(
            #     "Cannot connect to running Blotter [%s]", blotter_name)
            # if os.isatty(0):
            #     sys.exit(0)
            return {}

    # no name provided - connect to last running
    else:
        blotter_files = sorted(
            glob.glob(tempfile.gettempdir() + "/*.kinetick"), key=os.path.getmtime)

        if not blotter_files:
            # logger.critical(
            #     "Cannot connect to running Blotter [%s]", blotter_name)
            # if os.isatty(0):
            #     sys.exit(0)
            return {}

        args_cache_file = blotter_files[-1]

    args = pickle.load(open(args_cache_file, "rb"))
    args['as_client'] = True

    return args


# -------------------------------------------
def prepare_history(data, resolution=None, tz="UTC", continuous=False):
    # setup dataframe
    data.set_index('datetime', inplace=True)
    data.index = pd.to_datetime(data.index, utc=True)
    # data['expiry'] = pd.to_datetime(data['expiry'], utc=True)

    # data['symbol'] = data['symbol'].str.replace("_STK", "")

    # force options columns
    # data = utils.force_options_columns(data)

    # construct continuous contracts for futures
    if continuous and resolution[-1] not in ("K", "V", "S"):
        all_dfs = [data[~data['symbol'].str.contains('FUT')]]

        # generate dict of df per future
        futures_symbol_groups = list(
            data[data['symbol'].str.contains('FUT')]['symbol'].unique())
        for key in futures_symbol_groups:
            future_group = data[data['symbol'] == key]
            continuous = create_continuous_contract(
                future_group, resolution)
            all_dfs.append(continuous)

        # make one df again
        # data = pd.concat(all_dfs, sort=True)
        data['datetime'] = data.index
        data.groupby([data.index, 'symbol'], as_index=False
                     ).last().set_index('datetime').dropna()

    if resolution is not None:
        data["symbol_group"] = data['symbol']
        data["asset_class"] = data['symbol']
        data = force_options_columns(data)
        data = resample(data, resolution, tz, sync_last_timestamp=False)
    return data


# -------------------------------------------
if __name__ == "__main__":
    blotter = Blotter()
    blotter.run()
