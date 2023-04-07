#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

from ats.modules.core.aggregates.Bar import Bar
from ats.modules.core.aggregates.Quote import Quote
from ats.modules.core.aggregates.Tick import Tick
from ats.modules.core.repo.BaseRepo import BaseRepo
from ats.shared.types.enums import DateTimeFormatEnum
from kinetick.instrument import Instrument

from numpy import (
    isnan as np_isnan,
    nan as np_nan,
    int64 as np_int64
)

from ats.modules.core.services.streamprovider.StreamProvider import StreamProvider
from ats.shared.config.Appconfig import Appconfig
from ats.shared.infra.logging.logger import logger
from ats.shared.types.timeframe import Timeframe
from kinetick import (
    path,
)
from kinetick.utils import utils, asynctools
from mongoengine import connect as mongo_connect, NotUniqueError, Q
from kinetick.enums import Timeframes, COMMON_TYPES
from kinetick.lib.brokers import Webull
from kinetick.utils.utils import (
    read_single_argv,
    is_number,
    create_logger, force_options_columns, datetime64_to_datetime, wb_lookback_str, chmod, resample,
    create_continuous_contract, gen_symbol_group, gen_asset_class
)

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
        orderbook : str
            Get Order Book (Market Depth) data (default: False)
    """

    __metaclass__ = ABCMeta

    def __init__(self, streamclient: StreamProvider, barRepo: BaseRepo[Bar], tickRepo: BaseRepo[Tick],
                 symbols="symbols.csv", orderbook=False, **kwargs):
        self.barRepo = barRepo
        self.tickRepo = tickRepo
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

        self.context = None
        self.socket = None
        self.streamclient: StreamProvider = streamclient  # stream provider

        self.symbol_ids = {}  # cache
        self.cash_ticks = cash_ticks  # outside cache

        self.zmqtopic = Appconfig.BLOTTER_ZMQ_TOPIC
        self.zmqport = Appconfig.BLOTTER_ZMQ_PORT

        # if no path given for symbols' csv, use same dir
        if symbols == "symbols.csv":
            symbols = path['caller'] + '/' + symbols

        atexit.register(self._on_exit)

        # track historical data download status
        self.backfilled = False
        self.backfilled_symbols = []
        self.backfill_resolution = Timeframe.MINUTE_1.value  # default to 1 min

        # be aware of thread count
        self.threads = asynctools.multitasking.getPool(__name__)['threads']

    # -------------------------------------------
    def _on_exit(self, terminate=True):
        logger.info("Blotter::_on_exit: Blotter stopped...")
        if terminate:
            sys.exit(0)

    # -------------------------------------------
    def setCallbacks(self, streamclient: StreamProvider):
        streamclient.addTickHandler(self.on_tick_string_received)
        streamclient.addQuoteHandler(self.on_quote_received)
        streamclient.addBarHandler(self.on_ohlc_received)

    # -------------------------------------------
    def on_ohlc_received(self, instrument: Instrument, bar: Bar):
        self.barRepo.save(bar)

    # -------------------------------------------
    @asynctools.multitasking.task
    def on_tick_string_received(self, instrument: Instrument, tick: Tick):
        symbol = instrument
        data = tick

        self.tickRepo.save(tick)

        # proceed if data exists
        if data is not None:
            # cache last tick
            if symbol in self.cash_ticks.keys():
                if data == self.cash_ticks[symbol]:
                    return

            self.cash_ticks[symbol] = data

            # print('.', end="", flush=True)
            self.on_tick_received(tick)

    # -------------------------------------------
    @asynctools.multitasking.task
    def on_quote_received(self, instrument: Instrument, quote: Quote):
        try:
            # TODO store quote
            self.broadcast(quote.serialize(), "QUOTE")
        except Exception as e:
            logger.error(f'Blotter::on_quote_received: Error {e}')

    # -------------------------------------------
    @asynctools.multitasking.task
    def on_tick_received(self, tick: Tick):
        # data
        symbol = tick.symbol
        timestamp = datetime.strptime(tick.timestamp,
                                      DateTimeFormatEnum.DATE_TIME_FORMAT_LONG_MILLISECS.value)

        try:
            timestamp = parse_date(timestamp)
        except Exception as e:
            logger.warn(f"Blotter::on_tick_received: ignored parse_date exception ${e}")

        if symbol not in self._raw_bars:
            self._raw_bars[symbol] = self._raw_bars['~']

        if symbol not in self._bars:
            self._bars[symbol] = self._bars['~']

        # send tick to message self.broadcast
        self.broadcast(tick.serialize(), "TICK")

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
                COMMON_TYPES["DATE_TIME_FORMAT_LONG"])

            barResult = Bar.createFromDict(bar)
            if barResult.isFailure:
                logger.error(f'Blotter::on_tick_received: {barResult.error.message}')
                return

            self.broadcast(barResult.getValue().serialize(), "BAR")
            self.barRepo.save(barResult.getValue())

            self._bars[symbol] = self._bars[symbol][-1:]
            _raw_bars.drop(_raw_bars.index[:], inplace=True)
        self._raw_bars[symbol] = _raw_bars

    # -------------------------------------------
    def broadcast(self, data, kind):
        def int64_handler(o):
            if isinstance(o, np_int64):
                try:
                    return pd.to_datetime(o, unit='ms').strftime(
                        DateTimeFormatEnum.DATE_TIME_FORMAT_LONG.value)
                except Exception as e:
                    return int(o)
            raise TypeError

        string2send = "%s %s" % (
            self.zmqtopic, json.dumps(data, default=int64_handler))

        # print(kind, string2send)
        try:
            self.socket.send_string(string2send)
        except Exception as e:
            logger.error(f'Blotter::broadcast: Error sending data. {e}')

    # -------------------------------------------
    def run(self):
        """Starts the blotter
        and broadcast it over TCP via ZeroMQ (which algo subscribe to)
        """

        self.context = zmq.Context(zmq.REP)
        self.socket = self.context.socket(zmq.PUB)
        self.socket.bind("tcp://*:" + str(self.zmqport))

        symbols_store_modified = 0
        contracts = []
        prev_contracts = []
        first_run = True

        logger.info("Blotter::run: Connecting to Broker...")

        self.streamclient.stream()

        logger.info("Blotter::run: Connection established...")

        try:
            while True:

                if not os.path.exists(self.symbols):
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
                            self.streamclient.cancelMarketDataSubscription()
                            time.sleep(0.1)
                            prev_contracts = []
                        continue

                    # modified?
                    if not first_run and db_last_modified == symbols_store_modified:
                        continue

                    # continue...
                    symbols_store_modified = db_last_modified

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
                                    self.streamclient.cancelMarketDataSubscription(
                                        self.streamclient.createContract(contract))
                                    if self.args['orderbook']:
                                        self.streamclient.cancelMarketDepth(
                                            self.streamclient.createContract(contract))
                                    time.sleep(0.1)
                                    contract_string = self.streamclient.contractString(
                                        contract).split('_')[0]
                                    self.log_blotter.info(
                                        'Contract Removed [%s]', contract_string)

                    # request market data
                    for contract in contracts:
                        if contract not in prev_contracts:
                            self.streamclient.subscribeMarketData(
                                self.streamclient.createContract(contract))
                            if self.args['orderbook']:
                                self.streamclient.subscribeMarketDepth(
                                    self.streamclient.createContract(contract))
                            # time.sleep(0.1)
                            contract_string = self.streamclient.contractString(
                                contract).split('_')[0]
                            self.log_blotter.info(
                                'Contract Added [%s]', contract_string)

                    # update latest contracts
                    if prev_contracts != contracts:
                        if not self.streamclient.started:
                            self.streamclient.stream()

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
        data = Bar.objects(from_to_query, **query).order_by(
            'datetime')  # pd.read_sql(query, self.db_connection)  # .dropna()
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
            self.streamclient.requestHistoricalData(**params)
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
