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
import atexit
import hashlib
import logging
import os
import time
import sys

# from decimal import *
import decimal

from abc import ABCMeta, abstractmethod
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from mongoengine import connect

from kinetick.models import Contract
from kinetick.blotter import load_blotter_args, Blotter
from kinetick.enums import SecurityType, PositionType
from kinetick.instrument import Instrument
from kinetick.lib.brokers import Webull, Zerodha
from kinetick.utils import utils
from kinetick.utils.utils import datetime_to_timezone

decimal.getcontext().prec = 5

# =============================================
# check min, python version
if sys.version_info < (3, 4):
    raise SystemError("Kinetick requires Python version >= 3.4")

# =============================================
utils.create_logger(__name__, level=os.getenv('LOGLEVEL') or logging.INFO)


# =============================================


class Broker():
    """Broker class initializer (abstracted, parent class of ``Algo``)
    Current implementation is in hybrid form i.e, streaming data is provided by webull
    and orders are executed on the zerodha. WIP: remove webull dependency.

    :Parameters:

        instruments : list
            List of contract tuples
        zerodha_user : zerodha user id. In next version this will be abstracted to handle multiple brokers.
        zerodha_password: zerodha password
        zerodha_pin: zerodha pin
    """

    __metaclass__ = ABCMeta

    def __init__(self, instruments, zerodha_user=None, zerodha_password=None, zerodha_pin=None, **kwargs):

        # detect running strategy
        self.strategy = str(self.__class__).split('.')[-1].split("'")[0]

        # initialize class logger
        self.log_broker = logging.getLogger(__name__)

        # -----------------------------------
        # assign default vals if not propagated from algo
        if not hasattr(self, 'timezone'):
            self.timezone = "UTC"
        if not hasattr(self, 'tick_window'):
            self.tick_window = 1000
        if not hasattr(self, 'bar_window'):
            self.bar_window = 100
        if not hasattr(self, 'last_price'):
            self.last_price = {}
        if not hasattr(self, 'backtest'):
            self.backtest = False
        if not hasattr(self, 'sms_numbers'):
            self.sms_numbers = []
        if not hasattr(self, 'trade_log_dir'):
            self.trade_log_dir = None
        if not hasattr(self, 'blotter_name'):
            self.blotter_name = None

        self.broker = Webull()
        self.broker.callbacks = self._callback
        self.zerodha = Zerodha(zerodha_user, zerodha_password, zerodha_pin,
                               debug=True) if not self.backtest else None
        self.connect()

        self.log_broker.info("Connection established...")

        # -----------------------------------
        # create contracts
        instrument_tuples_dict = {}
        for instrument in instruments:
            try:
                if isinstance(instrument, Contract):
                    instrument = self.broker.contract_to_tuple(instrument)
                else:
                    instrument = utils.create_contract_tuple(instrument)
                contractString = self.broker.contractString(instrument)
                instrument_tuples_dict[contractString] = instrument
                self.broker.createContract(instrument)
            except Exception as e:
                self.log_broker.error("Error creating contract", e)

        self.instruments = instrument_tuples_dict
        self.symbols = list(self.instruments.keys())
        self.instrument_combos = {}

        self.instrument_obj = {}

        # -----------------------------------
        # track orders & trades
        self.active_trades = {}
        self.trades = []

        # shortcut
        self.account = self.broker.account

        # use: self.orders.pending...
        self.orders = utils.make_object(
            by_tickerid=self.broker.orders,
            by_symbol=self.broker.symbol_orders,
            pending_ttls={},
            pending={},
            filled={},
            active={},
            history={},
            nextId=1,
            recent={}
        )

        # -----------------------------------
        self.db_connection = None

        # -----------------------------------
        # load blotter settings
        self.blotter_args = load_blotter_args(
            self.blotter_name, logger=self.log_broker)
        self.blotter = Blotter(**self.blotter_args)
        self.blotter.args.update(self.blotter.load_cli_args())
        self.blotter_args = self.blotter.args.copy()

        # connect to DB using blotter's settings
        if not self.blotter_args['dbskip']:
            self.mongo_connect()
        # -----------------------------------
        # do stuff on exit
        atexit.register(self._on_exit)

    # ---------------------------------------
    def mongo_connect(self):
        # skip db connection
        if self.blotter_args['dbskip']:
            return

        # already connected?
        if self.db_connection is not None:
            return

        # connect
        params = {
            'host': str(self.blotter_args['dbhost']) if str(self.blotter_args['dbhost']) is not None else 'localhost',
            'port': int(self.blotter_args['dbport']) if self.blotter_args['dbport'] is not None else 27017,
            'username': str(self.blotter_args['dbuser']) if str(self.blotter_args['dbuser']) is not None else None,
            'password': str(self.blotter_args['dbpass']) if str(self.blotter_args['dbpass']) is not None else None,
            'db': str(self.blotter_args['dbname']) if str(self.blotter_args['dbname']) else 'kinetick'
        }
        self.db_connection = connect(**params)

    # ---------------------------------------
    def add_instruments(self, *instruments):
        """ add instruments after initialization """
        for instrument in instruments:
            instrument = self.broker.contract_to_tuple(instrument)
            contractString = self.broker.contractString(instrument)
            self.instruments[contractString] = instrument
            self.broker.createContract(instrument)

        self.symbols = list(self.instruments.keys())

    # ---------------------------------------

    @abstractmethod
    def on_fill(self, instrument, order):
        pass

    # -------------------------------------------
    def _on_exit(self):
        self.log_broker.info("Algo stopped...")

        if self.broker is not None:
            self.log_broker.info("Disconnecting...")
            self.broker.disconnect()

        self.log_broker.info("Disconnecting from DB...")
        try:
            self.db_connection.close()
        except Exception as e:
            pass

    # ---------------------------------------
    def connect(self):
        self.broker.connect(stream=False)
        if not self.backtest and self.zerodha:
            self.zerodha.login()

    # ---------------------------------------
    # @abstractmethod
    def _callback(self, caller, msg, **kwargs):

        if caller == "handleHistoricalData":
            # transmit "as-is" to blotter for handling
            self.blotter.callbacks("handleHistoricalData", msg, **kwargs)

        if caller == "handleConnectionClosed":
            self.log_broker.info("Lost connection to Broker...")

            while not self.broker.connected:
                self.connect()
                time.sleep(1.3)
                if not self.broker.connected:
                    print('*', end="", flush=True)

            self.log_broker.info("Connection established...")

        elif caller == "handleOrders":
            if not hasattr(self, "orders"):
                return

            # order canceled? do some cleanup
            if hasattr(msg, 'status') and "CANCELLED" in msg.status.upper():
                if msg.orderId in self.orders.recent.keys():
                    symbol = self.orders.recent[msg.orderId]['symbol']
                    try:
                        del self.orders.pending_ttls[msg.orderId]
                    except Exception as e:
                        pass
                    try:
                        del self.orders.recent[msg.orderId]
                    except Exception as e:
                        pass
                    try:
                        if self.orders.pending[symbol]['orderId'] == msg.orderId:
                            del self.orders.pending[symbol]
                    except Exception as e:
                        pass
                return

            # continue...

            order = self.zerodha.orders[msg.orderId]

            # print("***********************\n\n", order, "\n\n***********************")
            orderId = msg.orderId
            symbol = order["symbol"]

            try:
                try:
                    quantity = self.orders.history[symbol][orderId]['quantity']
                except Exception as e:
                    quantity = self.orders.history[symbol][order['parentId']]['quantity']
                    # ^^ for child orders auto-created by ezibpy
            except Exception as e:
                quantity = 1

            # update pending order to the time actually submitted
            if order["status"] in ["OPENED", "SUBMITTED"]:
                if orderId in self.orders.pending_ttls:
                    self._update_pending_order(symbol, orderId,
                                               self.orders.pending_ttls[orderId],
                                               quantity)

            elif order["status"] == "FILLED":
                self._update_order_history(
                    symbol, orderId, quantity, filled=True)
                self._expire_pending_order(symbol, orderId)
                self._cancel_orphan_orders(orderId)
                self._register_trade(order)

                # filled
                time.sleep(0.005)
                self.on_fill(self.get_instrument(order['symbol']), order)

    # ---------------------------------------
    def _register_trade(self, order):
        """ constructs trade info from order data """
        if order['id'] in self.orders.recent:
            orderId = order['id']
        else:
            orderId = order['parentId']
        # entry / exit?
        symbol = order["symbol"]
        order_data = self.orders.recent[orderId]
        position = self.get_positions(symbol)['position']

        if position != 0:
            # entry
            order_data['action'] = "ENTRY"
            order_data['position'] = position
            order_data['entry_time'] = datetime_to_timezone(
                order['time'])
            order_data['exit_time'] = None
            order_data['entry_order'] = order_data['order_type']
            order_data['entry_price'] = order['avgFillPrice']
            order_data['exit_price'] = 0
            order_data['exit_reason'] = None

        else:
            order_data['action'] = "EXIT"
            order_data['position'] = 0
            order_data['exit_time'] = datetime_to_timezone(order['time'])
            order_data['exit_price'] = order['avgFillPrice']

            # target / stop?
            if order['id'] == order_data['targetOrderId']:
                order_data['exit_reason'] = "TARGET"
            elif order['id'] == order_data['stopOrderId']:
                order_data['exit_reason'] = "STOP"
            else:
                order_data['exit_reason'] = "SIGNAL"

            # remove from collection
            del self.orders.recent[orderId]

        if order_data is None:
            return None

        # trade identifier
        tradeId = self.strategy.upper() + '_' + symbol.upper()
        tradeId = hashlib.sha1(tradeId.encode()).hexdigest()

        # existing trade?
        if tradeId not in self.active_trades:
            self.active_trades[tradeId] = {
                "strategy": self.strategy,
                "action": order_data['action'],
                "quantity": abs(order_data['position']),
                "position": order_data['position'],
                "symbol": order_data["symbol"].split('_')[0],
                "direction": order_data['direction'],
                "entry_time": None,
                "exit_time": None,
                "duration": "0s",
                "exit_reason": order_data['exit_reason'],
                "order_type": order_data['order_type'],
                "market_price": order_data['price'],
                "target": order_data['target'],
                "stop": order_data['initial_stop'],
                "entry_price": 0,
                "exit_price": order_data['exit_price'],
                "realized_pnl": 0
            }
            if "entry_time" in order_data:
                self.active_trades[tradeId]["entry_time"] = order_data['entry_time']
            if "entry_price" in order_data:
                self.active_trades[tradeId]["entry_price"] = order_data['entry_price']
        else:
            # self.active_trades[tradeId]['direction']   = order_data['direction']
            self.active_trades[tradeId]['action'] = order_data['action']
            self.active_trades[tradeId]['position'] = order_data['position']
            self.active_trades[tradeId]['exit_price'] = order_data['exit_price']
            self.active_trades[tradeId]['exit_reason'] = order_data['exit_reason']
            self.active_trades[tradeId]['exit_time'] = order_data['exit_time']

            # calculate trade duration
            try:
                delta = int((self.active_trades[tradeId]['exit_time'] -
                             self.active_trades[tradeId]['entry_time']).total_seconds())
                days, remainder = divmod(delta, 86400)
                hours, remainder = divmod(remainder, 3600)
                minutes, seconds = divmod(remainder, 60)
                duration = ('%sd %sh %sm %ss' %
                            (days, hours, minutes, seconds))
                self.active_trades[tradeId]['duration'] = duration.replace(
                    "0d ", "").replace("0h ", "").replace("0m ", "")
            except Exception as e:
                pass

            trade = self.active_trades[tradeId]
            if trade['entry_price'] > 0 and trade['position'] == 0:
                if trade['direction'] == "SELL":
                    pnl = trade['entry_price'] - trade['exit_price']
                else:
                    pnl = trade['exit_price'] - trade['entry_price']

                pnl = utils.to_decimal(pnl)
                # print("1)", pnl)
                self.active_trades[tradeId]['realized_pnl'] = pnl

        # print("\n\n-----------------")
        # print(self.active_trades[tradeId])
        # print("-----------------\n\n")

        # get trade
        trade = self.active_trades[tradeId].copy()

        # rename trade direction
        trade['direction'] = trade['direction'].replace(
            "BUY", "LONG").replace("SELL", "SHORT")

        # log
        self.log_trade(trade)

        # remove from active trades and add to trade
        if trade['action'] == "EXIT":
            del self.active_trades[tradeId]
            self.trades.append(trade)

        # return trade
        return trade

    # ---------------------------------------
    def log_trade(self, trade):

        # first trade is an exit?
        if trade['entry_time'] is None:
            return

        # connection established
        if self.db_connection is not None:
            pass
            # TODO save to db

        if self.trade_log_dir:
            self.trade_log_dir = (self.trade_log_dir + '/').replace('//', '/')
            trade_log_path = self.trade_log_dir + self.strategy.lower() + "_" + \
                             datetime.now().strftime('%Y%m%d') + ".csv"

            # convert None to empty string !!
            trade.update((k, '') for k, v in trade.items() if v is None)

            # create df
            trade_df = pd.DataFrame(index=[0], data=trade)[[
                'strategy', 'symbol', 'direction', 'quantity', 'entry_time',
                'exit_time', 'exit_reason', 'order_type', 'market_price', 'target',
                'stop', 'entry_price', 'exit_price', 'realized_pnl'
            ]]

            if os.path.exists(trade_log_path):
                trades = pd.read_csv(trade_log_path, header=0)
                trades = trades.append(trade_df, ignore_index=True, sort=True)
                trades.drop_duplicates(['entry_time', 'symbol', 'strategy'],
                                       keep="last", inplace=True)
                trades.to_csv(trade_log_path, header=True, index=False)
                utils.chmod(trade_log_path)
            else:
                trade_df.to_csv(trade_log_path, header=True, index=False)
                utils.chmod(trade_log_path)

    # ---------------------------------------
    def active_order(self, symbol, order_type="STOP"):
        if symbol in self.orders.history:
            for orderId in self.orders.history[symbol]:
                order = self.orders.history[symbol][orderId]
                if order['order_type'].upper() == order_type.upper():
                    return order
        return None

    # ---------------------------------------
    @staticmethod
    def _get_locals(local_params):
        del local_params['self']
        return local_params

    # ---------------------------------------
    def _create_order(self, symbol, direction, quantity, order_type="",
                      limit_price=0, expiry=0, order_id=0, target=0,
                      initial_stop=0, trail_stop_at=0, trail_stop_by=0,
                      stop_limit=False, trail_stop_type='percent', pos_type=None, **kwargs):

        # fix prices to comply with contract's min-tick
        ticksize = self.get_contract_details(symbol)['m_minTick']
        limit_price = utils.round_to_fraction(limit_price, ticksize)
        target = utils.round_to_fraction(target, ticksize)
        initial_stop = utils.round_to_fraction(initial_stop, ticksize)
        trail_stop_at = utils.round_to_fraction(trail_stop_at, ticksize)
        trail_stop_by = utils.round_to_fraction(trail_stop_by, ticksize)
        trail_stop_type = "amount" if trail_stop_type == "amount" else "percent"

        self.log_broker.debug('CREATE ORDER: %s %4d %s %s', direction,
                              quantity, symbol, dict(locals(), **kwargs))

        # force BUY/SELL (not LONG/SHORT)
        direction = direction.replace("LONG", "BUY").replace("SHORT", "SELL")

        # modify order?
        if order_type.upper() == "MODIFY":
            self.modify_order(symbol, order_id, quantity, limit_price)
            return

        # continue...

        if "stoploss" in kwargs and initial_stop == 0:
            initial_stop = kwargs['stoploss']

        order_type = Zerodha.ORDER_TYPE_MARKET if limit_price == 0 else Zerodha.ORDER_TYPE_LIMIT
        fillorkill = kwargs["fillorkill"] if "fillorkill" in kwargs else False
        iceberg = kwargs["iceberg"] if "iceberg" in kwargs else False
        tif = kwargs["tif"] if "tif" in kwargs else "DAY"

        # clear expired pending orders
        # self._cancel_expired_pending_orders()

        # # don't submit order if a pending one is waiting
        # if symbol in self.orders.pending:
        #     self.log_broker.warning(
        #         'Not submitting %s order, orders pending: %s', symbol,
        #         self.orders.pending)
        #     return

        # continue...
        order_quantity = abs(quantity)
        if direction.upper() == "SELL":
            order_quantity = -order_quantity

        contract = self.get_contract(symbol)

        # is bracket order
        bracket = (target > 0) | (initial_stop > 0) | (
                trail_stop_at > 0) | (trail_stop_by > 0)
        trigger_price = kwargs['trigger_price'] if 'trigger_price' in kwargs else initial_stop
        # create & submit order
        sec_type = kwargs['sec_type'] if 'sec_type' in kwargs else SecurityType.STOCK
        pos_type = pos_type or PositionType.CO if bracket else None
        variety, product = self.zerodha.get_order_variety(sec_type, pos_type)

        if sec_type == SecurityType.OPTION:
            tradingsymbol = kwargs['opt_ticker']
            order_id = self.zerodha.place_order(variety=variety, tradingsymbol=tradingsymbol,
                                                transaction_type=direction.upper(),
                                                quantity=abs(order_quantity), product=product,
                                                order_type=order_type,
                                                exchange=Zerodha.EXCHANGE_NFO, price=limit_price,
                                                trigger_price=trigger_price, stoploss=initial_stop)
        else:
            # simple order
            order_id = self.zerodha.place_order(variety=variety, tradingsymbol=symbol,
                                                transaction_type=direction.upper(),
                                                quantity=abs(order_quantity), product=product,
                                                order_type=order_type,
                                                exchange=Zerodha.EXCHANGE_NSE, price=limit_price,
                                                trigger_price=trigger_price, stoploss=initial_stop)
        self.log_broker.debug('PLACED ORDER: %s %s', self.broker.contract_to_dict(
            contract), order_id)

        # triggered trailing stop?
        # if trail_stop_by != 0 and trail_stop_at != 0:
        #     trail_stop_params = {
        #         "symbol": symbol,
        #         "quantity": -order_quantity,
        #         "triggerPrice": trail_stop_at,
        #         "parentId": order["entryOrderId"],
        #         "stopOrderId": order["stopOrderId"]
        #     }
        #     if trail_stop_type.lower() == 'amount':
        #         trail_stop_params["trailAmount"] = trail_stop_by
        #     else:
        #         trail_stop_params["trailPercent"] = trail_stop_by
        #     self.ibConn.createTriggerableTrailingStop(**trail_stop_params)

        # add all orders to history
        self._update_order_history(symbol=symbol,
                                   orderId=order_id,
                                   quantity=order_quantity,
                                   order_type='ENTRY')

        # self._update_order_history(symbol=symbol,
        #                            orderId=order["targetOrderId"],
        #                            quantity=-order_quantity,
        #                            order_type='TARGET',
        #                            parentId=order["entryOrderId"])
        #
        # self._update_order_history(symbol=symbol,
        #                            orderId=order["stopOrderId"],
        #                            quantity=-order_quantity,
        #                            order_type='STOP',
        #                            parentId=order["entryOrderId"])

        # have original params available for FILL event
        self.orders.recent[order_id] = self._get_locals(locals())
        self.orders.recent[order_id]['targetOrderId'] = 0
        self.orders.recent[order_id]['stopOrderId'] = 0

        if bracket:
            pass
            # self.orders.recent[orderId]['targetOrderId'] = order["targetOrderId"]
            # self.orders.recent[orderId]['stopOrderId'] = order["stopOrderId"]

        # append market price at the time of order
        try:
            self.orders.recent[order_id]['price'] = self.last_price[symbol]
        except Exception as e:
            self.orders.recent[order_id]['price'] = 0

        # add orderId / ttl to (auto-adds to history)
        expiry = expiry * 1000 if expiry > 0 else 60000  # 1min
        self._update_pending_order(symbol, order_id, expiry, order_quantity)
        time.sleep(0.1)
        return order_id

    # ---------------------------------------
    def _cancel_order(self, order_id):
        if order_id is not None:
            return self.zerodha.exit_order(order_id)

    # ---------------------------------------
    def modify_order_group(self, symbol, orderId, entry=None,
                           target=None, stop=None, quantity=None):
        """ IMPORTANT! Not yet supported TODO """
        raise Exception("Not supported")
        # order_group = self.orders.recent[orderId]['order']
        #
        # if entry is not None:
        #     self.modify_order(
        #         symbol, orderId, limit_price=entry, quantity=quantity)
        #
        # if target is not None:
        #     self.modify_order(symbol, order_group['targetOrderId'],
        #                       limit_price=target, quantity=quantity)
        # if stop is not None:
        #     stop_quantity = quantity * -1 if quantity is not None else None
        #     self.modify_order(symbol, order_group['stopOrderId'],
        #                       limit_price=stop, quantity=stop_quantity)

    # ---------------------------------------
    def modify_order(self, symbol, orderId, quantity=None, limit_price=None):
        """ IMPORTANT! Not supported yet """
        raise Exception("Not supported")

    # ---------------------------------------
    @staticmethod
    def _milliseconds_delta(delta):
        return delta.days * 86400000 + delta.seconds * 1000 + delta.microseconds / 1000

    # ---------------------------------------
    def _cancel_orphan_orders(self, orderId):
        """ cancel child orders when parent is gone """
        orders = self.zerodha.orders
        for order in orders:
            order = orders[order]
            if order['parentId'] != orderId:
                self.broker.cancelOrder(order['id'])

    # ---------------------------------------
    def _cancel_expired_pending_orders(self):
        """ expires pending orders TODO"""
        # use a copy to prevent errors
        pending = self.orders.pending.copy()
        for symbol in pending:
            orderId = pending[symbol]["orderId"]
            expiration = pending[symbol]["expires"]

            delta = expiration - datetime.now()
            delta = self._milliseconds_delta(delta)

            # cancel order if expired
            if delta < 0:
                self.zerodha.cancel_order(orderId)
                if orderId in self.orders.pending_ttls:
                    if orderId in self.orders.pending_ttls:
                        del self.orders.pending_ttls[orderId]
                    if symbol in self.orders.pending:
                        if self.orders.pending[symbol]['orderId'] == orderId:
                            del self.orders.pending[symbol]

    # ---------------------------------------------------------
    def _expire_pending_order(self, symbol, orderId):
        self.zerodha.cancel_order(order_id=orderId) #TODO

        if orderId in self.orders.pending_ttls:
            del self.orders.pending_ttls[orderId]

        if symbol in self.orders.pending:
            if self.orders.pending[symbol]['orderId'] == orderId:
                del self.orders.pending[symbol]

    # ---------------------------------------------------------
    def _update_pending_order(self, symbol, orderId, expiry, quantity):
        self.orders.pending[symbol] = {
            "orderId": orderId,
            "quantity": quantity,
            # "created": datetime.now(),
            "expires": datetime.now() + timedelta(milliseconds=expiry)
        }

        # ibCallback needs this to update with submittion time
        self.orders.pending_ttls[orderId] = expiry
        self._update_order_history(
            symbol=symbol, orderId=orderId, quantity=quantity)

    # ---------------------------------------------------------
    def _update_order_history(self, symbol, orderId, quantity,
                              order_type='entry', filled=False, parentId=0):
        if symbol not in self.orders.history:
            self.orders.history[symbol] = {}

        self.orders.history[symbol][orderId] = {
            "orderId": orderId,
            "quantity": quantity,
            "order_type": order_type.upper(),
            "filled": filled,
            "parentId": parentId
        }

    # ---------------------------------------
    # UTILITY FUNCTIONS
    # ---------------------------------------
    def get_instrument(self, symbol):
        """
        A string subclass that provides easy access to misc
        symbol-related methods and information using shorthand.
        Refer to the `Instruments API <#instrument-api>`_
        for available methods and properties

        Call from within your strategy:
        ``instrument = self.get_instrument("SYMBOL")``

        :Parameters:
            symbol : string
                instrument symbol

        """
        symbol = self.get_symbol(symbol)
        if symbol in self.instrument_obj:
            return self.instrument_obj[symbol]
        else:
            instrument = Instrument(symbol)
            instrument._set_parent(self)
            instrument._set_windows(ticks=self.tick_window, bars=self.bar_window)
            self.instrument_obj[symbol] = instrument
            return instrument

    # ---------------------------------------
    @staticmethod
    def get_symbol(symbol):
        if not isinstance(symbol, str):
            if isinstance(symbol, dict):
                symbol = symbol['symbol']
            elif isinstance(symbol, pd.DataFrame):
                symbol = symbol[:1]['symbol'].values[0]

        return symbol

    # ---------------------------------------
    def get_account(self):
        return self.broker.account

    # ---------------------------------------
    def get_contract(self, symbol):
        return self.broker.contracts[self.broker.tickerId(symbol)]

    # ---------------------------------------
    def get_contract_details(self, symbol):
        return self.broker.contractDetails(symbol)

    # ---------------------------------------
    def get_tickerId(self, symbol):
        return self.broker.tickerId(symbol)

    # ---------------------------------------
    def get_orders(self, symbol):
        symbol = self.get_symbol(symbol)

        self.orders.by_symbol = self.broker.group_orders("symbol")
        if symbol in self.orders.by_symbol:
            return self.orders.by_symbol[symbol]

        return {}

    # ---------------------------------------
    def get_positions(self, symbol):
        symbol = self.get_symbol(symbol)

        if self.backtest:
            position = 0
            avgCost = 0.0

            if self.datastore.recorded is not None:
                data = self.datastore.recorded
                col = symbol.upper() + '_POSITION'
                position = data[col].values[-1]
                if position != 0:
                    pos = data[col].diff()
                    avgCost = data[data.index.isin(pos[pos != 0][-1:].index)
                    ][symbol.upper() + '_OPEN'].values[-1]
            return {
                "symbol": symbol,
                "position": position,
                "avgCost": avgCost,
                "account": "Backtest"
            }

        elif symbol in self.broker.positions:
            return self.broker.positions[symbol]

        return {
            "symbol": symbol,
            "position": 0,
            "avgCost": 0.0,
            "account": None
        }

    # ---------------------------------------
    def get_portfolio(self, symbol=None):
        raise Exception("Not supported")
        # if symbol is not None:
        #     symbol = self.get_symbol(symbol)
        #
        #     if symbol in self.zerodha.portfolio:
        #         portfolio = self.zerodha.portfolio[symbol]
        #         if "symbol" in portfolio:
        #             return portfolio
        #
        #     return {
        #         "symbol": symbol,
        #         "position": 0.0,
        #         "marketPrice": 0.0,
        #         "marketValue": 0.0,
        #         "averageCost": 0.0,
        #         "unrealizedPNL": 0.0,
        #         "realizedPNL": 0.0,
        #         "totalPNL": 0.0,
        #         "account": None
        #     }
        #
        # return self.zerodha.portfolio

    # ---------------------------------------
    def get_pending_orders(self, symbol=None):
        if symbol is not None:
            symbol = self.get_symbol(symbol)
            if symbol in self.orders.pending:
                return self.orders.pending[symbol]
            return {}

        return self.orders.pending

    # ---------------------------------------
    def get_trades(self, symbol=None):

        # closed trades
        trades = pd.DataFrame(self.trades)
        if not trades.empty:
            trades.loc[:, 'closed'] = True

        # ongoing trades
        active_trades = pd.DataFrame(list(self.active_trades.values()))
        if not active_trades.empty:
            active_trades.loc[:, 'closed'] = False

        # combine dataframes
        df = pd.concat([trades, active_trades], sort=True).reset_index()

        # set last price
        if not df.empty:

            # conert values to floats
            df['entry_price'] = df['entry_price'].astype(float)
            df['exit_price'] = df['exit_price'].astype(float)
            df['market_price'] = df['market_price'].astype(float)
            df['realized_pnl'] = df['realized_pnl'].astype(float)
            df['stop'] = df['stop'].astype(float)
            df['target'] = df['target'].astype(float)
            df['quantity'] = df['quantity'].astype(int)

            try:
                df.loc[:, 'last'] = self.last_price[symbol]
            except Exception as e:
                df.loc[:, 'last'] = 0

            # calc unrealized pnl
            df['unrealized_pnl'] = np.where(df['direction'] == "SHORT",
                                            df['entry_price'] - df['last'],
                                            df['last'] - df['entry_price'])

            df.loc[df['closed'], 'unrealized_pnl'] = 0

            # drop index column
            df.drop('index', axis=1, inplace=True)

            # get single symbol
            if symbol is not None:
                df = df[df['symbol'] == symbol.split("_")[0]]
                df.loc[:, 'symbol'] = symbol

        # return
        return df
