#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# webull: wrapper around unofficial webull APIs
# https://github.com/tedchou12/webull.git
# Copyright 2019-2021 vin8tech
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

import atexit
import os
import time
import logging
import sys

from datetime import datetime
import dateutil.parser
from pandas import DataFrame

from webull import webull as wb
from webull import paper_webull

import copy

from webull.streamconn import StreamConn

from kinetick.enums import COMMON_TYPES
from kinetick.models import Contract
from kinetick.utils import utils, asynctools

# ---------------------------------------------
LOGLEVEL = os.getenv('LOGLEVEL') or logging.getLevelName(logging.INFO)
utils.create_logger('webull-client', LOGLEVEL)


# =============================================

class Webull:

    # -----------------------------------------
    @staticmethod
    def roundClosestValid(val, res=0.01, decimals=None):
        if val is None:
            return None
        """ round to closest resolution """
        if decimals is None and "." in str(res):
            decimals = len(str(res).split('.')[1])

        return round(round(val / res) * res, decimals)

    # -----------------------------------------
    def __init__(self, paper=False):
        """Initialize a new webull object."""
        self.streamConnection = StreamConn(debug_flg=LOGLEVEL.upper() == "DEBUG")
        self.streamConnection.price_func = self.handleServerEvents
        self.streamConnection.order_func = self.handleServerEvents
        self.username = ""
        self.password = ""
        self.paper = paper
        if not paper:
            self.wb = wb()
        else:
            self.wb = paper_webull()

        self.connected = False
        self.started = False

        self.time = 0
        self.commission = 0
        self.orderId = int(time.time()) - 1553126400  # default
        self.default_account = None

        # auto-construct for every contract/order
        self.tickerIds = {0: "SYMBOL"}
        self.contracts = {}
        self.orders = {}
        self.account_orders = {}
        self.account_symbols_orders = {}
        self.symbol_orders = {}

        self._accounts = {}
        self._positions = {}
        self._portfolios = {}
        self._contract_details = {}  # multiple expiry/strike/side contracts

        self.contract_details = {}
        self.localSymbolExpiry = {}

        # do not reconnect if disconnected by user
        # only try and reconnect if disconnected by network/other issues
        self._disconnected_by_user = False

        # -------------------------------------
        self.log = logging.getLogger('webull-client')  # get logger
        # -------------------------------------

        # holds market data
        tickDF = DataFrame({
            "datetime": [0], "buy": [0], "buysize": [0],
            "sell": [0], "sellsize": [0], "last": [0], "lastsize": [0]
        })
        tickDF.set_index('datetime', inplace=True)
        self.marketData = {0: tickDF}  # idx = tickerId

        # holds market quote data
        quoteDF = DataFrame({
            "datetime": [0], "bid": [0], "bidsize": [0],
            "ask": [0], "asksize": [0],
            "open": [0], "high": [0], "low": [0], "close": [0],
            "volume": [0], "vwap": [0], "symbol": [0]
        })
        quoteDF.set_index('datetime', inplace=True)
        self.marketQuoteData = {0: quoteDF}  # idx = tickerId

        # holds orderbook data
        l2DF = DataFrame(index=range(5), data={
            "bid": 0, "bidsize": 0,
            "ask": 0, "asksize": 0
        })
        # holds time of sale
        # holds quote
        self.marketDepthData = {0: l2DF}  # idx = tickerId

        # trailing stops
        self.trailingStops = {}
        # "tickerId" = {
        #     orderId: ...
        #     lastPrice: ...
        #     trailPercent: ...
        #     trailAmount: ...
        #     quantity: ...
        # }

        # triggerable trailing stops
        self.triggerableTrailingStops = {}
        # "tickerId" = {
        #     parentId: ...
        #     stopOrderId: ...
        #     triggerPrice: ...
        #     trailPercent: ...
        #     trailAmount: ...
        #     quantity: ...
        # }

        # holds options data
        optionsDF = DataFrame({
            "datetime": [0], "oi": [0], "volume": [0], "underlying": [0], "iv": [0],
            "bid": [0], "bidsize": [0], "ask": [0], "asksize": [0], "last": [0], "lastsize": [0],
            # opt field
            "price": [0], "dividend": [0], "imp_vol": [0], "delta": [0],
            "gamma": [0], "vega": [0], "theta": [0],
            "last_price": [0], "last_dividend": [0], "last_imp_vol": [0], "last_delta": [0],
            "last_gamma": [0], "last_vega": [0], "last_theta": [0],
            "bid_price": [0], "bid_dividend": [0], "bid_imp_vol": [0], "bid_delta": [0],
            "bid_gamma": [0], "bid_vega": [0], "bid_theta": [0],
            "ask_price": [0], "ask_dividend": [0], "ask_imp_vol": [0], "ask_delta": [0],
            "ask_gamma": [0], "ask_vega": [0], "ask_theta": [0],
        })
        optionsDF.set_index('datetime', inplace=True)
        self.optionsData = {0: optionsDF}  # idx = tickerId

        # historical data contrainer
        self.historicalData = {}  # idx = symbol
        self.utc_history = False

        # register exit
        atexit.register(self.disconnect)

        # fire connected/disconnected callbacks/errors once per event
        self.connection_tracking = {
            "connected": False,
            "disconnected": False,
            "errors": []
        }

    # -----------------------------------------
    def log_msg(self, title, msg):
        # log handler msg
        logmsg = copy.copy(msg)
        if hasattr(logmsg, "contract"):
            logmsg.contract = self.contractString(logmsg.contract)
        self.log.info("[" + str(title).upper() + "]: %s", str(logmsg))

    # -----------------------------------------
    def connect(self, username='test@test.com', password='pa$$w0rd', stream=False):
        """ login to webull """
        # connect
        if not self.connected:
            self.log.info("[CONNECTING TO WEBULL]")
            if not self.paper:
                self.wb.login(username, password)
            if stream:
                self.streamConnect()

            self.connected = True
            self._disconnected_by_user = False
            self.username = username
            self.password = password
            self.log.info("[connected to webull]")
            # time.sleep(1)
            self.callbacks(caller="handleConnectionOpened", msg="<connectionOpened>")
        else:
            raise Exception("Already connected! Please disconnect to connect again.")

    # -----------------------------------------
    def disconnect(self):
        if self.connected and self.wb is not None:
            self.log.info("[DISCONNECTING FROM WEBULL]")
            self.wb.logout()
            if self.streamConnection and self.started:
                self.streamConnection.client_streaming_quotes.loop_stop()
                self.streamConnection.client_streaming_quotes.disconnect()
            self._disconnected_by_user = True
            self.connected = False
            self.started = False

    # -----------------------------------------
    def getServerTime(self):
        """ get the current time on Server """
        self.time = datetime.utcnow()

    # -----------------------------------------

    # -----------------------------------------
    def getAccountDetails(self):
        """ get the current user details """
        self.wb.get_account()

    # -----------------------------------------
    @staticmethod
    def contract_to_dict(contract):
        """Convert Contract object to a dict containing any non-default values."""
        default = Contract()
        return {field: val for field, val in vars(contract).items() if val != getattr(default, field, None)}

    # -----------------------------------------
    @staticmethod
    def contract_to_tuple(contract):
        return (contract.symbol, contract.sec_type,
                contract.exchange, contract.currency, contract.expiry,
                contract.strike, contract.right)

    # -----------------------------------------
    def registerContract(self, contract):
        """ used for when callback receives a contract
        that isn't found in local database """

        if contract.exchange == "":
            return

        """
        if contract not in self.contracts.values():
            contract_tuple = self.contract_to_tuple(contract)
            self.createContract(contract_tuple)

        if self.tickerId(contract) not in self.contracts.keys():
            contract_tuple = self.contract_to_tuple(contract)
            self.createContract(contract_tuple)
        """

        if self.getConId(contract) == 0:
            contract_tuple = self.contract_to_tuple(contract)
            self.createContract(contract_tuple)

    # -----------------------------------------
    # Start event handlers
    # -----------------------------------------
    def handleErrorEvents(self, msg):
        """ logs error messages """
        self.log.error("[#%s] %s" % (msg['errorCode'], msg['errorMsg']))
        self.callbacks(caller="handleError", msg=msg)

    # -----------------------------------------
    def handleServerEvents(self, topic, data, msg=None):
        if isinstance(topic, str):
            if topic == "error":
                self.handleErrorEvents(msg)
            elif topic == "CONNECTION_CLOSED":
                self.handleConnectionClosed(msg)

        elif topic['type'] in [105, 106, 102]:
            tickdata = {'tickerId': topic['tickerId'], 'data': data}
            # mktdata = self.wb.get_quote(tId=topic['tickerId'])
            self.log.debug('MSG %s', msg)
            """ dispatch msg to the right handler """
            if topic['type'] == 105:
                self.handleTickPrice(msg=tickdata)
                # self.handleTickSize(msg=tickdata)
                # self.handleTickString(msg=tickdata)
            elif topic['type'] == 106:
                self.handleMarketDepth(msg=tickdata)

        elif topic['type'] == 'ohlc':
            self.handleHistoricalData(msg=data, tickerId=topic['tickerId'], completed=topic['completed'])

        elif topic['type'] == 'quote':
            quote_data = {'tickerId': data['tickerId'], 'data': data}
            self.handleTickPrice(msg=quote_data)

    # -----------------------------------------
    # generic callback function - can be used externally
    # -----------------------------------------
    def callbacks(self, caller, msg, **kwargs):
        pass

    # -----------------------------------------
    # Start admin handlers
    # -----------------------------------------
    def handleConnectionState(self, msg):
        self.connected = not (msg.typeName == "error")

        if self.connected:
            self.connection_tracking["errors"] = []
            self.connection_tracking["disconnected"] = False

            if msg.typeName is not (self.connection_tracking["connected"]):
                self.log.info("[CONNECTION TO WEBULL ESTABLISHED]")
                self.connection_tracking["connected"] = True
                self.callbacks(caller="handleConnectionOpened", msg="<connectionOpened>")
        else:
            self.connection_tracking["connected"] = False

            if not self.connection_tracking["disconnected"]:
                self.connection_tracking["disconnected"] = True
                self.log.info("[CONNECTION TO WEBULL LOST]")

    # -----------------------------------------
    def handleConnectionClosed(self, msg):
        self.connected = False
        self.started = False
        self.callbacks(caller="handleConnectionClosed", msg=msg)

        # retry to connect
        # if not self._disconnected_by_user:
        #     self.reconnect()

    # -----------------------------------------

    def handleContractDetails(self, msg, end=False):
        """ handles contractDetails and contractDetailsEnd """

        if end:
            # mark as downloaded
            self._contract_details[msg.reqId]['downloaded'] = True
            self._contract_details[msg.reqId]['tickerId'] = msg.reqId

            # move details from temp to permanent collector
            self.contract_details[msg.reqId] = self._contract_details[msg.reqId]
            del self._contract_details[msg.reqId]

            # adjust fields if multi contract
            if len(self.contract_details[msg.reqId]["contracts"]) > 1:
                self.contract_details[msg.reqId]["m_contractMonth"] = ""
                # m_summary should hold closest expiration
                expirations = self.getExpirations(self.contracts[msg.reqId], expired=0)
                contract = self.contract_details[msg.reqId]["contracts"][-len(expirations)]
                self.contract_details[msg.reqId]["m_summary"] = vars(contract)
            else:
                self.contract_details[msg.reqId]["m_summary"] = vars(
                    self.contract_details[msg.reqId]["contracts"][0])

            # update local db with correct contractString
            for tid in self.contract_details:
                oldString = self.tickerIds[tid]
                newString = self.contractString(self.contract_details[tid]["contracts"][0])

                if len(self.contract_details[msg.reqId]["contracts"]) > 1:
                    self.tickerIds[tid] = newString
                    if newString != oldString:
                        if oldString in self._portfolios:
                            self._portfolios[newString] = self._portfolios[oldString]
                        if oldString in self._positions:
                            self._positions[newString] = self._positions[oldString]

            # fire callback
            self.callbacks(caller="handleContractDetailsEnd", msg=msg)

            # exit
            return

        # continue...

        # collect data on all contract details
        # (including those with multiple expiry/strike/sides)
        details = vars(msg.contractDetails)
        contract = details["m_summary"]

        if msg.reqId in self._contract_details:
            details['contracts'] = self._contract_details[msg.reqId]["contracts"]
        else:
            details['contracts'] = []

        details['contracts'].append(contract)
        details['downloaded'] = False
        self._contract_details[msg.reqId] = details

        # add details to local symbol list
        if contract.m_localSymbol not in self.localSymbolExpiry:
            self.localSymbolExpiry[contract.m_localSymbol] = details["m_contractMonth"]

        # add contract's multiple expiry/strike/sides to class collectors
        contractString = self.contractString(contract)
        tickerId = self.tickerId(contractString)
        self.contracts[tickerId] = contract

        # continue if this is a "multi" contract
        if tickerId == msg.reqId:
            self._contract_details[msg.reqId]["m_summary"] = vars(contract)
        else:
            # print("+++", tickerId, contractString)
            self.contract_details[tickerId] = details.copy()
            self.contract_details[tickerId]["m_summary"] = vars(contract)
            self.contract_details[tickerId]["contracts"] = [contract]

        # fire callback
        self.callbacks(caller="handleContractDetails", msg=msg)

    # -----------------------------------------
    # Account handling
    # -----------------------------------------
    def handleAccount(self, msg):
        """
        handle account info update
        Obsolete.
        """

        # parse value
        try:
            msg.value = float(msg.value)
        except Exception:
            msg.value = msg.value
            if msg.value in ['true', 'false']:
                msg.value = (msg.value == 'true')

        try:
            # log handler msg
            self.log_msg("account", msg)

            # new account?
            if msg.accountName not in self._accounts.keys():
                self._accounts[msg.accountName] = {}

            # set value
            self._accounts[msg.accountName][msg.key] = msg.value

            # fire callback
            self.callbacks(caller="handleAccount", msg=msg)
        except Exception:
            pass

    def _get_active_account(self, account):
        account = None if account == "" else None
        if account is None:
            if self.default_account is not None:
                return self.default_account
            elif len(self._accounts) > 0:
                return self.accountCodes[0]
        return account

    @property
    def accounts(self):
        return self._accounts

    @property
    def account(self):
        return self.getAccount()

    @property
    def accountCodes(self):
        return list(self._accounts.keys())

    @property
    def accountCode(self):
        return self.accountCodes[0]

    def getAccount(self, account=None):
        if len(self._accounts) == 0:
            return {}

        account = self._get_active_account(account)

        if account is None:
            if len(self._accounts) > 1:
                raise ValueError("Must specify account number as multiple accounts exists.")
            return self._accounts[list(self._accounts.keys())[0]]

        if account in self._accounts:
            return self._accounts[account]

        raise ValueError("Account %s not found in account list" % account)

    # -----------------------------------------
    # Position handling
    # -----------------------------------------
    def handlePosition(self, msg):
        """ handle positions changes """

        # log handler msg
        self.log_msg("position", msg)

        # contract identifier
        contract_tuple = self.contract_to_tuple(msg.contract)
        contractString = self.contractString(contract_tuple)

        # try creating the contract
        self.registerContract(msg.contract)

        # new account?
        if msg.account not in self._positions.keys():
            self._positions[msg.account] = {}

        # if msg.pos != 0 or contractString in self.contracts.keys():
        self._positions[msg.account][contractString] = {
            "symbol": contractString,
            "position": int(msg.pos),
            "avgCost": float(msg.avgCost),
            "account": msg.account
        }

        # fire callback
        self.callbacks(caller="handlePosition", msg=msg)

    @property
    def positions(self):
        return self.getPositions()

    def getPositions(self, account=None):
        if len(self._positions) == 0:
            return {}

        account = self._get_active_account(account)

        if account is None:
            if len(self._positions) > 1:
                raise ValueError("Must specify account number as multiple accounts exists.")
            return self._positions[list(self._positions.keys())[0]]

        if account in self._positions:
            return self._positions[account]

        raise ValueError("Account %s not found in account list" % account)

    # -----------------------------------------
    # Portfolio handling
    # -----------------------------------------
    def handlePortfolio(self, msg):
        """ handle portfolio updates """

        # log handler msg
        self.log_msg("portfolio", msg)

        # contract identifier
        contract_tuple = self.contract_to_tuple(msg.contract)
        contractString = self.contractString(contract_tuple)

        # try creating the contract
        self.registerContract(msg.contract)

        # new account?
        if msg.accountName not in self._portfolios.keys():
            self._portfolios[msg.accountName] = {}

        self._portfolios[msg.accountName][contractString] = {
            "symbol": contractString,
            "position": int(msg.position),
            "marketPrice": float(msg.marketPrice),
            "marketValue": float(msg.marketValue),
            "averageCost": float(msg.averageCost),
            "unrealizedPNL": float(msg.unrealizedPNL),
            "realizedPNL": float(msg.realizedPNL),
            "totalPNL": float(msg.realizedPNL) + float(msg.unrealizedPNL),
            "account": msg.accountName
        }

        # fire callback
        self.callbacks(caller="handlePortfolio", msg=msg)

    @property
    def portfolios(self):
        return self._portfolios

    @property
    def portfolio(self):
        return self.getPortfolio()

    def getPortfolio(self, account=None):
        if len(self._portfolios) == 0:
            return {}

        account = self._get_active_account(account)

        if account is None:
            if len(self._portfolios) > 1:
                raise ValueError("Must specify account number as multiple accounts exists.")
            return self._portfolios[list(self._portfolios.keys())[0]]

        if account in self._portfolios:
            return self._portfolios[account]

        raise ValueError("Account %s not found in account list" % account)

    # -----------------------------------------
    # Order handling
    # -----------------------------------------
    def handleOrders(self, msg):
        """ handle order open & status """
        raise Exception("Not supported!")

    # -----------------------------------------
    def _assgin_order_to_account(self, order):
        # assign order to account_orders dict
        raise Exception("Not supported!")

    # -----------------------------------------
    def getOrders(self, account=None):
        raise Exception("Not supported!")

    # -----------------------------------------
    def group_orders(self, by="symbol", account=None):
        raise Exception("Not supported!")

    # -----------------------------------------
    # Start price handlers
    # -----------------------------------------
    def handleMarketDepth(self, msg):
        """
        topic: {'type': 106, 'tickerId': 913295125, 'modules': ['']} ----- payload: {'transId': 1382128, 'askList': [
        {'price': '179.85', 'volume': '20165'}, {}, {}, {}, {}, {}, {}, {}, {}, {}], 'pubId': 30947, 'tickerId':
        913295125, 'trdSeq': 15691, 'status': 'T', 'bidList': [{'price': '179.75', 'volume': '488'}, {}, {}, {}, {},
        {}, {}, {}, {}, {}]}
        :param msg:
        :return:
        """
        tickerId = msg['tickerId']
        data = msg['data']
        # make sure symbol exists
        if tickerId not in self.marketDepthData.keys():
            self.marketDepthData[tickerId] = self.marketDepthData[0].copy()

        # bid
        if 'bidList' in data:
            self.marketDepthData[tickerId].loc[0, "bid"] = data['bidList'][0]['price'] \
                if 'price' in data['bidList'][0] else 0
            self.marketDepthData[tickerId].loc[0, "bidsize"] = data['bidList'][0]['volume'] \
                if 'volume' in data['bidList'][0] else 0
        # ask
        if 'askList' in data:
            self.marketDepthData[tickerId].loc[0, "ask"] = data['askList'][0]['price'] \
                if 'price' in data['askList'][0] else 0
            self.marketDepthData[tickerId].loc[0, "asksize"] = data['askList'][0]['volume'] \
                if 'volume' in data['askList'][0] else 0

        """
        # bid/ask spread / vol diff
        self.marketDepthData[msg.tickerId].loc[msg.position, "spread"] = \
            self.marketDepthData[msg.tickerId].loc[msg.position, "ask"]-\
            self.marketDepthData[msg.tickerId].loc[msg.position, "bid"]

        self.marketDepthData[msg.tickerId].loc[msg.position, "spreadsize"] = \
            self.marketDepthData[msg.tickerId].loc[msg.position, "asksize"]-\
            self.marketDepthData[msg.tickerId].loc[msg.position, "bidsize"]
        """

        self.callbacks(caller="handleMarketDepth", msg=msg)

    # -----------------------------------------
    def handleHistoricalData(self, msg, tickerId, completed):
        # self.log.debug("[HISTORY]: %s", msg)
        print('.', end="", flush=True)

        # hist_rows = DataFrame(index=['datetime'], data={
        #     "datetime": ts, "O": msg.open, "H": msg.high,
        #     "L": msg.low, "C": msg.close, "V": msg.volume,
        #     "OI": msg.count, "WAP": msg.WAP})
        hist_rows = msg.copy()
        hist_rows.index.names = ['datetime']
        # hist_rows.columns = ['O', 'H', 'L', 'C', 'V', 'WAP']
        # hist_rows.set_index('datetime', inplace=True)

        symbol = self.tickerSymbol(tickerId)
        if symbol not in self.historicalData.keys():
            self.historicalData[symbol] = hist_rows
        else:
            try:
                self.historicalData[symbol] = self.historicalData[symbol].append(hist_rows, verify_integrity=True)
            except ValueError:
                self.log.info('discarded duplicate rows')
        # print(self.historicalData)
        if completed:
            # self.historicalData[symbol] = self.historicalData[symbol].drop_duplicates()

            if self.utc_history:
                for sym in self.historicalData:
                    contractString = str(sym)
                    self.historicalData[contractString] = utils.local_to_utc(self.historicalData[contractString])

            if self.csv_path is not None:
                for sym in self.historicalData:
                    contractString = str(sym)
                    self.log.info("[HISTORICAL DATA FOR %s DOWNLOADED]" % contractString)
                    self.historicalData[contractString].to_csv(
                        self.csv_path + contractString + '.csv'
                    )

            print('.')
        # fire callback
        self.callbacks(caller="handleHistoricalData", msg=msg, completed=completed, tickerId=tickerId)

    # -----------------------------------------
    def handleTickGeneric(self, msg):
        """
        holds latest tick bid/ask/last price
        """

        df2use = self.marketData
        if self.contracts[msg._tickerId].sec_type in ("OPT", "FOP"):
            df2use = self.optionsData

        # create tick holder for ticker
        if msg._tickerId not in df2use.keys():
            df2use[msg._tickerId] = df2use[0].copy()

        # if msg.tickType == TYPES["FIELD_OPTION_IMPLIED_VOL"]:
        #     df2use[msg.tickerId]['iv'] = round(float(msg.value), 2)

        # elif msg.tickType == TYPES["FIELD_OPTION_HISTORICAL_VOL"]:
        #     df2use[msg.tickerId]['historical_iv'] = round(float(msg.value), 2)

        # fire callback
        self.callbacks(caller="handleTickGeneric", msg=msg)

    # -----------------------------------------
    def handleTickPrice(self, msg):
        """
        holds latest tick bid/ask/last price
        """
        # self.log.debug("[TICK PRICE]: %s - %s", 'TICK_PRICE_CLOSE', msg['close'])
        # return
        # print(type(msg['pPrice']))
        tickerId = msg['tickerId']
        data = msg['data']

        # if int(data['close']) < 0:
        #     return

        df2use = self.marketData
        # canAutoExecute = msg.canAutoExecute == 1
        if self.contracts[tickerId].sec_type in ("OPT", "FOP"):
            df2use = self.optionsData
        #     canAutoExecute = True

        # create tick holder for ticker
        if tickerId not in df2use.keys():
            df2use[tickerId] = df2use[0].copy()

        df2use[tickerId]['tickerId'] = str(tickerId)

        # tick
        if 'deal' in data:
            # bid price
            if data['deal']['trdBs'] == 'B':
                df2use[tickerId]['buy'] = float(data['deal']['price'])
                df2use[tickerId]['buysize'] = int(data['deal']['volume'])
            # ask price
            if data['deal']['trdBs'] == 'S':
                df2use[tickerId]['sell'] = float(data['deal']['price'])
                df2use[tickerId]['sellsize'] = int(data['deal']['volume'])
            # last price
            df2use[tickerId]['lastsize'] = int(data['deal']['volume'])
            df2use[tickerId]['last'] = float(data['deal']['price'])

        elif 'close' in data:
            # last price
            df2use[tickerId]['last'] = float(data['close'])
            if 'volume' in data:
                df2use[tickerId]['lastsize'] = int(data['volume'])
            else:
                df2use[tickerId]['lastsize'] = 1

        if 'tradeTime' in data:
            ts = dateutil.parser.parse(data['tradeTime']) \
                .strftime(COMMON_TYPES["DATE_TIME_FORMAT_LONG_MILLISECS"])
            df2use[tickerId].index = [ts]
        else:
            ts = datetime.utcnow().strftime(COMMON_TYPES["DATE_TIME_FORMAT_LONG_MILLISECS"])
            df2use[tickerId].index = [ts]

        # fire callback
        self.callbacks(caller="handleTickPrice", msg=msg, tickerId=tickerId)

    # -----------------------------------------
    def handleTickSize(self, msg):
        """
        holds latest tick bid/ask/last size
        """
        tickerId = msg['tickerId']
        data = msg['data']

        df2use = self.marketData
        if self.contracts[tickerId].sec_type in ("OPT", "FOP"):
            df2use = self.optionsData

        # create tick holder for ticker
        if tickerId not in df2use.keys():
            df2use[tickerId] = df2use[0].copy()

        # ---------------------
        # market data
        # ---------------------
        # bid size
        if data['deal']['trdBs'] == 'B':
            df2use[tickerId]['bidsize'] = int(data['deal']['volume'])
        # ask size
        if data['deal']['trdBs'] == 'S':
            df2use[tickerId]['asksize'] = int(data['deal']['volume'])
        # last size
        # elif msg.field == TYPES["FIELD_LAST_SIZE"]:
        df2use[tickerId]['lastsize'] = int(data['deal']['volume'])

        # ---------------------
        # options data
        # ---------------------
        # open interest
        # elif msg.field == TYPES["FIELD_OPEN_INTEREST"]:
        #     df2use[msg.tickerId]['oi'] = int(msg.size)

        # elif msg.field == TYPES["FIELD_OPTION_CALL_OPEN_INTEREST"] and \
        #         self.contracts[msg.tickerId].m_right == "CALL":
        #     df2use[msg.tickerId]['oi'] = int(msg.size)

        # elif msg.field == TYPES["FIELD_OPTION_PUT_OPEN_INTEREST"] and \
        #         self.contracts[msg.tickerId].m_right == "PUT":
        #     df2use[msg.tickerId]['oi'] = int(msg.size)

        # volume
        # elif msg.field == TYPES["FIELD_VOLUME"]:
        df2use[tickerId]['volume'] = int(data['volume'])

        # elif msg.field == TYPES["FIELD_OPTION_CALL_VOLUME"] and \
        #         self.contracts[msg.tickerId].m_right == "CALL":
        #     df2use[msg.tickerId]['volume'] = int(msg.size)

        # elif msg.field == TYPES["FIELD_OPTION_PUT_VOLUME"] and \
        #         self.contracts[msg.tickerId].m_right == "PUT":
        #     df2use[msg.tickerId]['volume'] = int(msg.size)

        # fire callback
        self.callbacks(caller="handleTickSize", msg=msg)

    # -----------------------------------------
    def handleTickString(self, msg):
        """
        holds latest tick bid/ask/last timestamp
        """
        tickerId = msg['tickerId']
        data = msg['data']

        df2use = self.marketData
        if self.contracts[tickerId].sec_type in ("OPT", "FOP"):
            df2use = self.optionsData

        # create tick holder for ticker
        if tickerId not in df2use.keys():
            df2use[tickerId] = df2use[0].copy()

            # update timestamp
            # if msg.tickType == TYPES["FIELD_LAST_TIMESTAMP"]:
        ts = dateutil.parser.parse(data['tradeTime']) \
            .strftime(COMMON_TYPES["DATE_TIME_FORMAT_LONG_MILLISECS"])
        df2use[tickerId].index = [ts]
        # self.log.debug("[TICK TS]: %s", ts)

        # handle trailing stop orders
        # if self.contracts[msg.tickerId].m_secType not in ("OPT", "FOP"):
        #     self.triggerTrailingStops(msg.tickerId)
        #     self.handleTrailingStops(msg.tickerId)

        # fire callback
        self.callbacks(caller="handleTickString", msg=msg)

        # elif (msg.tickType == TYPES["FIELD_RTVOLUME"]):
        #
        #     # log handler msg
        #     # self.log_msg("rtvol", msg)
        #
        #     tick = dict(TYPES["RTVOL_TICKS"])
        #     (tick['price'], tick['size'], tick['time'], tick['volume'],
        #      tick['wap'], tick['single']) = msg.value.split(';')
        #
        #     try:
        #         tick['last'] = float(tick['price'])
        #         tick['lastsize'] = float(tick['size'])
        #         tick['volume'] = float(tick['volume'])
        #         tick['wap'] = float(tick['wap'])
        #         tick['single'] = tick['single'] == 'true'
        #         tick['instrument'] = self.tickerSymbol(msg.tickerId)
        #
        #         # parse time
        #         s, ms = divmod(int(tick['time']), 1000)
        #         tick['time'] = '{}.{:03d}'.format(
        #             time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(s)), ms)
        #
        #         # add most recent bid/ask to "tick"
        #         tick['bid'] = df2use[msg.tickerId]['bid'][0]
        #         tick['bidsize'] = int(df2use[msg.tickerId]['bidsize'][0])
        #         tick['ask'] = df2use[msg.tickerId]['ask'][0]
        #         tick['asksize'] = int(df2use[msg.tickerId]['asksize'][0])
        #
        #         # self.log.debug("%s: %s\n%s", tick['time'], self.tickerSymbol(msg.tickerId), tick)
        #
        #         # fire callback
        #         self.ibCallback(caller="handleTickString", msg=msg, tick=tick)
        #
        #     except Exception:
        #         pass

        # else:
        #     # self.log.info("tickString-%s", msg)
        #     # fire callback
        #     self.ibCallback(caller="handleTickString", msg=msg)

        # print(msg)

    # -----------------------------------------
    def handleTickOptionComputation(self, msg):
        """
        holds latest option data timestamp
        only option price is kept at the moment
        https://www.interactivebrokers.com/en/software/api/apiguide/java/tickoptioncomputation.htm
        """

        def calc_generic_val(data, field):
            last_val = data['last_' + field].values[-1]
            bid_val = data['bid_' + field].values[-1]
            ask_val = data['ask_' + field].values[-1]
            bid_ask_val = last_val
            if bid_val != 0 and ask_val != 0:
                bid_ask_val = (bid_val + ask_val) / 2
            return max([last_val, bid_ask_val])

        def valid_val(val):
            return float(val) if val < 1000000000 else None

        # create tick holder for ticker
        if msg._tickerId not in self.optionsData.keys():
            self.optionsData[msg._tickerId] = self.optionsData[0].copy()

        col_prepend = ""
        if msg.field == "FIELD_BID_OPTION_COMPUTATION":
            col_prepend = "bid_"
        elif msg.field == "FIELD_ASK_OPTION_COMPUTATION":
            col_prepend = "ask_"
        elif msg.field == "FIELD_LAST_OPTION_COMPUTATION":
            col_prepend = "last_"

        # save side
        self.optionsData[msg._tickerId][col_prepend + 'imp_vol'] = valid_val(msg.impliedVol)
        self.optionsData[msg._tickerId][col_prepend + 'dividend'] = valid_val(msg.pvDividend)
        self.optionsData[msg._tickerId][col_prepend + 'delta'] = valid_val(msg.delta)
        self.optionsData[msg._tickerId][col_prepend + 'gamma'] = valid_val(msg.gamma)
        self.optionsData[msg._tickerId][col_prepend + 'vega'] = valid_val(msg.vega)
        self.optionsData[msg._tickerId][col_prepend + 'theta'] = valid_val(msg.theta)
        self.optionsData[msg._tickerId][col_prepend + 'price'] = valid_val(msg.optPrice)

        # save generic/mid
        data = self.optionsData[msg._tickerId]
        self.optionsData[msg._tickerId]['imp_vol'] = calc_generic_val(data, 'imp_vol')
        self.optionsData[msg._tickerId]['dividend'] = calc_generic_val(data, 'dividend')
        self.optionsData[msg._tickerId]['delta'] = calc_generic_val(data, 'delta')
        self.optionsData[msg._tickerId]['gamma'] = calc_generic_val(data, 'gamma')
        self.optionsData[msg._tickerId]['vega'] = calc_generic_val(data, 'vega')
        self.optionsData[msg._tickerId]['theta'] = calc_generic_val(data, 'theta')
        self.optionsData[msg._tickerId]['price'] = calc_generic_val(data, 'price')
        self.optionsData[msg._tickerId]['underlying'] = valid_val(msg.undPrice)

        # fire callback
        self.callbacks(caller="handleTickOptionComputation", msg=msg)

    # -----------------------------------------
    # trailing stops
    # -----------------------------------------
    def createTriggerableTrailingStop(self, symbol, quantity=1,
                                      triggerPrice=0, trailPercent=100., trailAmount=0.,
                                      parentId=0, stopOrderId=None, targetOrderId=None,
                                      account=None, **kwargs):
        """
        adds order to triggerable list

        IMPORTANT! NOT SUPPORTED
        For trailing stop to work you'll need
            1. real time market data subscription for the tracked ticker
            2. the python/algo script to be kept alive
        """
        raise Exception("Not supported!")

    # -----------------------------------------
    def cancelTriggerableTrailingStop(self, symbol):
        """ cancel **pending** triggerable trailing stop """
        raise Exception("Not supported!")

    # -----------------------------------------
    def modifyTriggerableTrailingStop(self, symbol, quantity=1,
                                      triggerPrice=0, trailPercent=100., trailAmount=0.,
                                      parentId=0, stopOrderId=None, targetOrderId=None, **kwargs):

        raise Exception("Not supported!")

    # -----------------------------------------
    def registerTrailingStop(self, tickerId, orderId=0, quantity=1,
                             lastPrice=0, trailPercent=100., trailAmount=0., parentId=0, **kwargs):
        """ adds trailing stop to monitor list """
        raise Exception("Not supported!")

    # -----------------------------------------
    def modifyStopOrder(self, orderId, parentId, newStop, quantity,
                        transmit=True, stop_limit=False, account=None):
        """ modify stop order """
        raise Exception("Not supported!")

    # -----------------------------------------
    def handleTrailingStops(self, tickerId):
        """ software-based trailing stop """

        raise Exception("Not supported!")

    # -----------------------------------------
    def triggerTrailingStops(self, tickerId, **kwargs):
        """ trigger waiting trailing stops """
        raise Exception("Not supported!")

    # -----------------------------------------
    # tickerId/Symbols constructors
    # -----------------------------------------
    def tickerId(self, contract_identifier):
        """
        returns the tickerId for the symbol or
        sets one if it doesn't exits
        """
        # contract passed instead of symbol?
        symbol = contract_identifier
        if isinstance(symbol, Contract):
            symbol = self.contractString(symbol)

        for tickerId in self.tickerIds:
            if symbol == self.tickerIds[tickerId]:
                return tickerId
        else:
            tickerId = self.wb.get_ticker(stock=symbol)  # len(self.tickerIds)
            self.tickerIds[tickerId] = symbol
            return tickerId

    # -----------------------------------------
    def tickerSymbol(self, tickerId):
        """ returns the symbol of a tickerId """
        try:
            return self.tickerIds[tickerId]
        except Exception:
            return ""

    # -----------------------------------------
    def contractString(self, contract, seperator="_"):
        """ returns string from contract tuple """

        contractTuple = contract

        if type(contract) != tuple:
            contractTuple = self.contract_to_tuple(contract)

        # build identifier
        try:
            if contractTuple[1] in ("OPT", "FOP"):
                # if contractTuple[5]*100 - int(contractTuple[5]*100):
                #     strike = contractTuple[5]
                # else:
                #     strike = "{0:.2f}".format(contractTuple[5])
                strike = '{:0>5d}'.format(int(contractTuple[5])) + \
                         format(contractTuple[5], '.3f').split('.')[1]

                contractString = (contractTuple[0] + str(contractTuple[4]) +
                                  contractTuple[6][0] + strike, contractTuple[1])
                # contractTuple[6], str(strike).replace(".", ""))

            elif contractTuple[1] == "FUT":
                exp = str(contractTuple[4])[:6]
                exp = COMMON_TYPES["MONTH_CODES"][int(exp[4:6])] + exp[:4]
                contractString = (contractTuple[0] + exp, contractTuple[1])

            elif contractTuple[1] == "CASH":
                contractString = (contractTuple[0] + contractTuple[3], contractTuple[1])

            else:  # STK
                contractString = (contractTuple[0], contractTuple[1] or "STK")

            # construct string
            contractString = seperator.join(
                str(v) for v in contractString).replace(seperator + "STK", "")

        except Exception:
            contractString = contractTuple[0]

        return contractString.replace(" ", "_").upper()

    # -----------------------------------------
    def contractDetails(self, contract_identifier):
        """ returns string from contract tuple """

        if isinstance(contract_identifier, Contract):
            tickerId = self.tickerId(contract_identifier)
        else:
            if str(contract_identifier).isdigit():
                tickerId = contract_identifier
            else:
                tickerId = self.tickerId(contract_identifier)

        if tickerId in self.contract_details:
            return self.contract_details[tickerId]
        elif tickerId in self._contract_details:
            return self._contract_details[tickerId]

        # default values
        return {
            'tickerId': 0,
            'm_category': None, 'm_contractMonth': '', 'downloaded': False, 'm_evMultiplier': 0,
            'm_evRule': None, 'm_industry': None, 'm_liquidHours': '', 'm_longName': '',
            'm_marketName': '', 'm_minTick': 0.05, 'm_orderTypes': '', 'm_priceMagnifier': 0,
            'm_subcategory': None, 'm_timeZoneId': '', 'm_tradingHours': '', 'm_underConId': 0,
            'm_validExchanges': 'NSE', 'contracts': [Contract()], 'm_summary': {
                'm_conId': 0, 'currency': 'USD', 'exchange': 'NSE', 'expiry': '',
                'm_includeExpired': False, 'm_localSymbol': '', 'm_multiplier': '',
                'm_primaryExch': None, 'right': None, 'sec_type': '',
                'strike': 0.0, 'symbol': '', 'm_tradingClass': '',
            }
        }

    # -----------------------------------------
    # contract constructors
    # -----------------------------------------
    def isMultiContract(self, contract):
        """ tells if is this contract has sub-contract with expiries/strikes/sides """
        if contract.sec_type == "FUT" and contract.expiry == "":
            return True

        if contract.sec_type in ["OPT", "FOP"] and \
                (contract.expiry == "" or contract.strike == "" or contract.right == ""):
            return True

        tickerId = self.tickerId(contract)
        if tickerId in self.contract_details and \
                len(self.contract_details[tickerId]["contracts"]) > 1:
            return True

        return False

    # -----------------------------------------
    def createContract(self, contractTuple, **kwargs):
        # https://www.interactivebrokers.com/en/software/api/apiguide/java/contract.htm

        contractString = self.contractString(contractTuple)

        self.log.info("getting contract details for sym {}".format(contractString))
        # get (or set if not set) the tickerId for this symbol
        # tickerId = self.tickerId(contractTuple[0])
        tickerId = self.tickerId(contractString)

        # construct contract
        exchange = contractTuple[2]
        if exchange is not None:
            exchange = exchange.upper()
        newContract = Contract()
        newContract.symbol = contractTuple[0]
        newContract.sec_type = contractTuple[1]
        newContract.exchange = exchange
        newContract.currency = contractTuple[3]
        newContract.expiry = contractTuple[4]
        newContract.strike = contractTuple[5]
        newContract.right = contractTuple[6]

        if len(contractTuple) == 8:
            newContract.m_multiplier = contractTuple[7]

        # include expired (needed for historical data)
        newContract.m_includeExpired = (newContract.sec_type in ["FUT", "OPT", "FOP"])

        if "comboLegs" in kwargs:
            newContract.m_comboLegs = kwargs["comboLegs"]

        # add contract to pool
        self.contracts[tickerId] = newContract

        # request contract details
        if "comboLegs" not in kwargs:
            try:
                self.requestContractDetails(newContract)
                time.sleep(1.5 if self.isMultiContract(newContract) else 0.5)
            except KeyboardInterrupt:
                sys.exit()

        # print(vars(newContract))
        # print('Contract Values:%s,%s,%s,%s,%s,%s,%s:' % contractTuple)
        return newContract

    # shortcuts
    # -----------------------------------------
    def createStockContract(self, symbol, currency="INR", exchange="NSE"):
        contract_tuple = (symbol, "STK", exchange, currency, "", 0.0, "")
        contract = self.createContract(contract_tuple)
        return contract

    # -----------------------------------------
    def createFuturesContract(self, symbol, currency="INR", expiry=None,
                              exchange="NSE", multiplier=""):
        if symbol[0] == "@":
            return self.createContinuousFuturesContract(symbol[1:], exchange)

        expiry = [expiry] if not isinstance(expiry, list) else expiry

        contracts = []
        for fut_expiry in expiry:
            contract_tuple = (symbol, "FUT", exchange, currency,
                              fut_expiry, 0.0, "", multiplier)
            contract = self.createContract(contract_tuple)
            contracts.append(contract)

        return contracts[0] if len(contracts) == 1 else contracts

    # -----------------------------------------
    def createContinuousFuturesContract(self, symbol, exchange="NSE",
                                        output="contract", is_retry=False):

        contfut_contract = self.createContract((
            symbol, "CONTFUT", exchange, '', '', '', ''))

        # wait max 250ms for contract details
        for x in range(25):
            time.sleep(0.01)
            contfut = self.contractDetails(contfut_contract)
            if contfut["tickerId"] != 0 and contfut["m_summary"]["m_conId"] != 0:
                break

        # if contfut["m_summary"]["m_conId"] == 0:
        #     contfut = self.contractDetails(contfut["contracts"][0])

        # can't find contract? retry and if still fails - raise error
        if contfut["m_summary"]["m_conId"] == 0:
            # print(symbol, contfut["m_summary"]["m_conId"])
            if not is_retry:
                return self.createContinuousFuturesContract(
                    symbol, exchange, output, True)
            raise ValueError("Can't find a valid Contract using this "
                             "combination (%s/%s)" % (symbol, exchange))

        # delete continuous placeholder
        tickerId = contfut["tickerId"]
        del self.contracts[tickerId]
        del self.contract_details[tickerId]

        # create futures contract
        expiry = contfut["m_contractMonth"]
        currency = contfut["m_summary"]["m_currency"]
        multiplier = int(contfut["m_summary"]["m_multiplier"])

        if output == "tuple":
            return (symbol, "FUT", exchange, currency,
                    expiry, 0.0, "", multiplier)

        return self.createFuturesContract(
            symbol, currency, expiry, exchange, multiplier)

    # -----------------------------------------
    def createOptionContract(self, symbol, expiry=None, strike=0.0, otype="CALL",
                             currency="INR", secType="OPT", exchange="NSE"):

        # secType = OPT (Option) / FOP (Options on Futures)
        expiry = [expiry] if not isinstance(expiry, list) else expiry
        strike = [strike] if not isinstance(strike, list) else strike
        otype = [otype] if not isinstance(otype, list) else otype

        contracts = []
        for opt_expiry in expiry:
            for opt_strike in strike:
                for opt_otype in otype:
                    contract_tuple = (symbol, secType, exchange, currency,
                                      opt_expiry, opt_strike, opt_otype)
                    contract = self.createContract(contract_tuple)
                    contracts.append(contract)

        return contracts[0] if len(contracts) == 1 else contracts

    # -----------------------------------------
    def createCashContract(self, symbol, currency="USD", exchange="IDEALPRO"):
        """ Used for FX, etc:
        createCashContract("EUR", currency="USD")
        """
        contract_tuple = (symbol, "CASH", exchange, currency, "", 0.0, "")
        contract = self.createContract(contract_tuple)
        return contract

    # -----------------------------------------
    def createIndexContract(self, symbol, currency="INR", exchange="NSE"):
        """ Used for indexes (SPX, DJX, ...) """
        contract_tuple = (symbol, "IND", exchange, currency, "", 0.0, "")
        contract = self.createContract(contract_tuple)
        return contract

    # -----------------------------------------
    # order constructors
    # -----------------------------------------
    def createOrder(self, quantity, price=0., stop=0., tif="DAY",
                    fillorkill=False, iceberg=False, transmit=True, rth=False,
                    account=None, **kwargs):

        raise Exception("Not supported!")

    # -----------------------------------------
    def createTargetOrder(self, quantity, parentId=0,
                          target=0., orderType=None, transmit=True, group=None, tif="DAY",
                          rth=False, account=None):
        """ Creates TARGET order """
        raise Exception("Not supported!")

    # -----------------------------------------
    def createStopOrder(self, quantity, parentId=0, stop=0., trail=None,
                        transmit=True, trigger=None, group=None, stop_limit=False,
                        rth=False, tif="DAY", account=None, **kwargs):

        """ Creates STOP order """
        raise Exception("Not supported!")

    # -----------------------------------------
    def createTrailingStopOrder(self, contract, quantity,
                                parentId=0, trailType='percent', trailValue=100.,
                                group=None, stopTrigger=None, account=None, **kwargs):

        """ convert hard stop order to trailing stop order """
        raise Exception("Not supported!")

    # -----------------------------------------
    def createBracketOrder(self, contract, quantity,
                           entry=0., target=0., stop=0.,
                           targetType=None, stopType=None,
                           trailingStop=False,  # (pct/amt/False)
                           trailingValue=None,  # value to train by (amt/pct)
                           trailingTrigger=None,  # (price where hard stop starts trailing)
                           group=None, tif="DAY",
                           fillorkill=False, iceberg=False, rth=False,
                           transmit=True, account=None, **kwargs):

        """
        creates One Cancels All Bracket Order
        """
        raise Exception("Not supported!")

    # -----------------------------------------
    def placeOrder(self, contract, order, orderId=None, account=None):
        """ Place order on IB TWS """

        # get latest order id before submitting an order
        self.requestOrderIds()
        # time.sleep(0.01)

        # make sure the price confirms to th contract
        raise Exception("Not supported!")

    # -----------------------------------------
    def cancelOrder(self, orderId):
        """ cancel order on IB TWS """
        raise Exception("Not supported!")

    # -----------------------------------------
    # data requesters
    # -----------------------------------------
    def requestOpenOrders(self):
        """
        Request open orders - loads up orders that wasn't created using this session
        """
        raise Exception("Not supported!")

    # -----------------------------------------
    def requestOrderIds(self, numIds=1):
        """
        Request the next valid ID that can be used when placing an order.
        Triggers the nextValidId() event, and the id returned is that next valid ID.
        """
        raise Exception("Not supported!")

    # -----------------------------------------
    def subscribeMarketDepth(self, contracts=None, num_rows=10):
        if contracts is None:
            contracts = list(self.contracts.values())
        elif not isinstance(contracts, list):
            contracts = [contracts]

        for contract in contracts:
            tickerId = self.tickerId(self.contractString(contract))
            self.streamConnection.subscribe(tId=tickerId, level=106)  # bid/ask list
            # self.streamConnection.subscribe(tId=tickerId, level=102)  # quotes

    # -----------------------------------------
    def cancelMarketDepth(self, contracts=None):
        """
        Cancel streaming market data for contract
        """
        if contracts == None:
            contracts = list(self.contracts.values())
        elif not isinstance(contracts, list):
            contracts = [contracts]

        for contract in contracts:
            tickerId = self.tickerId(self.contractString(contract))
            self.streamConnection.unsubscribe(tId=tickerId, level=106)

    # -----------------------------------------

    @asynctools.multitasking.task
    def requestMarketQuote(self, contracts=None):
        if contracts == None:
            contracts = list(self.contracts.values())
        elif not isinstance(contracts, list):
            contracts = [contracts]
        for contract in contracts:
            tickerId = self.tickerId(self.contractString(contract))
            quote = self.wb.get_quote(tId=tickerId)

            if float(quote['close']) < 0:
                return

            df2use = self.marketQuoteData

            if self.contracts[tickerId].sec_type in ("OPT", "FOP"):
                df2use = self.optionsData

            # create tick holder for ticker
            if tickerId not in df2use.keys():
                df2use[tickerId] = df2use[0].copy()

            # df2use[tickerId]['tickerId'] = str(tickerId)

            # quote
            # last price
            # df2use[tickerId]['lastsize'] = int(quote['volume'])
            # df2use[tickerId]['last'] = float(quote['close'])

            ts = dateutil.parser.parse(quote['tradeTime']) \
                .strftime(COMMON_TYPES["DATE_TIME_FORMAT_LONG_MILLISECS"])
            df2use[tickerId].index = [ts]

            df2use[tickerId]['high'] = float(quote['high'])
            df2use[tickerId]['low'] = float(quote['low'])
            df2use[tickerId]['open'] = float(quote['open'])
            df2use[tickerId]['close'] = float(quote['close'])

            if 'askList' in quote:
                df2use[tickerId]['ask'] = float(quote['askList'][0]['price'])
                df2use[tickerId]['asksize'] = int(quote['askList'][0]['volume'])
            if 'bidList' in quote:
                df2use[tickerId]['bid'] = float(quote['bidList'][0]['price'])
                df2use[tickerId]['bidsize'] = int(quote['bidList'][0]['volume'])

            df2use[tickerId]['volume'] = int(quote['volume'])
            # print(quote)
            self.callbacks(caller="handleMarketQuote", msg=quote, tickerId=tickerId)

    # -----------------------------------------
    def subscribeMarketData(self, contracts=None, snapshot=False):
        if contracts is None:
            contracts = list(self.contracts.values())
        elif not isinstance(contracts, list):
            contracts = [contracts]

        for contract in contracts:
            if not self.isMultiContract(contract):
                tickerId = self.tickerId(self.contractString(contract))
                self.streamConnection.subscribe(tId=tickerId)  # tick price

    # -------------------------------------------------
    def streamConnect(self):
        retry = 10
        while retry > 0:
            try:
                self.streamConnection.connect(did=self.wb._get_did())
                self.streamConnection.client_streaming_quotes.enable_logger()
                retry = 0
            except Exception:
                retry -= 1
                self.log.warning(f'StreamConnection failed. retrying {retry}')
                time.sleep(1)

    def _on_disconnect(self):
        def on_disconnect(client, user_data, rc):
            client.disconnect()
            client.loop_stop()
            self.handleConnectionClosed(user_data)

        return on_disconnect

    @asynctools.multitasking.task
    def stream(self):
        if not self.connected:
            raise Exception('client is not connected!')
        if self.started:
            raise Exception('Client has already started!')
        try:
            self.started = True
            # while self.started:
            self.streamConnection.client_streaming_quotes.on_disconnect = self._on_disconnect()
            self.streamConnection.run_blocking_loop()
        except KeyboardInterrupt:
            sys.exit()
        except Exception as e:
            self.streamConnection.client_streaming_quotes.disconnect()
            self.streamConnection.client_streaming_quotes.loop_stop()
            self.log.error(e)

    # -------------------------------------------------

    @asynctools.multitasking.task
    def run_quotes_request_loop(self):
        while self.started and self.connected:
            contracts = list(self.contracts.values())
            for contract in contracts:
                if not self.isMultiContract(contract):
                    tickerId = self.tickerId(self.contractString(contract))
                    quote = self.wb.get_quote(tId=tickerId)
                    self.handleServerEvents(topic={'type': 'quote'}, data=quote)
                    time.sleep(10)

    # -------------------------------------------------

    def cancelMarketDataSubscription(self, contracts=None):
        """
        Cancel streaming market data for contract
        """
        if contracts == None:
            contracts = list(self.contracts.values())

        elif not isinstance(contracts, list):
            contracts = [contracts]

        for contract in contracts:
            # tickerId = self.tickerId(contract.m_symbol)
            tickerId = self.tickerId(self.contractString(contract))
            self.streamConnection.unsubscribe(tId=tickerId)
        # self.reconnect()

    # -----------------------------------------
    def get_bars(self, tickerId, timestamp=None, lookback=1, interval='m1'):
        bars = self.wb.get_bars(
            tId=tickerId,
            timeStamp=timestamp,
            count=lookback,
            interval=interval,
            extendTrading=0
        )
        return bars

    # -----------------------------------------
    def requestHistoricalData(self, contracts=None, resolution="15",
                              lookback=800, end_datetime=None, rth=False,
                              csv_path=None, format_date=2, utc=False):

        """
        Download to historical data
        """

        self.csv_path = csv_path
        self.utc_history = utc

        if end_datetime is None:
            end_datetime = int(datetime.utcnow().timestamp())
        else:
            end_datetime = int(end_datetime)

        if contracts is None:
            contracts = list(self.contracts.values())

        if not isinstance(contracts, list):
            contracts = [contracts]
        lookback_copy = lookback

        for contract in contracts:
            # tickerId = self.tickerId(contract.m_symbol)
            tickerId = self.tickerId(self.contractString(contract))
            timestamp = end_datetime
            while int(lookback) > 0:
                ohlc = self.wb.get_bars(
                    tId=tickerId,
                    timeStamp=timestamp,
                    count=lookback,
                    interval='m' + str(resolution),
                    extendTrading=int(rth)
                )

                lookback = lookback - len(ohlc.index)
                timestamp = int(ohlc.iloc[0].name.timestamp()) - 1

                if lookback <= 0:
                    self.handleServerEvents(topic={'type': 'ohlc', 'tickerId': tickerId, 'completed': True}, data=ohlc)
                else:
                    self.handleServerEvents(topic={'type': 'ohlc', 'tickerId': tickerId, 'completed': False}, data=ohlc)
            lookback = lookback_copy

    def cancelHistoricalData(self, contracts=None):
        """ cancel historical data stream """
        if contracts == None:
            contracts = list(self.contracts.values())
        elif not isinstance(contracts, list):
            contracts = [contracts]

        for contract in contracts:
            # tickerId = self.tickerId(contract.m_symbol)
            tickerId = self.tickerId(self.contractString(contract))
            self.wb.cancelHistoricalData(tickerId=tickerId)  # TODO

    # -----------------------------------------
    def requestPositionUpdates(self, subscribe=True):
        """ Request/cancel request real-time position data for all accounts. """
        raise Exception("Not supported!")

    # -----------------------------------------
    def requestAccountUpdates(self, subscribe=True):
        """
        Register to account updates
        """
        raise Exception("Not supported!")

    # -----------------------------------------
    def requestContractDetails(self, contract):
        """
        Get contract details
        """
        return self.wb.get_quote(tId=self.tickerId(contract))

    # -----------------------------------------
    def getConId(self, contract_identifier):
        """ Get contracts conId """
        details = self.contractDetails(contract_identifier)
        if len(details["contracts"]) > 1:
            return details["m_underConId"]
        return details["m_summary"]["m_conId"]

    # -----------------------------------------
    # combo orders
    # -----------------------------------------
    def createComboLeg(self, contract, action, ratio=1, exchange=None):
        """ create combo leg
        https://www.interactivebrokers.com/en/software/api/apiguide/java/comboleg.htm
        """
        leg = ComboLeg()

        loops = 0
        conId = 0
        while conId == 0 and loops < 100:
            conId = self.getConId(contract)
            loops += 1
            time.sleep(0.05)

        leg.m_conId = conId
        leg.m_ratio = abs(ratio)
        leg.m_action = action
        leg.m_exchange = contract.exchange if exchange is None else exchange
        leg.m_openClose = 0
        leg.m_shortSaleSlot = 0
        leg.m_designatedLocation = ""

        return leg

    # -----------------------------------------
    def createComboContract(self, symbol, legs, currency="USD", exchange=None):
        """ Used for ComboLegs. Expecting list of legs """
        exchange = legs[0].m_exchange if exchange is None else exchange
        contract_tuple = (symbol, "BAG", exchange, currency, "", 0.0, "")
        contract = self.createContract(contract_tuple, comboLegs=legs)
        return contract

    # -----------------------------------------
    def getStrikes(self, contract_identifier, smin=None, smax=None):
        """ return strikes of contract / "multi" contract's contracts """
        strikes = []
        contracts = self.contractDetails(contract_identifier)["contracts"]

        if contracts[0].sec_type not in ("FOP", "OPT"):
            return []

        # collect expirations
        for contract in contracts:
            strikes.append(contract.strike)

        # convert to floats
        strikes = list(map(float, strikes))
        # strikes = list(set(strikes))

        # get min/max
        if smin is not None or smax is not None:
            smin = smin if smin is not None else 0
            smax = smax if smax is not None else 1000000000
            srange = list(set(range(smin, smax, 1)))
            strikes = [n for n in strikes if n in srange]

        strikes.sort()
        return tuple(strikes)

    # -----------------------------------------
    def getExpirations(self, contract_identifier, expired=0):
        """ return expiration of contract / "multi" contract's contracts """
        expirations = []
        contracts = self.contractDetails(contract_identifier)["contracts"]

        if contracts[0].sec_type not in ("FUT", "FOP", "OPT"):
            return []

        # collect expirations
        for contract in contracts:
            expirations.append(contract.expiry)

        # convert to ints
        expirations = list(map(int, expirations))
        # expirations = list(set(expirations))

        # remove expired contracts
        today = int(datetime.now().strftime("%Y%m%d"))
        closest = min(expirations, key=lambda x: abs(x - today))
        expirations = expirations[expirations.index(closest) - expired:]

        return tuple(expirations)
