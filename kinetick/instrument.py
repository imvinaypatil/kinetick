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
import logging
from pandas import concat as pd_concat
from kinetick.bot import Bot
from kinetick.enums import PositionType
from kinetick.models import Position


# =============================================


class Instrument(str):
    """A string subclass that provides easy access to misc
    symbol-related methods and information.
    """

    parent = None
    tick_window = None
    bar_window = None

    _position = None

    bot = Bot()
    logger = logging.getLogger(__name__)

    # ---------------------------------------
    def _set_parent(self, parent):
        """ sets the parent object to communicate with """
        self.parent = parent

    # ---------------------------------------
    def _set_windows(self, ticks, bars):
        """ be aware of default windows """
        self.tick_window = ticks
        self.bar_window = bars

    # ---------------------------------------
    @staticmethod
    def _get_symbol_dataframe(df, symbol):
        try:
            # this produce a "IndexingError using Boolean Indexing" (on rare occasions)
            return df[(df['symbol'] == symbol) | (df['symbol_group'] == symbol)].copy()
        except Exception as e:
            df = pd_concat([df[df['symbol'] == symbol]], sort=True)
            df.loc[:, '_idx_'] = df.index
            return df.drop_duplicates(subset=['_idx_'], keep='last').drop('_idx_', axis=1)

    # ---------------------------------------
    def get_bars(self, lookback=None, as_dict=False):
        """ Get bars for this instrument

        :Parameters:
            lookback : int
                Max number of bars to get (None = all available bars)
            as_dict : bool
                Return a dict or a pd.DataFrame object

        :Retruns:
            bars : pd.DataFrame / dict
                The bars for this instruments
        """
        bars = self._get_symbol_dataframe(self.parent.bars, self)

        # add signal history to bars
        bars = self.parent._add_signal_history(df=bars, symbol=self)

        lookback = self.bar_window if lookback is None else lookback
        bars = bars[-lookback:]
        # if lookback is not None:
        #     bars = bars[-lookback:]

        if not bars.empty > 0:  # and bars['asset_class'].values[-1] not in ("OPT", "FOP")
            bars.drop(bars.columns[
                          bars.columns.str.startswith('opt_')].tolist(),
                      inplace=True, axis=1)

        if as_dict:
            bars.loc[:, 'datetime'] = bars.index
            bars = bars.to_dict(orient='records')
            if lookback == 1:
                bars = None if not bars else bars[0]

        return bars

    # ---------------------------------------
    def get_bar(self):
        """ Shortcut to self.get_bars(lookback=1, as_dict=True) """
        return self.get_bars(lookback=1, as_dict=True)

    # ---------------------------------------
    def get_ticks(self, lookback=None, as_dict=False):
        """ Get ticks for this instrument

        :Parameters:
            lookback : int
                Max number of ticks to get (None = all available ticks)
            as_dict : bool
                Return a dict or a pd.DataFrame object

        :Retruns:
            ticks : pd.DataFrame / dict
                The ticks for this instruments
        """
        ticks = self._get_symbol_dataframe(self.parent.ticks, self)

        lookback = self.tick_window if lookback is None else lookback
        ticks = ticks[-lookback:]
        # if lookback is not None:
        #     ticks = ticks[-lookback:]

        if not ticks.empty and ticks['asset_class'].values[-1] not in ("OPT", "FOP"):
            ticks.drop(ticks.columns[
                           ticks.columns.str.startswith('opt_')].tolist(),
                       inplace=True, axis=1)

        if as_dict:
            ticks.loc[:, 'datetime'] = ticks.index
            ticks = ticks.to_dict(orient='records')
            if lookback == 1:
                ticks = None if not ticks else ticks[0]

        return ticks

    # ---------------------------------------
    def get_tick(self):
        """ Shortcut to self.get_ticks(lookback=1, as_dict=True) """
        return self.get_ticks(lookback=1, as_dict=True)

    # ---------------------------------------
    def get_price(self):
        """ Shortcut to self.get_ticks(lookback=1, as_dict=True)['last'] """
        tick = self.get_ticks(lookback=1, as_dict=True)
        return None if tick is None else tick['last']

    # ---------------------------------------
    def get_quote(self):
        """ Get last quote for this instrument

        :Retruns:
            quote : dict
                The quote for this instruments
        """
        if self in self.parent.quotes.keys():
            return self.parent.quotes[self]
        return None

    # ---------------------------------------
    def get_orderbook(self):
        """Get orderbook for the instrument

        :Retruns:
            orderbook : dict
                orderbook dict for the instrument
        """
        if self in self.parent.books.keys():
            return self.parent.books[self]

        return {
            "bid": [0], "bidsize": [0],
            "ask": [0], "asksize": [0]
        }

    # ---------------------------------------
    def create_position(self, entry_price, stop_loss, quantity=None, pos_type=PositionType.CO) -> Position:
        """
        return trade if all the conditions are met
        :param pos_type: position variety. ex MIS, CO, CNC. default to MIS. Possible types are defined in enums.PositionType
        :param quantity: quantity will be calculated based on risk assessment if null.
        :param entry_price:
        :param stop_loss:
        :return: position:Position
        """
        if self.parent.risk_assessor is not None:
            position = self.parent.risk_assessor.create_position(entry_price, stop_loss, quantity=quantity)
            position._tickerId = str(self.get_tickerId())
            position._symbol = self
            position.algo = self.parent.name
            position._variety = pos_type
            return position
        else:
            direction = "LONG" if entry_price > stop_loss else "SHORT"
            return Position(_tickerId=str(self.get_tickerId()), _symbol=self, entry_price=entry_price,
                            stop=stop_loss, _direction=direction, algo=self.parent.name, _quantity=quantity,
                            _variety=pos_type)

    # ---------------------------------------
    def open_position(self, position: Position, **kwargs):
        if position.active or self._position is not None:
            raise Exception("Position can't be opened because there is an active open position")

        self._position = position

        txn_type = position.direction.replace("LONG", "BUY").replace("SHORT", "SELL")
        if self.parent.backtest:
            self.order(txn_type, position.quantity, **kwargs)
        else:
            def callback(trade=position, txn=txn_type, commands=(), opts=kwargs, **args):
                cancel = True if 'cancel' in commands else False
                market = True if 'market' in commands else False
                if cancel:
                    self._position = None
                    self.logger.info(f'Order cancelled - ${trade.symbol}')
                else:
                    position.open_position()
                    self.parent.risk_assessor.enter_position(position)
                    order_id = self.order(txn, trade.quantity, limit_price=0 if market else trade.entry_price,
                                          # target=trade.target, providing
                                          # target will result in BO order
                                          pos_type=trade.variety,
                                          initial_stop=trade.stop, **opts)
                    position._broker_order_id = order_id
                    self.save_to_db(position)

            self.bot.send_order(position, "**ENTER** #" + self, callback=callback,
                                commands=('limit', 'cancel', 'market'))

    # ---------------------------------------
    def close_position(self, position: Position, exit_price=0, force=False, **kwargs):
        txn_type = "SELL" if position.direction == "LONG" else "BUY"  # EXIT
        tick = self.get_tick()
        exit_price = exit_price or (tick['last'] if tick else
                                    self.get_bar()['close'] if self.get_bar() else 0)
        position.exit_price = exit_price

        def _close(trade=position, txn=txn_type, market=True, opts=kwargs, **args):
            if self._position is not None and trade is self._position:
                self._position = None
            if not trade.active:
                raise Exception("Position can't be closed because the status is inactive")
            trade.close_position()
            self.parent.risk_assessor.exit_position(trade)
            if trade.variety == PositionType.MIS or trade.variety == PositionType.CNC:
                # TODO if MIS verify if position is open with broker executed.
                self.order(txn, trade.quantity, pos_type=trade.variety,
                           limit_price=0 if market else trade.exit_price,
                           **opts, **args)
            else:
                self.cancel_order(trade.broker_order_id)
            self.save_to_db(position)

        if self.parent.backtest:
            self.order(txn_type, position.quantity, **kwargs)
        elif force:
            _close()
        else:
            def callback(trade=position, commands=(), **args):
                market = True if 'market' in commands else False
                _close(market=market, **args)

            self.bot.send_order(position, "**EXIT** #" + self, callback=callback,
                                commands=('limit', 'market'))

        self.save_to_db(position)

    # ---------------------------------------
    def order(self, txn_type, quantity, **kwargs):
        """ Send an order for this instrument

        :Parameters:

            direction : string
                Order Type (BUY/SELL, EXIT/FLATTEN)
            quantity : int
                Order quantity

        :Optional:

            limit_price : float
                In case of a LIMIT order, this is the LIMIT PRICE
            expiry : int
                Cancel this order if not filled after *n* seconds (default 60 seconds)
            order_type : string
                Type of order: Market (default), LIMIT (default when limit_price is passed),
                MODIFY (required passing or orderId)
            orderId : int
                If modifying an order, the order id of the modified order
            target : float
                target (exit) price
            initial_stop : float
                price to set hard stop
            stop_limit: bool
                Flag to indicate if the stop should be STOP or STOP LIMIT (default False=STOP)
            trail_stop_at : float
                price at which to start trailing the stop
            trail_stop_by : float
                % of trailing stop distance from current price
            fillorkill: bool
                fill entire quantiry or none at all
            iceberg: bool
                is this an iceberg (hidden) order
            tif: str
                time in force (DAY, GTC, IOC, GTD). default is ``DAY``
        """
        txn_type = txn_type.upper().replace("LONG", "BUY").replace("SHORT", "SELL")
        return self.parent.order(txn_type.upper(), self, quantity, **kwargs)

    # ---------------------------------------
    def cancel_order(self, order_id):
        """ Cancels an order for this instrument

        :Parameters:
            orderId : int
                Order ID
        """

        return self.parent.cancel_order(order_id)

    # ---------------------------------------
    def market_order(self, direction, quantity, **kwargs):
        """ Shortcut for ``instrument.order(...)`` and accepts all of its
        `optional parameters <#qtpylib.instrument.Instrument.order>`_

        :Parameters:
            direction : string
                Order Type (BUY/SELL, EXIT/FLATTEN)
            quantity : int
                Order quantity
        """
        kwargs['limit_price'] = 0
        kwargs['order_type'] = "MARKET"
        self.parent.order(direction.upper(), self, quantity=quantity, **kwargs)

    # ---------------------------------------
    def limit_order(self, direction, quantity, price, **kwargs):
        """ Shortcut for ``instrument.order(...)`` and accepts all of its
        `optional parameters <#qtpylib.instrument.Instrument.order>`_

        :Parameters:
            direction : string
                Order Type (BUY/SELL, EXIT/FLATTEN)
            quantity : int
                Order quantity
            price : float
                Limit price
        """
        kwargs['limit_price'] = price
        kwargs['order_type'] = "LIMIT"
        self.parent.order(direction.upper(), self, quantity=quantity, **kwargs)

    # ---------------------------------------
    def buy(self, quantity, **kwargs):
        """ Shortcut for ``instrument.order("BUY", ...)`` and accepts all of its
        `optional parameters <#qtpylib.instrument.Instrument.order>`_

        :Parameters:
            quantity : int
                Order quantity
        """
        self.parent.order("BUY", self, quantity=quantity, **kwargs)

    # ---------------------------------------
    def buy_market(self, quantity, **kwargs):
        """ Shortcut for ``instrument.order("BUY", ...)`` and accepts all of its
        `optional parameters <#qtpylib.instrument.Instrument.order>`_

        :Parameters:
            quantity : int
                Order quantity
        """
        kwargs['limit_price'] = 0
        kwargs['order_type'] = "MARKET"
        self.parent.order("BUY", self, quantity=quantity, **kwargs)

    # ---------------------------------------
    def buy_limit(self, quantity, price, **kwargs):
        """ Shortcut for ``instrument.order("BUY", ...)`` and accepts all of its
        `optional parameters <#qtpylib.instrument.Instrument.order>`_

        :Parameters:
            quantity : int
                Order quantity
            price : float
                Limit price
        """
        kwargs['limit_price'] = price
        kwargs['order_type'] = "LIMIT"
        self.parent.order("BUY", self, quantity=quantity, **kwargs)

    # ---------------------------------------
    def sell(self, quantity, **kwargs):
        """ Shortcut for ``instrument.order("SELL", ...)`` and accepts all of its
        `optional parameters <#qtpylib.instrument.Instrument.order>`_

        :Parameters:
            quantity : int
                Order quantity
        """
        self.parent.order("SELL", self, quantity=quantity, **kwargs)

    # ---------------------------------------
    def sell_market(self, quantity, **kwargs):
        """ Shortcut for ``instrument.order("SELL", ...)`` and accepts all of its
        `optional parameters <#qtpylib.instrument.Instrument.order>`_

        :Parameters:
            quantity : int
                Order quantity
        """
        kwargs['limit_price'] = 0
        kwargs['order_type'] = "MARKET"
        self.parent.order("SELL", self, quantity=quantity, **kwargs)

    # ---------------------------------------
    def sell_limit(self, quantity, price, **kwargs):
        """ Shortcut for ``instrument.order("SELL", ...)`` and accepts all of its
        `optional parameters <#qtpylib.instrument.Instrument.order>`_

        :Parameters:
            quantity : int
                Order quantity
            price : float
                Limit price
        """
        kwargs['limit_price'] = price
        kwargs['order_type'] = "LIMIT"
        self.parent.order("SELL", self, quantity=quantity, **kwargs)

    # ---------------------------------------
    def exit(self):
        """ Shortcut for ``instrument.order("EXIT", ...)``
        (accepts no parameters)"""
        self.parent.order("EXIT", self)

    # ---------------------------------------
    def flatten(self):
        """ Shortcut for ``instrument.order("FLATTEN", ...)``
        (accepts no parameters)"""
        self.parent.order("FLATTEN", self)

    def clear(self):
        if self._position and self._position.active:
            self.logger.warning("Called clear when there's an active position")
        self._position = None
        self.logger.warning("Reset performed on " + self)

    # ---------------------------------------
    def get_contract(self):
        """Get contract object for this instrument

        :Retruns:
            contract : Object
                IB Contract object
        """
        return self.parent.get_contract(self)

    # ---------------------------------------
    def get_contract_details(self):
        """Get contract details for this instrument

        :Retruns:
            contract_details : dict
                IB Contract details
        """
        return self.parent.get_contract_details(self)

    # ---------------------------------------
    def get_tickerId(self) -> object:
        """Get contract's tickerId for this instrument

        :Retruns:
            tickerId : int
                IB Contract's tickerId
        """
        return self.parent.get_tickerId(self)

    # ---------------------------------------
    def get_combo(self):
        """Get instrument's group if part of an instrument group
        !IMPORTANT: NOT SUPPORTED. UNSTABLE API
        """
        raise Exception("Not supported")

    # ---------------------------------------
    def get_positions(self, attr=None):
        """Get the positions data for the instrument
        !IMPORTANT: NOT SUPPORTED. UNSTABLE API
        :Optional:
            attr : string
                Position attribute to get
                (optional attributes: symbol, position, avgCost, account)

        :Retruns:
            positions : dict (positions) / float/str (attribute)
                positions data for the instrument
        """
        pos = [self._position] if self._position is not None else []
        return pos

    # ---------------------------------------
    def set_position(self, position):
        self._position = position

    # ---------------------------------------
    def get_portfolio(self):
        """Get portfolio data for the instrument
        !IMPORTANT: NOT SUPPORTED. UNSTABLE API
        :Retruns:
            portfolio : dict
                portfolio data for the instrument
        """
        raise Exception("Not supported")

    # ---------------------------------------
    def get_orders(self):
        """Get orders for the instrument
        !IMPORTANT: NOT SUPPORTED. UNSTABLE API
        :Retruns:
            orders : list
                list of order data as dict
        """
        raise Exception("Not supported")

    # ---------------------------------------
    def get_pending_orders(self):
        """Get pending orders for the instrument
        !IMPORTANT: NOT SUPPORTED. UNSTABLE API
        :Retruns:
            orders : list
                list of pending order data as dict
        """
        raise Exception("Not supported")

    # ---------------------------------------
    def get_active_order(self, order_type="STOP"):
        """Get artive order id for the instrument by order_type
        !IMPORTANT: NOT SUPPORTED. UNSTABLE API
        :Optional:
            order_type : string
                the type order to return: STOP (default), LIMIT, MARKET

        :Retruns:
            order : object
                IB Order object of instrument
        """
        raise Exception("Not supported")

    # ---------------------------------------
    def get_trades(self):
        """Get orderbook for the instrument
        !IMPORTANT: NOT SUPPORTED. UNSTABLE API
        :returns:
            trades : pd.DataFrame
                instrument's trade log as DataFrame
        """
        raise Exception("Not supported")

    # ---------------------------------------
    def get_symbol(self):
        """Get symbol of this instrument

        :Retruns:
            symbol : string
                instrument's symbol
        """
        return self

    # ---------------------------------------
    def modify_order(self, orderId, quantity=None, limit_price=None):
        """Modify quantity and/or limit price of an active order for the instrument
        !IMPORTANT: NOT SUPPORTED. UNSTABLE API
        :Parameters:
            orderId : int
                the order id to modify

        :Optional:
            quantity : int
                the required quantity of the modified order
            limit_price : int
                the new limit price of the modified order
        """
        raise Exception("Not supported")

    # ---------------------------------------
    def modify_order_group(self, orderId, entry=None, target=None, stop=None, quantity=None):
        """Modify bracket order
        !IMPORTANT: NOT SUPPORTED. UNSTABLE API
        :Parameters:
            orderId : int
                the order id to modify

        :Optional:
            entry: float
                new entry limit price (for unfilled limit orders only)
            target: float
                new target limit price (for unfilled limit orders only)
            stop: float
                new stop limit price (for unfilled limit orders only)
            quantity : int
                the required quantity of the modified order
        """
        raise Exception("Not supported")

    # ---------------------------------------
    def move_stoploss(self, stoploss):
        """Modify stop order.
        Auto-discover **orderId** and **quantity** and invokes ``self.modify_order(...)``.
        !IMPORTANT: NOT SUPPORTED. UNSTABLE API
        :Parameters:
            stoploss : float
                the new stoploss limit price

        """
        raise Exception("Not supported")

    # ---------------------------------------
    def get_margin_requirement(self):
        """ Get margin requirements for intrument (futures only)
        !IMPORTANT: NOT SUPPORTED. UNSTABLE API
        :Retruns:
            margin : dict
                margin requirements for instrument
                (all values are ``None`` for non-futures instruments)
        """
        raise Exception("Not supported yet!")

    # ---------------------------------------
    def get_max_contracts_allowed(self, overnight=True):
        """ Get maximum contracts allowed to trade
        baed on required margin per contract and
        current account balance (futures only)
        !IMPORTANT: NOT SUPPORTED. UNSTABLE API
        :Parameters:
            overnight : bool
                Calculate based on Overnight margin (set to ``False`` to use Intraday margin req.)

        :Retruns:
            contracts : int
                maximum contracts allowed to trade (returns ``None`` for non-futures)
        """
        raise Exception("Not supported yet!")

    def get_margin_max_contracts(self, overnight=True):
        """ Deprecated (renamed to ``get_max_contracts_allowed``)"""
        raise Exception("Not supported yet!")

    # ---------------------------------------
    def get_ticksize(self):
        """ Get instrument ticksize

        :Retruns:
            ticksize : int
                Min. tick size
        """
        ticksize = self.parent.get_contract_details(self)['m_minTick']
        return float(ticksize)

    # ---------------------------------------
    def pnl_in_range(self, min_pnl, max_pnl):
        """ Check if instrument pnl is within given range
        !IMPORTANT: NOT SUPPORTED. UNSTABLE API
        :Parameters:
            min_pnl : flaot
                minimum session pnl (in USD / IB currency)
            max_pnl : flaot
                maximum session pnl (in USD / IB currency)

        :Retruns:
            status : bool
                if pnl is within range
        """
        portfolio = self.get_portfolio()
        return -abs(min_pnl) < portfolio['totalPNL'] < abs(max_pnl)

    # ---------------------------------------
    def log_signal(self, signal):
        """ Log Signal for instrument

        :Parameters:
            signal : integer
                signal identifier (1, 0, -1)
        """
        return self.parent._log_signal(self, signal)

    # ---------------------------------------
    def save_to_db(self, obj):
        if not self.parent.blotter_args["dbskip"]:
            obj.save()

    # ---------------------------------------
    @property
    def bars(self):
        """(Property) Shortcut to self.get_bars()"""
        return self.get_bars()

    # ---------------------------------------
    @property
    def bar(self):
        """(Property) Shortcut to self.get_bar()"""
        return self.get_bar()

    # ---------------------------------------
    @property
    def ticks(self):
        """(Property) Shortcut to self.get_ticks()"""
        return self.get_ticks()

    # ---------------------------------------
    @property
    def tick(self):
        """(Property) Shortcut to self.get_tick()"""
        return self.get_tick()

    # ---------------------------------------
    @property
    def price(self):
        """(Property) Shortcut to self.get_price()"""
        return self.get_price()

    # ---------------------------------------
    @property
    def quote(self):
        """(Property) Shortcut to self.get_quote()"""
        return self.get_quote()

    # ---------------------------------------
    @property
    def orderbook(self):
        """(Property) Shortcut to self.get_orderbook()"""
        return self.get_orderbook()

    # ---------------------------------------
    @property
    def symbol(self):
        """(Property) Shortcut to self.get_symbol()"""
        return self

    # ---------------------------------------
    @property
    def contract(self):
        """(Property) Shortcut to self.get_contract()"""
        return self.get_contract()

    # ---------------------------------------
    @property
    def contract_details(self):
        """(Property) Shortcut to self.get_contract_details()"""
        return self.get_contract_details()

    # ---------------------------------------
    @property
    def tickerId(self):
        """(Property) Shortcut to self.get_tickerId()"""
        return self.get_tickerId()

    # ---------------------------------------
    @property
    def combo(self):
        """(Property) Shortcut to self.get_combo()"""
        return self.get_combo()

    # ---------------------------------------
    @property
    def positions(self):
        """(Property) Shortcut to self.get_positions()"""
        raise Exception("Not Supported")

    # ---------------------------------------
    @property
    def position(self):
        """(Property) Shortcut to self._position"""
        return self._position

    # ---------------------------------------
    @property
    def portfolio(self):
        """(Property) Shortcut to self.get_portfolio()"""
        raise Exception("Not Supported")

    # ---------------------------------------
    @property
    def orders(self):
        """(Property) Shortcut to self.get_orders()"""
        return self.get_orders()

    # ---------------------------------------
    @property
    def pending_orders(self):
        """(Property) Shortcut to self.get_pending_orders()"""
        return self.get_pending_orders()

    # ---------------------------------------
    @property
    def trades(self):
        """(Property) Shortcut to self.get_trades()"""
        return self.get_trades()

    # ---------------------------------------
    @property
    def margin_requirement(self):
        """(Property) Shortcut to self.get_margin_requirement()"""
        return self.get_margin_requirement()

    # ---------------------------------------
    @property
    def margin_max_contracts(self):
        """ Deprecated (renamed to ``max_contracts_allowed``)"""
        return self.get_max_contracts_allowed()

    @property
    def max_contracts_allowed(self):
        """(Property) Shortcut to self.get_max_contracts_allowed()"""
        return self.get_max_contracts_allowed()

    # ---------------------------------------
    @property
    def ticksize(self):
        """(Property) Shortcut to self.get_ticksize()"""
        return self.get_ticksize()
