# Copyright 2021 vin8tech
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import json
import logging
import threading
import os

from kinetick.utils import utils

from kinetick.algo import Algo
from kinetick.bot import Bot
from kinetick.enums import Timeframes
from kinetick.risk_assessor import RiskAssessor
import pandas as pd
import numpy as np
from decimal import getcontext

from kinetick.lib.indicators.talib_indicators import MA

getcontext().prec = 6

utils.create_logger(__name__, level=os.getenv('LOGLEVEL') or logging.INFO)
logger = logging.getLogger(__name__)

__LOOKBACK__ = 800


class MacdSuperStrategy(Algo):
    """
    slow fast macd cross over
    entry on supertrend stop at supertrend and VWAP
    stochastic
    """

    count = {}

    ta_df = pd.DataFrame({'open': [0], 'high': [0], 'low': [0], 'close': [0], 'vwap': [0], 'ma': [0],
                          'macd': [0], 'macdsignal': [0], 'macdhist': [0], 'ATR_7': [0], 'ST': [0], 'STX': [0],
                          'slow_k': [0], 'slow_d': [0], 'signal': [0]})

    ti = {}  # symbol:ta
    _bars = {"~": pd.DataFrame()}

    def __init__(self, instruments, fast_period=1, slow_period=15, **kwargs):
        super().__init__(instruments, **kwargs)
        self.__MIN_PERIOD__ = Timeframes.timeframe_to_minutes(Timeframes.to_timeframe(self.resolution))
        self.__FAST_PERIOD__ = fast_period
        self.__SLOW_PERIOD__ = slow_period
        self.bot = Bot()

    # ---------------------------------------
    def gen_signal(self, macd_hist, stx, k, d):
        if utils.to_decimal(macd_hist, 2) > 0.0 and stx == 'up' \
                and k > d and (int(k) <= 90 or int(d) <= 90):
            return 'B'
        elif utils.to_decimal(macd_hist, 2) < 0.0 and stx == 'down' \
                and k < d and (k > 20 or d > 20):
            return 'S'
        return 'N'

    # ---------------------------------------
    def gen_slow_signal(self, hist, k, d):
        if hist > 0.0 and (k <= 90 or d <= 90):
            return 'B'
        elif hist < 0.0 and (k >= 30 or d >= 30):
            return 'S'
        return 'N'

    # ---------------------------------------
    def fill_ta(self, **kwargs):
        df = pd.DataFrame()
        for key, value in kwargs.items():
            df[key] = value[-2:]
        return df

    # ---------------------------------------
    def gen_indicators(self, bars, fast=True):
        ma = MA(data=bars, timeperiod=5)  # TODO maybe redundant on slow ti
        macd = bars['close'].macd(fast=12, slow=26, smooth=9)
        stoch = bars.stoch(window=26, d=6, k=6, fast=False)
        if fast:
            super_trend = bars.supertrend(period=7, multiplier=3)
        else:
            super_trend = pd.DataFrame({'ATR_7': [0], 'ST': [0], 'STX': [0]})
            # stoch = pd.DataFrame({'slow_k': [0], 'slow_d': [0]})
        return ma, macd, super_trend, stoch

    # ---------------------------------------
    def get_statistics(self, instrument, period=1, bars=None):
        if bars is None:
            bars = self.sync_bars(instrument)

        if 'vwap' not in bars.columns:
            bars['vwap'] = bars.vwap()

        if not period == self.__MIN_PERIOD__:
            ticks = str(period) + 'T'
            bars = bars.resample(ticks).agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'vwap': 'last',
                # vwap could be missing initially until sync_bars is called and therefore
                # the resulting bars would be empty
                'volume': 'sum'}).dropna()

        fast = False if not period == self.__FAST_PERIOD__ else True

        ma, macd, super_trend, stoch = self.gen_indicators(bars, fast)

        params = {
            'open': bars['open'],
            'high': bars['high'],
            'low': bars['low'],
            'close': bars['close'],
            'volume': bars['volume'],
            'ma': ma,
            'macd': macd['macd'],
            'macdsignal': macd['signal'],
            'macdhist': macd['histogram'],
            'ATR_7': super_trend['ATR_7'],
            'ST': super_trend['ST'],
            'STX': super_trend['STX'],
            'slow_k': stoch['slow_k'],
            'slow_d': stoch['slow_d']
        }

        if 'vwap' in bars.columns:
            params['vwap'] = bars[['vwap']].fillna(method='pad')

        return self.fill_ta(**params)

    # ---------------------------------------
    def fill_slow_ta(self, instrument):
        bars = instrument.get_bars(lookback=__LOOKBACK__)
        # bars = self._bars[instrument]
        slow_ta = self.get_statistics(instrument=instrument, period=self.__SLOW_PERIOD__, bars=bars)
        slow_ta['signal'] = \
            np.vectorize(self.gen_slow_signal)(slow_ta['macdhist'], slow_ta['slow_k'], slow_ta['slow_d'])
        self.ti[instrument]['slow_ta'] = slow_ta
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            logger.debug("--------\nSLOW TA - %s\n %s", instrument, self.ti[instrument]['slow_ta'][-1:])

    # ---------------------------------------
    def fill_fast_ta(self, instrument):
        bars = instrument.get_bars(lookback=__LOOKBACK__)
        # bars = self._bars[instrument]  # append raw ticks from time delta
        fast_ta = self.get_statistics(instrument, period=self.__FAST_PERIOD__, bars=bars)
        fast_ta['signal'] = \
            np.vectorize(self.gen_signal)(fast_ta['macdhist'], fast_ta['STX'], fast_ta['slow_k'], fast_ta['slow_d'])
        self.ti[instrument]['fast_ta'] = fast_ta
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            logger.debug("--------\nFAST TA - %s\n %s", instrument, self.ti[instrument]['fast_ta'][-1:])

    # ---------------------------------------
    def exit_trade(self, instrument):
        try:
            logger.info(f'Exit position {instrument}')
            position = instrument.position
            instrument.close_position(position)

            self.record(time=position.exit_time)
            self.record(EXIT_REASON=position.exit_reason)
            return position
        except Exception as e:
            logger.error(e)

    # ---------------------------------------
    def enter_trade(self, instrument, entry_price, stop_loss):
        try:
            logger.info(f'Enter position {instrument}')
            position = instrument.create_position(entry_price, stop_loss)

            instrument.open_position(position)
            return position
        except Exception as e:
            logger.error(e)

    # ---------------------------------------

    def init_instruments(self, instruments=None):
        if instruments is None:
            instruments = self.instruments
        for instrument in instruments:
            self.count[instrument] = 0
            if instrument not in self.ti.keys():
                self.ti[instrument] = {'fast_ta': self.ta_df.copy(), 'slow_ta': self.ta_df.copy()}

    # ---------------------------------------
    def add_instrument(self, symbol):
        pass

    # ---------------------------------------
    def resetHandler(self, update, context):
        self.risk_assessor.reset()
        self.log_algo.info("Strategy reset complete.")
        update.message.reply_text("Strategy reset complete.")

    # ---------------------------------------
    def health(self, update, context):
        update.message.reply_text(f'count={self.count}')

    # ---------------------------------------

    def on_start(self):
        """ initialize tick counter """
        self.init_instruments()

        self.bot.add_command_handler("reset", self.resetHandler, "Reset")
        self.bot.add_command_handler("health", self.health, "Health Check")

    # ---------------------------------------
    def on_quote(self, instrument):
        pass

    # ---------------------------------------
    def on_orderbook(self, instrument):
        pass

    # ---------------------------------------
    def on_fill(self, instrument, order):
        pass

    # ----------------------------------------
    def apply(self, instrument, ltp, timestamp=None):
        # self.fill_fast_ta(instrument)
        slow_ta = self.ti[instrument]['slow_ta'].to_dict(orient='records')
        fast_ta = self.ti[instrument]['fast_ta'].to_dict(orient='records')

        # validate stop-loss and target hit
        open_positions = [instrument.position] if instrument.position is not None else []
        for position in open_positions:
            """Exit checks"""
            direction = position.direction

            sl_hit = False
            if direction == 'LONG':
                sl_hit = True if ltp <= position.stop else False
            elif direction == 'SHORT':
                sl_hit = True if ltp >= position.stop else False

            if (ltp <= position.target and direction == "SHORT") or \
                    (ltp >= position.target and direction == "LONG") or sl_hit:
                """target or stop-loss hit (exit conditions)"""
                position.exit_price = ltp
                position.exit_time = timestamp

                if sl_hit:
                    position.exit_reason = 'SL Hit'
                    self.exit_trade(instrument)

                elif (fast_ta[-1]['signal'] == 'B' or (fast_ta[-1]['slow_k'] > fast_ta[-1]['slow_d'])) \
                        and position.direction == "SHORT":
                    position.exit_reason = 'Target Hit'
                    self.exit_trade(instrument)

                elif (fast_ta[-1]['signal'] == 'S' or (fast_ta[-1]['slow_k'] < fast_ta[-1]['slow_d'])) \
                        and position.direction == "LONG":
                    position.exit_reason = 'Target Hit'
                    self.exit_trade(instrument)
                else:
                    # TODO trail stop-loss
                    position.exit_reason = 'Target Hit'
                    self.exit_trade(instrument)

        """ entry conditions """
        if len(open_positions) == 0:

            entry_price = (fast_ta[-1]['ma'] + ltp) / 2

            if slow_ta[-1]['signal'] == 'B' and fast_ta[-1]['signal'] == 'B':
                # one minute super trend
                bars = instrument.get_bars(lookback=__LOOKBACK__)
                ma, macd, super_trend, stoch = self.gen_indicators(bars, fast=True)
                if super_trend.iloc[-1]['STX'] == 'up':
                    entry_price = entry_price if entry_price < ltp else ltp
                    entry_price = 5 * round(entry_price / 5, 2)
                    st = fast_ta[-1]['ST']
                    st_buffer = st * 0.001
                    logger.info(f'BUY {instrument} at: {entry_price}')
                    logger.info("FAST TA - %s\n %s", instrument, json.dumps(fast_ta[-1], indent=2))
                    logger.info("SLOW TA - %s\n %s", instrument, json.dumps(slow_ta[-1], indent=2))
                    self.enter_trade(instrument, entry_price,
                                     stop_loss=utils.round_to_fraction(st - st_buffer, 0.05))

            elif slow_ta[-1]['signal'] == 'S' and fast_ta[-1]['signal'] == 'S':
                # one minute super trend
                bars = instrument.get_bars(lookback=__LOOKBACK__)
                ma, macd, super_trend, stoch = self.gen_indicators(bars, fast=True)
                if super_trend.iloc[-1]['STX'] == 'down':
                    entry_price = entry_price if entry_price > ltp else ltp
                    entry_price = 5 * round(entry_price / 5, 2)
                    st = fast_ta[-1]['ST']
                    st_buffer = st * 0.001
                    logger.info(f'SELL {instrument} at: {entry_price}')
                    logger.info("FAST TA - %s\n %s", instrument, json.dumps(fast_ta[-1], indent=2))
                    logger.info("SLOW TA - %s\n %s", instrument, json.dumps(slow_ta[-1], indent=2))
                    self.enter_trade(instrument, entry_price,
                                     stop_loss=utils.round_to_fraction(st + st_buffer, 0.05))

    # ---------------------------------------
    def on_tick(self, instrument):
        # increase counter and do nothing if nor 100th tick
        self.count[instrument] += 1

        if self.count[instrument] % 100 != 0:
            return

        # get last tick dict
        tick = instrument.get_ticks(lookback=1, as_dict=True)
        time = tick['datetime']
        self.apply(instrument, tick['last'], time)

    # ---------------------------------------
    def on_bar(self, instrument):
        if self.backtest:
            self.simulate(instrument)
        else:
            self.fill_slow_ta(instrument)
            self.fill_fast_ta(instrument)
            bar = instrument.get_bars(lookback=1, as_dict=True)
            self.apply(instrument, bar['close'], bar['datetime'])

    # --------------------------------------------
    def simulate(self, instrument):
        bars = instrument.get_bars(lookback=__LOOKBACK__)
        self.count[instrument] += 1
        if len(bars) < __LOOKBACK__:
            return

        bar = instrument.get_bars(lookback=1, as_dict=True)

        self.fill_slow_ta(instrument)
        self.fill_fast_ta(instrument)

        self.apply(instrument, bar['close'], bar['datetime'])
    # ----------------------------------------


# ===========================================
if __name__ == "__main__":
    strategy = MacdSuperStrategy(
        instruments=["ACC"],
        resolution=Timeframes.MINUTE_1,
        tick_window=50,
        bar_window=__LOOKBACK__,
        preload="1D",
        backtest=True,
        start="2020-07-01 00:15:00",
        risk_assessor=RiskAssessor(max_trades=4, initial_capital=120000, initial_margin=1000,
                                   risk2reward=1.2, risk_per_trade=100),
        # backfill=True,
        timezone="Asia/Calcutta"
    )
    strategy.run()
