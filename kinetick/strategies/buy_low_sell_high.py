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
import logging
import os

from kinetick.utils import utils

from kinetick.algo import Algo
from kinetick.enums import Timeframes, PositionType
from kinetick.risk_assessor import RiskAssessor
from decimal import getcontext

getcontext().prec = 6

utils.create_logger(__name__, level=os.getenv('LOGLEVEL') or logging.INFO)
logger = logging.getLogger(__name__)

__LOOKBACK__ = 800


class BuyLowSellHigh(Algo):
    """
    buy low sell high algorithm
    """

    def on_start(self):
        """ initialize your strategy. Perform any operation that are required to be run before starting algo."""
        pass

    # ---------------------------------------
    def on_quote(self, instrument):
        logger.debug(instrument.quote)

    # ---------------------------------------
    def on_orderbook(self, instrument):
        logger.debug(instrument.orderbook)

    # ---------------------------------------
    def on_fill(self, instrument, order):
        """ Not supported currently """
        pass

    # ---------------------------------------
    def on_tick(self, instrument):
        # get last tick dict
        logger.debug(instrument.tick)
        # get last 10 ticks
        logger.debug(instrument.get_ticks(lookback=10, as_dict=True))
        # get all the ticks
        ticks = instrument.get_ticks()

    # ---------------------------------------
    def on_bar(self, instrument):
        bars = instrument.get_bars(lookback=3)
        if instrument.position is None:
            """ check if there any open positions on the instrument."""
            if len(bars) >= 2:  # at least 2 bars are needed
                if bars['close'][-1] < bars['close'][0]:
                    position = instrument.create_position(entry_price=bars['close'][-1],
                                                          stop_loss=bars['low'][-1],
                                                          quantity=1, pos_type=PositionType.MIS)
                    """ Create position instance with inputs. 
                        If quantity is not provided then it will be automatically calculated based on risk parameters.
                    """
                    instrument.open_position(position)
                    """ instrument.open_position() will send order request to bot and 
                        attach the position instance to instrument to indicate about open position.
                        when order request is accepted from bot the position becomes active.
                    """
        elif instrument.position.active:
            """ check IF the position was executed in bot """
            if len(bars) >= 2:
                if bars['close'][-1] > bars['close'][0]:
                    instrument.close_position(instrument.position)
                    """ close the position. """

    # --------------------------------------------


# ===========================================
if __name__ == "__main__":
    strategy = BuyLowSellHigh(
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
