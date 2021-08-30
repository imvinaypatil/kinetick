from .buy_low_sell_high import BuyLowSellHigh


def strategy():
    algo = BuyLowSellHigh(
        instruments=['SBIN', 'HDFC'],
        tick_window=100,
        bar_window=800,
        preload="1D",
        output="./orders.csv",
        timezone="Asia/Calcutta",
        # backtest=True,
        # start="2020-07-01 00:15:00",
        # backfill=True,
    )

    return algo
