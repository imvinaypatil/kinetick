import argparse
import os
from kinetick.enums import Timeframes
from kinetick.risk_assessor import RiskAssessor
from webull import webull

__LOOKBACK__ = 800

from kinetick.strategies import MacdSuperStrategy


def load_cli_args():
    parser = argparse.ArgumentParser(
        description='Kinetick Strategy Runner',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--zerodha_user', default=os.getenv("zerodha_user"),
                        help='Zerodha Username', required=False)
    parser.add_argument('--zerodha_password', default=os.getenv("zerodha_password"),
                        help='Zerodha Password', required=False)
    parser.add_argument('--zerodha_pin', default=os.getenv("zerodha_pin"),
                        help='Zerodha PIN', required=False)
    parser.add_argument('--webull_user', default=os.getenv("webull_user"),
                        help='webull user', required=False)
    parser.add_argument('--webull_password', default=os.getenv("webull_password"),
                        help='webull password', required=False)
    parser.add_argument('--stocks_list', default=os.getenv("stocks_list"),
                        help='webull stocks watchlist name', required=False)
    parser.add_argument('--resolution', default=os.getenv("resolution") or Timeframes.MINUTE_5,
                        help='webull bar interval in terms of resolution (default=5m). '
                             'ex. 1m, 3m, 5m, 15m, 30m, 1h, 2h, 3h, 4h, 6h, 8h, 1D',
                        required=False)
    parser.add_argument('--fast_period', default=os.getenv("fast_period") or 1,
                        help='Strategy fast period interval (default=1). ex. 3', required=False)
    parser.add_argument('--slow_period', default=os.getenv("slow_period") or 15,
                        help='Strategy slow period interval (default=15). ex. 15', required=False)
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
    parser.add_argument('--SYM', default=os.getenv("SYM"),
                        help='list of SYMBOLS separated by ";" ex. "APPL;AMZN"', required=False)

    # only return non-default cmd line args
    # (meaning only those actually given)
    cmd_args, _ = parser.parse_known_args()
    args = {arg: val for arg, val in vars(
        cmd_args).items()}
    return args


# ===========================================
if __name__ == "__main__":

    # override args with any (non-default) command-line args
    args = load_cli_args()
    instruments = []

    if 'SYM' in args and args['SYM'] is not None:
        instruments = args['SYM'].split(';')
    else:
        webull = webull()
        webull.login(username=args['webull_user'], password=args['webull_password'], device_name='MacOS Firefox')
        watchlist = webull.get_watchlists()
        watchlist = [wc for wc in watchlist if wc['name'] == args['stocks_list']]

        if len(watchlist) > 0:
            instruments = list(map(
                lambda ticker: (
                    ticker['symbol'], "STK", ticker['disExchangeCode'], ticker['currencySymbol'], "", 0.0, ""),
                watchlist[0]['tickerList']))

    strategy = MacdSuperStrategy(
        # instruments,
        instruments=["UJJIVAN", "GAIL", 'SUNPHARMA', "SBIN", "GLENMARK", "BANDHANBNK"],
        # resolution=Timeframes.MINUTE_5,
        tick_window=__LOOKBACK__,
        bar_window=__LOOKBACK__,
        preload="1D",
        output="./orders.csv",
        # start="2020-08-01 00:15:00",
        risk_assessor=RiskAssessor(max_trades=args['max_trades'], initial_capital=args['initial_capital'],
                                   initial_margin=args['initial_margin'], risk2reward=args['risk2reward'],
                                   risk_per_trade=args['risk_per_trade']),
        timezone="Asia/Calcutta",
        **args
    )
    strategy.run()
