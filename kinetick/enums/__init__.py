class sides:
    BUY = 'buy'
    SELL = 'sell'


class trade_types:
    LONG = 'long'
    SHORT = 'short'


class order_statuses:
    ACTIVE = 'ACTIVE'
    CANCELED = 'CANCELED'
    EXECUTED = 'EXECUTED'
    QUEUED = 'QUEUED'


class Timeframes:
    MINUTE_T = 'T'
    MINUTE_1 = '1m'
    MINUTE_3 = '3m'
    MINUTE_5 = '5m'
    MINUTE_15 = '15m'
    MINUTE_30 = '30m'
    HOUR_1 = '1h'
    HOUR_2 = '2h'
    HOUR_3 = '3h'
    HOUR_4 = '4h'
    HOUR_6 = '6h'
    HOUR_8 = '8h'
    DAY_1 = '1D'

    @staticmethod
    def tuple():
        return (
            Timeframes.MINUTE_T,
            Timeframes.MINUTE_1,
            Timeframes.MINUTE_3,
            Timeframes.MINUTE_5,
            Timeframes.MINUTE_15,
            Timeframes.MINUTE_30,
            Timeframes.HOUR_1,
            Timeframes.HOUR_2,
            Timeframes.HOUR_3,
            Timeframes.HOUR_4,
            Timeframes.HOUR_6,
            Timeframes.HOUR_8,
            Timeframes.DAY_1
        )

    @staticmethod
    def timeframe_to_minutes(timeframe):
        dic = {
            Timeframes.MINUTE_T: 1,
            Timeframes.MINUTE_1: 1,
            Timeframes.MINUTE_3: 3,
            Timeframes.MINUTE_5: 5,
            Timeframes.MINUTE_15: 15,
            Timeframes.MINUTE_30: 30,
            Timeframes.HOUR_1: 60,
            Timeframes.HOUR_2: 60 * 2,
            Timeframes.HOUR_3: 60 * 3,
            Timeframes.HOUR_4: 60 * 4,
            Timeframes.HOUR_6: 60 * 6,
            Timeframes.HOUR_8: 60 * 8,
            Timeframes.DAY_1: 60 * 24,
        }
        try:
            return dic[timeframe]
        except KeyError:
            raise Exception(
                'Timeframe "{}" is invalid. Supported timeframes are 1m, 3m, 5m, 15m, 30m, 1h, 2h, 3h, 4h, 6h, 8h, 1D'.format(
                    timeframe))

    @staticmethod
    def to_timeframe(minutes):
        if minutes in Timeframes.tuple():
            return minutes
        minutes = minutes if isinstance(minutes, int) else int(minutes.replace('T', ''))
        dic = {
            1: Timeframes.MINUTE_1,
            3: Timeframes.MINUTE_3,
            5: Timeframes.MINUTE_5,
            15: Timeframes.MINUTE_15,
            30: Timeframes.MINUTE_30,
            60: Timeframes.HOUR_1,
            120: Timeframes.HOUR_2,
            180: Timeframes.HOUR_3,
            240: Timeframes.HOUR_4,
            300: Timeframes.HOUR_6,
            360: Timeframes.HOUR_8,
            1440: Timeframes.DAY_1
        }
        try:
            return dic[minutes]
        except KeyError:
            raise Exception(
                'Minute "{}" is invalid. Supported timeframes are 1m, 3m, 5m, 15m, 30m, 1h, 2h, 3h, 4h, 6h, 8h, 1D'.format(
                    minutes))

    @staticmethod
    def timeframe_to_resolution(timeframe):
        minutes = Timeframes.timeframe_to_minutes(timeframe)
        return f'{minutes}T'

    @staticmethod
    def max_timeframe(timeframes_list):
        if Timeframes.DAY_1 in timeframes_list:
            return Timeframes.DAY_1
        if Timeframes.HOUR_8 in timeframes_list:
            return Timeframes.HOUR_8
        if Timeframes.HOUR_6 in timeframes_list:
            return Timeframes.HOUR_6
        if Timeframes.HOUR_4 in timeframes_list:
            return Timeframes.HOUR_4
        if Timeframes.HOUR_3 in timeframes_list:
            return Timeframes.HOUR_3
        if Timeframes.HOUR_2 in timeframes_list:
            return Timeframes.HOUR_2
        if Timeframes.HOUR_1 in timeframes_list:
            return Timeframes.HOUR_1
        if Timeframes.MINUTE_30 in timeframes_list:
            return Timeframes.MINUTE_30
        if Timeframes.MINUTE_15 in timeframes_list:
            return Timeframes.MINUTE_15
        if Timeframes.MINUTE_5 in timeframes_list:
            return Timeframes.MINUTE_5
        if Timeframes.MINUTE_3 in timeframes_list:
            return Timeframes.MINUTE_3

        return Timeframes.MINUTE_1


class colors:
    GREEN = 'green'
    YELLOW = 'yellow'
    RED = 'red'
    MAGENTA = 'magenta'
    BLACK = 'black'


class order_roles:
    OPEN_POSITION = 'OPEN POSITION'
    CLOSE_POSITION = 'CLOSE POSITION'
    INCREASE_POSITION = 'INCREASE POSITION'
    REDUCE_POSITION = 'REDUCE POSITION'


class order_flags:
    OCO = 'OCO'
    POST_ONLY = 'PostOnly'
    CLOSE = 'Close'
    HIDDEN = 'Hidden'
    REDUCE_ONLY = 'ReduceOnly'


class order_types:
    MARKET = 'MARKET'
    LIMIT = 'LIMIT'
    STOP = 'STOP'
    FOK = 'FOK'
    STOP_LIMIT = 'STOP LIMIT'


class PositionType:
    MIS = "MIS"  # Intraday with margin
    CO = "CO"  # Cover Order
    CNC = "CNC"  # Cash N Carry

    @staticmethod
    def get_all():
        return [PositionType.MIS, PositionType.CO, PositionType.CNC]

    @staticmethod
    def is_overnight_position(pos: str) -> bool:
        return pos.upper() == PositionType.CNC

class SecurityType:
    OPTION = "OPT"
    FUTURE = "FUT"
    STOCK = "STK"


class exchanges:
    COINBASE = 'Coinbase'
    BINANCE = 'Binance'
    BINANCE_FUTURES = 'Binance Futures'
    SANDBOX = 'Sandbox'
    BSE = 'BSE'
    NSE = 'NSE'
    NFO = 'NFO'


COMMON_TYPES = {
    "MONTH_CODES": ['', 'F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z'],

    "PRICE_TICKS": {1: "bid", 2: "ask", 4: "last", 6: "high", 7: "low", 9: "close", 14: "open"},
    "SIZE_TICKS": {0: "bid", 3: "ask", 5: "last", 8: "volume"},
    "RTVOL_TICKS": {"instrument": "", "ticketId": 0, "price": 0, "size": 0, "time": 0, "volume": 0, "vwap": 0,
                    "single": 0},

    "DURATION_1_HR": "3600 S",
    "DURATION_1_MIN": "60 S",
    "DURATION_1_DAY": "1 D",

    "BAR_SIZE_1_SEC": "1 secs",
    "BAR_SIZE_1_MIN": "1 min",

    "RTH_ALL": 0,
    "RTH_ONLY_TRADING_HRS": 1,

    "DATEFORMAT_STRING": 1,
    "DATEFORMAT_UNIX_TS": 2,

    "MSG_CURRENT_TIME": "currentTime",
    "MSG_COMMISSION_REPORT": "commissionReport",
    "MSG_CONNECTION_CLOSED": "connectionClosed",

    "MSG_CONTRACT_DETAILS": "contractDetails",
    "MSG_CONTRACT_DETAILS_END": "contractDetailsEnd",
    "MSG_TICK_SNAPSHOT_END": "tickSnapshotEnd",

    "MSG_TYPE_HISTORICAL_DATA": "historicalData",
    "MSG_TYPE_ACCOUNT_UPDATES": "updateAccountValue",
    "MSG_TYPE_ACCOUNT_TIME_UPDATES": "updateAccountTime",
    "MSG_TYPE_PORTFOLIO_UPDATES": "updatePortfolio",
    "MSG_TYPE_POSITION": "position",
    "MSG_TYPE_MANAGED_ACCOUNTS": "managedAccounts",

    "MSG_TYPE_NEXT_ORDER_ID": "nextValidId",
    "MSG_TYPE_OPEN_ORDER": "openOrder",
    "MSG_TYPE_ORDER_STATUS": "orderStatus",
    "MSG_TYPE_OPEN_ORDER_END": "openOrderEnd",

    "MSG_TYPE_MKT_DEPTH": "updateMktDepth",
    "MSG_TYPE_MKT_DEPTH_L2": "updateMktDepthL2",

    "MSG_TYPE_TICK_PRICE": "tickPrice",
    "MSG_TYPE_TICK_STRING": "tickString",
    "MSG_TYPE_TICK_GENERIC": "tickGeneric",
    "MSG_TYPE_TICK_SIZE": "tickSize",
    "MSG_TYPE_TICK_OPTION": "tickOptionComputation",

    "DATE_FORMAT": "%Y%m%d",
    "DATE_TIME_FORMAT": "%Y%m%d %H:%M:%S",
    "DATE_TIME_FORMAT_LONG": "%Y-%m-%d %H:%M:%S",
    "DATE_TIME_FORMAT_LONG_MILLISECS": "%Y-%m-%d %H:%M:%S.%f",
    "DATE_TIME_FORMAT_HISTORY": "%Y%m%d %H:%M:%S",
    "DATE_FORMAT_HISTORY": "%Y-%m-%d",

    "GENERIC_TICKS_NONE": "",
    "GENERIC_TICKS_RTVOLUME": "233",

    "SNAPSHOT_NONE": False,
    "SNAPSHOT_TRUE": True,

    # MARKET ORDERS
    "ORDER_TYPE_MARKET": "MKT",
    "ORDER_TYPE_MOC": "MOC",  # MARKET ON CLOSE
    "ORDER_TYPE_MOO": "MOO",  # MARKET ON OPEN
    "ORDER_TYPE_MIT": "MIT",  # MARKET IF TOUCHED

    # LIMIT ORDERS
    "ORDER_TYPE_LIMIT": "LMT",
    "ORDER_TYPE_LOC": "LOC",  # LIMIT ON CLOSE
    "ORDER_TYPE_LOO": "LOO",  # LIMIT ON OPEN
    "ORDER_TYPE_LIT": "LIT",  # LIMIT IF TOUCHED

    # STOP ORDERS
    "ORDER_TYPE_STOP": "STP",
    "ORDER_TYPE_STOP_LIMIT": "STP LMT",
    "ORDER_TYPE_TRAIL_STOP": "TRAIL",
    "ORDER_TYPE_TRAIL_STOP_LIMIT": "TRAIL LIMIT",
    "ORDER_TYPE_TRAIL_STOP_LIT": "TRAIL LIT",  # LIMIT IF TOUCHED
    "ORDER_TYPE_TRAIL_STOP_MIT": "TRAIL MIT",  # MARKET IF TOUCHED

    # MINC ORDERS
    "ORDER_TYPE_ONE_CANCELS_ALL": "OCA",
    "ORDER_TYPE_RELATIVE": "REL",
    "ORDER_TYPE_PEGGED_TO_MIDPOINT": "PEG MID",

    # ORDER ACTIONS
    "ORDER_ACTION_SELL": "SELL",
    "ORDER_ACTION_BUY": "BUY",
    "ORDER_ACTION_SHORT": "SSHORT"
}
