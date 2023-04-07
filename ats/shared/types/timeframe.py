import enum


class Timeframe(enum.Enum):
    TICK = 'T'
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
            Timeframe.TICK,
            Timeframe.MINUTE_1,
            Timeframe.MINUTE_3,
            Timeframe.MINUTE_5,
            Timeframe.MINUTE_15,
            Timeframe.MINUTE_30,
            Timeframe.HOUR_1,
            Timeframe.HOUR_2,
            Timeframe.HOUR_3,
            Timeframe.HOUR_4,
            Timeframe.HOUR_6,
            Timeframe.HOUR_8,
            Timeframe.DAY_1
        )

    def toMinute(self):
        dic = {
            Timeframe.TICK: 1,
            Timeframe.MINUTE_1: 1,
            Timeframe.MINUTE_3: 3,
            Timeframe.MINUTE_5: 5,
            Timeframe.MINUTE_15: 15,
            Timeframe.MINUTE_30: 30,
            Timeframe.HOUR_1: 60,
            Timeframe.HOUR_2: 60 * 2,
            Timeframe.HOUR_3: 60 * 3,
            Timeframe.HOUR_4: 60 * 4,
            Timeframe.HOUR_6: 60 * 6,
            Timeframe.HOUR_8: 60 * 8,
            Timeframe.DAY_1: 60 * 24,
        }
        try:
            return dic[self]
        except KeyError:
            raise Exception(
                'Timeframe "{}" is invalid. Supported timeframes are 1m, 3m, 5m, 15m, 30m, 1h, 2h, 3h, 4h, 6h, 8h, 1D'.format(
                    self))
