#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Modified version from QTPyLib: Quantitative Trading Python Library
# https://github.com/ranaroussi/qtpylib
# Copyright 2016-2018 Ran Aroussi
#
# Updated by vin8tech
# Copyright 2019-2021 vin8tech
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
import decimal
import logging
import random
import string
import numpy as np
import pandas as pd
from dateutil import relativedelta
from dateutil.parser import parse as parse_date
from pytz import timezone
from datetime import datetime, timedelta
from pandas import to_datetime as pd_to_datetime
import time
import os
import sys
from stat import S_IWRITE
from math import ceil

decimal.getcontext().prec = 5


def rand_pass(size):
    # Takes random choices from
    # ascii_letters and digits
    generate_pass = ''.join([random.choice(string.ascii_uppercase +
                                           string.ascii_lowercase +
                                           string.digits)
                             for n in range(size)])

    return generate_pass


# ---------------------------------------------

def create_logger(name, level=logging.WARNING):
    """:Return: a logger with the given `name` and optional `level`."""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s'))
    logger.addHandler(handler)
    logger.propagate = False
    return logger


# ---------------------------------------------

def local_to_utc(df):
    """ converts naive (usually local) timezone to UTC) """
    try:
        offset_hour = -(datetime.now() - datetime.utcnow()).seconds
    except:
        offset_hour = time.altzone if time.daylight else time.timezone

    offset_hour = offset_hour // 3600
    offset_hour = offset_hour if offset_hour < 10 else offset_hour // 10

    df = df.copy()
    df.index = pd_to_datetime(df.index, utc=True) + timedelta(hours=offset_hour)

    return df


# ---------------------------------------------

class make_object:
    """ create object from dict """

    def __init__(self, **entries):
        self.__dict__.update(entries)


# ---------------------------------------------


def read_single_argv(param, default=None):
    args = " ".join(sys.argv).strip().split(param)
    if len(args) > 1:
        args = args[1].strip().split(" ")[0]
        return args if "-" not in args else None
    return default


# ---------------------------------------------


def multi_shift(df, window):
    """
    get last N rows RELATIVE to another row in pandas
    http://stackoverflow.com/questions/25724056/how-to-get-last-n-rows-relative-to-another-row-in-pandas-vector-solution
    """
    if isinstance(df, pd.Series):
        df = pd.DataFrame(df)

    dfs = [df.shift(i) for i in np.arange(window)]
    for ix, df_item in enumerate(dfs[1:]):
        dfs[ix + 1].columns = [str(col) for col in df_item.columns + str(ix + 1)]
    return pd.concat(dfs, 1, sort=True)  # .apply(list, 1)


# ---------------------------------------------


def is_number(string):
    """ checks if a string is a number (int/float) """
    string = str(string)
    if string.isnumeric():
        return True
    try:
        float(string)
        return True
    except ValueError:
        return False


# ---------------------------------------------

def to_decimal(number, points=None):
    """ convert datatypes into Decimals """
    if not is_number(number):
        return number

    number = float(decimal.Decimal(number * 1.))  # can't Decimal an int
    if is_number(points):
        return round(number, points)
    return number


# ---------------------------------------------

def week_started_date(as_datetime=False):
    today = datetime.utcnow()
    start = today - timedelta((today.weekday() + 1) % 7)
    dt = start + relativedelta.relativedelta(weekday=relativedelta.SU(-1))

    if as_datetime:
        return dt

    return dt.strftime("%Y-%m-%d")


# ---------------------------------------------

def create_contract_tuple(instrument):
    """ create contract tuple """
    if isinstance(instrument, str):
        instrument = instrument.upper()

        if "FUT." not in instrument:
            # symbol stock
            instrument = (instrument, "STK", "NSE", "INR", "", 0.0, "")

        else:
            raise Exception("Unsupported contract type!")

    # tuples without strike/right
    elif len(instrument) <= 7:
        instrument_list = list(instrument)
        if len(instrument_list) < 3:
            instrument_list.append("NSE")
        if len(instrument_list) < 4:
            instrument_list.append("INR")
        if len(instrument_list) < 5:
            instrument_list.append("")
        if len(instrument_list) < 6:
            instrument_list.append(0.0)
        if len(instrument_list) < 7:
            instrument_list.append("")

        try:
            instrument_list[4] = int(instrument_list[4])
        except Exception as e:
            pass

        instrument_list[5] = 0. if isinstance(instrument_list[5], str) \
            else float(instrument_list[5])

        instrument = tuple(instrument_list)

    return instrument


# ---------------------------------------------

def gen_symbol_group(sym):
    sym = sym.strip()

    if "_FUT" in sym:
        sym = sym.split("_FUT")
        return sym[0][:-5] + "_F"

    elif "_CASH" in sym:
        return "CASH"

    if "_FOP" in sym or "_OPT" in sym:
        return sym[:-12]

    return sym


# ---------------------------------------------

def gen_asset_class(sym):
    sym_class = str(sym).split("_")
    if len(sym_class) > 1:
        return sym_class[-1].replace("CASH", "CSH")
    return "STK"


# ---------------------------------------------

def mark_options_values(data):
    if isinstance(data, dict):
        data['opt_price'] = data.pop('price')
        data['opt_underlying'] = data.pop('underlying')
        data['opt_dividend'] = data.pop('dividend')
        data['opt_volume'] = data.pop('volume')
        data['opt_iv'] = data.pop('iv')
        data['opt_oi'] = data.pop('oi')
        data['opt_delta'] = data.pop('delta')
        data['opt_gamma'] = data.pop('gamma')
        data['opt_vega'] = data.pop('vega')
        data['opt_theta'] = data.pop('theta')
        return data

    elif isinstance(data, pd.DataFrame):
        return data.rename(columns={
            'price': 'opt_price',
            'underlying': 'opt_underlying',
            'dividend': 'opt_dividend',
            'volume': 'opt_volume',
            'iv': 'opt_iv',
            'oi': 'opt_oi',
            'delta': 'opt_delta',
            'gamma': 'opt_gamma',
            'vega': 'opt_vega',
            'theta': 'opt_theta'
        })

    return data


# ---------------------------------------------

def force_options_columns(data):
    opt_cols = ['opt_price', 'opt_underlying', 'opt_dividend', 'opt_volume',
                'opt_iv', 'opt_oi', 'opt_delta', 'opt_gamma', 'opt_vega', 'opt_theta']

    if isinstance(data, dict):
        if not set(opt_cols).issubset(data.keys()):
            data['opt_price'] = None
            data['opt_underlying'] = None
            data['opt_dividend'] = None
            data['opt_volume'] = None
            data['opt_iv'] = None
            data['opt_oi'] = None
            data['opt_delta'] = None
            data['opt_gamma'] = None
            data['opt_vega'] = None
            data['opt_theta'] = None

    elif isinstance(data, pd.DataFrame):
        if not set(opt_cols).issubset(data.columns):
            data.loc[:, 'opt_price'] = np.nan
            data.loc[:, 'opt_underlying'] = np.nan
            data.loc[:, 'opt_dividend'] = np.nan
            data.loc[:, 'opt_volume'] = np.nan
            data.loc[:, 'opt_iv'] = np.nan
            data.loc[:, 'opt_oi'] = np.nan
            data.loc[:, 'opt_delta'] = np.nan
            data.loc[:, 'opt_gamma'] = np.nan
            data.loc[:, 'opt_vega'] = np.nan
            data.loc[:, 'opt_theta'] = np.nan

    return data


# ---------------------------------------------

def chmod(f):
    """ change mod to writeable """
    try:
        os.chmod(f, S_IWRITE)  # windows (cover all)
    except Exception as e:
        pass
    try:
        os.chmod(f, 0o777)  # *nix
    except Exception as e:
        pass


# ---------------------------------------------

def as_dict(df, ix=':'):
    """ converts df to dict and adds a datetime field if df is datetime """
    if isinstance(df.index, pd.DatetimeIndex):
        df['datetime'] = df.index
    return df.to_dict(orient='records')[ix]


# ---------------------------------------------

def ib_duration_str(start_date=None):
    """
    Get a datetime object or a epoch timestamp and return
    an IB-compatible durationStr for reqHistoricalData()
    """
    now = datetime.utcnow()

    if is_number(start_date):
        diff = now - datetime.fromtimestamp(float(start_date))
    elif isinstance(start_date, str):
        diff = now - parse_date(start_date)
    elif isinstance(start_date, datetime):
        diff = now - start_date
    else:
        return None

    # get diff
    second_diff = diff.seconds
    day_diff = diff.days

    # small diff?
    if day_diff < 0 or second_diff < 60:
        return None

    # return str(second_diff)+ " S"
    if day_diff == 0 and second_diff > 1:
        return str(second_diff) + " S"
    if 31 > day_diff > 0:
        return str(day_diff) + " D"
    if 365 > day_diff > 31:
        return str(ceil(day_diff / 30)) + " M"

    return str(ceil(day_diff / 365)) + " Y"


# ---------------------------------------------

def wb_resolution(res="T"):
    periods = ("".join([s for s in res if s.isdigit()]))
    periods = int(periods) if len(periods) else 0
    res = res.lower()

    if periods > 0:
        if "min" in res or 'm' in res:
            return "m" + str(periods), periods
        elif "hour" in res or "H" in res:
            mins = periods * 60
            return "m" + str(mins), mins
    else:
        if "m" in res:
            return "m1", 1
        elif "day" in res or "d" in res:
            return "d1", 24 * 60
        elif "week" in res or "W" in res:
            # if periods > 1:
            #     raise Exception('week %s not supported', periods)
            return "w1", 7 * 24 * 60
        elif "month" in res:
            return "mth1", 31 * 24 * 60
        elif "quarter" in res or "q" in res:
            return "mth3", 3 * 31 * 60
        elif "year" in res or "y" in res:
            return "y1", 365 * 24 * 60
    raise Exception("unknown resolution provided")


# ---------------------------------------------
def wb_lookback_str(start_date=None, end_date=datetime.utcnow(), interval=None):
    now = end_date

    if is_number(start_date):
        diff = now - datetime.fromtimestamp(float(start_date))
    elif isinstance(start_date, str):
        diff = now - parse_date(start_date)
    elif isinstance(start_date, datetime):
        diff = now - start_date
    else:
        return None

    # get diff
    day_diff = diff.days
    min_diff = int(diff.seconds / 60)

    # small diff?
    if day_diff < 0 and min_diff < 1:
        return 0

    # return str(second_diff)+ " S"
    if day_diff == 0 and min_diff > 1:
        return min_diff / interval
    if 31 > day_diff > 0:
        return (min_diff + day_diff * 24 * 60) / interval
    if 365 > day_diff > 31:
        return (min_diff + day_diff * 24 * 60) / interval

    return (min_diff + day_diff * 24 * 60) / interval


# ---------------------------------------------

def datetime64_to_datetime(dt):
    """ convert numpy's datetime64 to datetime """
    dt64 = np.datetime64(dt)
    ts = (dt64 - np.datetime64('1970-01-01T00:00:00')) / np.timedelta64(1, 's')
    return datetime.utcfromtimestamp(ts)


# ---------------------------------------------

def round_to_fraction(val, res, decimals=None):
    """ round to closest resolution """
    if val is None:
        return 0.0
    if decimals is None and "." in str(res):
        decimals = len(str(res).split('.')[1])

    return round(round(val / res) * res, decimals)


# ---------------------------------------------

def backdate(res, date=None, as_datetime=False, fmt='%Y-%m-%d'):
    """ get past date based on currect date """
    if res is None:
        return None

    if date is None:
        date = datetime.now()
    else:
        try:
            date = parse_date(date)
        except Exception as e:
            pass

    new_date = date

    periods = int("".join([s for s in res if s.isdigit()]))

    if periods > 0:

        if "K" in res:
            new_date = date - timedelta(microseconds=periods)
        elif "S" in res:
            new_date = date - timedelta(seconds=periods)
        elif "T" in res:
            new_date = date - timedelta(minutes=periods)
        elif "H" in res or "V" in res:
            new_date = date - timedelta(hours=periods)
        elif "W" in res:
            new_date = date - timedelta(weeks=periods)
        else:  # days
            new_date = date - timedelta(days=periods)

        # not a week day:
        while new_date.weekday() > 4:  # Mon-Fri are 0-4
            new_date = backdate(res="1D", date=new_date, as_datetime=True)

    if as_datetime:
        return new_date

    return new_date.strftime(fmt)


# ---------------------------------------------

def previous_weekday(day=None, as_datetime=False):
    """ get the most recent business day """
    if day is None:
        day = datetime.now()
    else:
        day = datetime.strptime(day, '%Y-%m-%d')

    day -= timedelta(days=1)
    while day.weekday() > 4:  # Mon-Fri are 0-4
        day -= timedelta(days=1)

    if as_datetime:
        return day
    return day.strftime("%Y-%m-%d")


# ---------------------------------------------

def is_third_friday(day=None):
    """ check if day is month's 3rd friday """
    day = day if day is not None else datetime.now()
    defacto_friday = (day.weekday() == 4) or (
            day.weekday() == 3 and day.hour() >= 17)
    return defacto_friday and 14 < day.day < 22


# ---------------------------------------------

def after_third_friday(day=None):
    """ check if day is after month's 3rd friday """
    day = day if day is not None else datetime.now()
    now = day.replace(day=1, hour=16, minute=0, second=0, microsecond=0)
    now += relativedelta.relativedelta(weeks=2, weekday=relativedelta.FR)
    return day > now


# =============================================
# timezone utilities
# =============================================

def get_timezone(as_timedelta=False):
    """ utility to get the machine's timezone """
    try:
        offset_hour = -(time.altzone if time.daylight else time.timezone)
    except Exception as e:
        offset_hour = -(datetime.now() -
                        datetime.utcnow()).seconds

    offset_hour = offset_hour // 3600
    offset_hour = offset_hour if offset_hour < 10 else offset_hour // 10

    if as_timedelta:
        return timedelta(hours=offset_hour)

    return 'Etc/GMT%+d' % offset_hour


# ---------------------------------------------

def datetime_to_timezone(date, tz="UTC"):
    """ convert naive datetime to timezone-aware datetime """
    if not date.tzinfo:
        date = date.replace(tzinfo=timezone(get_timezone()))
    return date.astimezone(timezone(tz))


# ---------------------------------------------


def convert_timezone(date_str, tz_from, tz_to="UTC", fmt=None):
    """ get timezone as tz_offset """
    tz_offset = datetime_to_timezone(
        datetime.now(), tz=tz_from).strftime('%z')
    tz_offset = tz_offset[:3] + ':' + tz_offset[3:]

    date = parse_date(str(date_str) + tz_offset)
    if tz_from != tz_to:
        date = datetime_to_timezone(date, tz_to)

    if isinstance(fmt, str):
        return date.strftime(fmt)
    return date


# ---------------------------------------------

def set_timezone(data, tz=None, from_local=False):
    """ change the timeozone to specified one without converting """
    # pandas object?
    if isinstance(data, pd.DataFrame) | isinstance(data, pd.Series):
        try:
            try:
                data.index = data.index.tz_convert(tz)
            except Exception as e:
                if from_local:
                    data.index = data.index.tz_localize(
                        get_timezone()).tz_convert(tz)
                else:
                    data.index = data.index.tz_localize('UTC').tz_convert(tz)
        except Exception as e:
            pass

    # not pandas...
    else:
        if isinstance(data, str):
            data = parse_date(data)
        try:
            try:
                data = data.astimezone(tz)
            except Exception as e:
                data = timezone('UTC').localize(data).astimezone(timezone(tz))
        except Exception as e:
            pass

    return data


# ---------------------------------------------

def fix_timezone(df, freq, tz=None):
    """ set timezone for pandas """
    index_name = df.index.name

    # fix timezone
    if isinstance(df.index[0], str):
        # timezone df exists
        if ("-" in df.index[0][-6:]) | ("+" in df.index[0][-6:]):
            df.index = pd.to_datetime(df.index, utc=False)
            df.index = df.index.tz_localize('UTC').tz_convert(tz)

        # no timezone df - do some resampling
        else:
            # original range
            start_range = df.index[0]
            end_range = df.index[-1]

            # resample df
            df.index = pd.to_datetime(df.index, utc=True)
            df = resample(df, resolution=freq, ffill=False, dropna=False)

            # create date range
            new_freq = ''.join(i for i in freq if not i.isdigit())
            rng = pd.date_range(start=start_range,
                                end=end_range, tz=tz, freq=new_freq)

            # assign date range to df and drop empty rows
            df.index = rng
            df.dropna(inplace=True)

    # finalize timezone (also for timezone-aware df)
    df = set_timezone(df, tz=tz)

    df.index.name = index_name
    return df


# =============================================
# resample based on time / tick count
# =============================================

def resample(data, resolution="1T", tz=None, ffill=False, dropna=False,
             sync_last_timestamp=True):
    def __finalize(data, tz=None):
        # figure out timezone
        try:
            tz = data.index.tz if tz is None else tz
        except Exception as e:
            pass

        if str(tz) != 'None':
            try:
                data.index = data.index.tz_convert(tz)
            except Exception as e:
                data.index = data.index.tz_localize('UTC').tz_convert(tz)

        # sort by index (datetime)
        data.sort_index(inplace=True)

        # drop duplicate rows per instrument
        data.loc[:, '_idx_'] = data.index
        data.drop_duplicates(
            subset=['_idx_', 'symbol', 'symbol_group', 'asset_class'],
            keep='last', inplace=True)
        data.drop('_idx_', axis=1, inplace=True)

        return data
        # return data[~data.index.duplicated(keep='last')]

    def __resample_ticks(data, freq=1000, by='last'):
        """
        function that re-samples tick data into an N-tick or N-volume OHLC format

        df = pandas pd.dataframe of raw tick data
        freq = resoltuin grouping
        by = the column name to resample by
        """

        data.fillna(value=np.nan, inplace=True)

        # get only ticks and fill missing data
        try:
            df = data[['last', 'lastsize', 'opt_underlying', 'opt_price',
                       'opt_dividend', 'opt_volume', 'opt_iv', 'opt_oi',
                       'opt_delta', 'opt_gamma', 'opt_theta', 'opt_vega']].copy()
            price_col = 'last'
            size_col = 'lastsize'
        except Exception as e:
            df = data[['close', 'volume', 'opt_underlying', 'opt_price',
                       'opt_dividend', 'opt_volume', 'opt_iv', 'opt_oi',
                       'opt_delta', 'opt_gamma', 'opt_theta', 'opt_vega']].copy()
            price_col = 'close'
            size_col = 'volume'

        # add group indicator evey N df
        if by == 'size' or by == 'lastsize' or by == 'volume':
            df['cumvol'] = df[size_col].cumsum()
            df['mark'] = round(
                round(round(df['cumvol'] / .1) * .1, 2) / freq) * freq
            df['diff'] = df['mark'].diff().fillna(0).astype(int)
            df['grp'] = np.where(df['diff'] >= freq - 1,
                                 (df['mark'] / freq), np.nan)
        else:
            df['grp'] = [np.nan if i %
                                   freq else i for i in range(len(df[price_col]))]

        df.loc[:1, 'grp'] = 0

        df.fillna(method='ffill', inplace=True)

        # print(df[['lastsize', 'cumvol', 'mark', 'diff', 'grp']].tail(1))

        # place timestamp index in T colums
        # (to be used as future df index)
        df['T'] = df.index

        # make group the index
        df = df.set_index('grp')

        # grop df
        groupped = df.groupby(df.index, sort=False)

        # build ohlc(v) pd.dataframe from new grp column
        newdf = pd.DataFrame({
            'open': groupped[price_col].first(),
            'high': groupped[price_col].max(),
            'low': groupped[price_col].min(),
            'close': groupped[price_col].last(),
            'volume': groupped[size_col].sum(),

            'opt_price': groupped['opt_price'].last(),
            'opt_underlying': groupped['opt_underlying'].last(),
            'opt_dividend': groupped['opt_dividend'].last(),
            'opt_volume': groupped['opt_volume'].last(),
            'opt_iv': groupped['opt_iv'].last(),
            'opt_oi': groupped['opt_oi'].last(),
            'opt_delta': groupped['opt_delta'].last(),
            'opt_gamma': groupped['opt_gamma'].last(),
            'opt_theta': groupped['opt_theta'].last(),
            'opt_vega': groupped['opt_vega'].last()
        })

        # set index to timestamp
        newdf['datetime'] = groupped.T.head(1)
        newdf.set_index(['datetime'], inplace=True)

        return newdf

    if data.empty:
        return __finalize(data, tz)

    # ---------------------------------------------
    # force same last timestamp to all symbols before resampling
    if sync_last_timestamp:
        data.loc[:, '_idx_'] = data.index
        start_date = str(data.groupby(["symbol"])[
                             ['_idx_']].min().max().values[-1]).replace('T', ' ')
        end_date = str(data.groupby(["symbol"])[
                           ['_idx_']].max().min().values[-1]).replace('T', ' ')

        data = data[(data.index <= end_date)].drop_duplicates(
            subset=['_idx_', 'symbol', 'symbol_group', 'asset_class'],
            keep='first')

        # try also sync start date
        trimmed = data[data.index >= start_date]
        if not trimmed.empty:
            data = trimmed

    # ---------------------------------------------
    # resample
    periods = int("".join([s for s in resolution if s.isdigit()]))
    meta_data = data.groupby(["symbol"])[
        ['symbol', 'symbol_group', 'asset_class']].last()
    combined = []

    if "K" in resolution:
        if periods > 1:
            for sym in meta_data.index.values:
                symdata = __resample_ticks(data[data['symbol'] == sym].copy(),
                                           freq=periods, by='last')
                symdata['symbol'] = sym
                symdata['symbol_group'] = meta_data[
                    meta_data.index == sym]['symbol_group'].values[0]
                symdata['asset_class'] = meta_data[
                    meta_data.index == sym]['asset_class'].values[0]

                # cleanup
                symdata.dropna(inplace=True, subset=[
                    'open', 'high', 'low', 'close', 'volume'])
                if sym[-3:] in ("OPT", "FOP"):
                    symdata.dropna(inplace=True)

                combined.append(symdata)

            data = pd.concat(combined, sort=True)

    elif "V" in resolution:
        if periods > 1:
            for sym in meta_data.index.values:
                symdata = __resample_ticks(data[data['symbol'] == sym].copy(),
                                           freq=periods, by='lastsize')
                symdata['symbol'] = sym
                symdata['symbol_group'] = meta_data[
                    meta_data.index == sym]['symbol_group'].values[0]
                symdata['asset_class'] = meta_data[
                    meta_data.index == sym]['asset_class'].values[0]

                # cleanup
                symdata.dropna(inplace=True, subset=[
                    'open', 'high', 'low', 'close', 'volume'])
                if sym[-3:] in ("OPT", "FOP"):
                    symdata.dropna(inplace=True)

                combined.append(symdata)

            data = pd.concat(combined, sort=True)

    # continue...
    else:
        ticks_ohlc_dict = {
            'lastsize': 'sum',
            'opt_price': 'last',
            'opt_underlying': 'last',
            'opt_dividend': 'last',
            'opt_volume': 'last',
            'opt_iv': 'last',
            'opt_oi': 'last',
            'opt_delta': 'last',
            'opt_gamma': 'last',
            'opt_theta': 'last',
            'opt_vega': 'last'
        }
        bars_ohlc_dict = {
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
            'vwap': 'last',
            'opt_price': 'last',
            'opt_underlying': 'last',
            'opt_dividend': 'last',
            'opt_volume': 'last',
            'opt_iv': 'last',
            'opt_oi': 'last',
            'opt_delta': 'last',
            'opt_gamma': 'last',
            'opt_theta': 'last',
            'opt_vega': 'last'
        }

        for sym in meta_data.index.values:

            if "last" in data.columns:
                tick_dict = {}
                for col in data[data['symbol'] == sym].columns:
                    if col in ticks_ohlc_dict.keys():
                        tick_dict[col] = ticks_ohlc_dict[col]

                ohlc = data[data['symbol'] == sym]['last'].resample(
                    resolution).ohlc()
                symdata = data[data['symbol'] == sym].resample(
                    resolution).apply(tick_dict).fillna(value=np.nan)
                symdata.rename(
                    columns={'lastsize': 'volume'}, inplace=True)

                symdata['open'] = ohlc['open']
                symdata['high'] = ohlc['high']
                symdata['low'] = ohlc['low']
                symdata['close'] = ohlc['close']

            else:
                bar_dict = {}
                for col in data[data['symbol'] == sym].columns:
                    if col in bars_ohlc_dict.keys():
                        bar_dict[col] = bars_ohlc_dict[col]

                original_length = len(data[data['symbol'] == sym])
                symdata = data[data['symbol'] == sym].resample(
                    resolution).apply(bar_dict).fillna(value=np.nan)
                # deal with new rows caused by resample
                if len(symdata) > original_length and ffill:
                    # volume is 0 on rows created using resample
                    symdata['volume'].fillna(0, inplace=True)
                    symdata.ffill(inplace=True)

                    # no fill / return original index
                    if ffill:
                        symdata['open'] = np.where(symdata['volume'] <= 0,
                                                   symdata['close'], symdata['open'])
                        symdata['high'] = np.where(symdata['volume'] <= 0,
                                                   symdata['close'], symdata['high'])
                        symdata['low'] = np.where(symdata['volume'] <= 0,
                                                  symdata['close'], symdata['low'])
                    else:  # yes, it's unreachable
                        symdata['open'] = np.where(symdata['volume'] <= 0,
                                                   np.nan, symdata['open'])
                        symdata['high'] = np.where(symdata['volume'] <= 0,
                                                   np.nan, symdata['high'])
                        symdata['low'] = np.where(symdata['volume'] <= 0,
                                                  np.nan, symdata['low'])
                        symdata['close'] = np.where(symdata['volume'] <= 0,
                                                    np.nan, symdata['close'])

                # drop NANs
                if dropna:
                    symdata.dropna(inplace=True)

            symdata['symbol'] = sym
            symdata['symbol_group'] = meta_data[meta_data.index ==
                                                sym]['symbol_group'].values[0]
            symdata['asset_class'] = meta_data[meta_data.index ==
                                               sym]['asset_class'].values[0]

            # cleanup
            symdata.dropna(inplace=True, subset=[
                'open', 'high', 'low', 'close', 'volume'])
            if sym[-3:] in ("OPT", "FOP"):
                symdata.dropna(inplace=True)

            combined.append(symdata)

        data = pd.concat(combined, sort=True)
        data['volume'] = data['volume'].astype(int)

    return __finalize(data, tz)


# =============================================
# store event in a temp data store
# =============================================

class DataStore():

    def __init__(self, output_file=None):
        self.auto = None
        self.recorded = None
        self.output_file = output_file
        self.rows = []

    def record(self, timestamp, *args, **kwargs):
        """ add custom data to data store """
        if self.output_file is None:
            return

        data = {'datetime': timestamp}

        # append all data
        if len(args) == 1:
            if isinstance(args[0], dict):
                data.update(dict(args[0]))
            elif isinstance(args[0], pd.DataFrame):
                data.update(args[0][-1:].to_dict(orient='records')[0])

        # add kwargs
        if kwargs:
            data.update(dict(kwargs))

        data['datetime'] = timestamp
        # self.rows.append(pd.DataFrame(data=data, index=[timestamp]))

        new_data = {}
        if "symbol" not in data.keys():
            new_data = dict(data)
        else:
            sym = data["symbol"]
            new_data["symbol"] = data["symbol"]
            for key in data.keys():
                if key not in ['datetime', 'symbol_group', 'asset_class']:
                    new_data[sym + '_' + str(key).upper()] = data[key]

        new_data['datetime'] = timestamp

        # append to rows
        self.rows.append(pd.DataFrame(data=new_data, index=[timestamp]))

        # create dataframe
        recorded = pd.concat(self.rows, sort=True)

        if "symbol" not in recorded.columns:
            return

        # group by symbol
        recorded['datetime'] = recorded.index
        data = recorded.groupby(['symbol', 'datetime'], as_index=False).sum()
        data.set_index('datetime', inplace=True)

        symbols = data['symbol'].unique().tolist()
        data.drop(columns=['symbol'], inplace=True)

        # cleanup:

        # remove symbols
        recorded.drop(['symbol'] + [sym + '_SYMBOL' for sym in symbols],
                      axis=1, inplace=True)

        # remove non-option data if not working with options
        for sym in symbols:
            try:
                opt_cols = recorded.columns[
                    recorded.columns.str.startswith(sym + '_OPT_')].tolist()
                if len(opt_cols) == len(recorded[opt_cols].isnull().all()):
                    recorded.drop(opt_cols, axis=1, inplace=True)
            except Exception as e:
                pass

        # group df
        recorded = recorded.groupby(recorded['datetime']).first()

        # shift position
        for sym in symbols:
            recorded[sym + '_POSITION'] = recorded[sym + '_POSITION'
                                                   ].shift(1).fillna(0)

        # make this public
        self.recorded = recorded.copy()

        # cleanup columns names before saving...
        recorded.columns = [col.replace('_FUT_', '_').replace(
            '_OPT_OPT_', '_OPT_') for col in recorded.columns]

        # save
        if ".csv" in self.output_file:
            recorded.to_csv(self.output_file)
        elif ".h5" in self.output_file:
            recorded.to_hdf(self.output_file, 0)
        elif (".pickle" in self.output_file) | (".pkl" in self.output_file):
            recorded.to_pickle(self.output_file)

        chmod(self.output_file)


def create_continuous_contract(df, resolution="1T"):
    def _merge_contracts(m1, m2):

        if m1 is None:
            return m2

        try:
            # rollver by date
            roll_date = m1['expiry'].unique()[-1]
        except Exception as e:
            # rollover by volume
            combined = m1.merge(m2, left_index=True, right_index=True)
            m_highest = combined['volume_y'] > combined['volume_x']
            if len(m_highest.index) == 0:
                return m1  # didn't rolled over yet
            roll_date = m_highest[m_highest].index[-1]

        return pd.concat([m1[m1.index <= roll_date], m2[m2.index > roll_date]
                          ], sort=True)

    def _continuous_contract_flags(daily_df):
        # grab expirations
        expirations = list(daily_df['expiry'].dropna().unique())
        expirations.sort()

        # set continuous contract markets
        flags = None
        for expiration in expirations:
            new_contract = daily_df[daily_df['expiry'] == expiration].copy()
            flags = _merge_contracts(flags, new_contract)

        # add gap
        flags['gap'] = 0
        for expiration in expirations:
            try:
                minidf = daily_df[daily_df.index ==
                                  expiration][['symbol', 'expiry', 'diff']]
                expiry = flags[
                    (flags.index > expiration) & (
                            flags['expiry'] >= expiration)
                    ]['expiry'][0]
                gap = minidf[minidf['expiry'] == expiry]['diff'][0]
                flags.loc[flags.index <= expiration, 'gap'] = gap
            except Exception as e:
                pass

        flags = flags[flags['symbol'].isin(flags['symbol'].unique())]

        # single row df won't resample
        if len(flags.index) <= 1:
            flags = pd.DataFrame(
                index=pd.date_range(start=flags[0:1].index[0],
                                    periods=24, freq="1H"), data=flags[
                    ['symbol', 'expiry', 'gap']]).ffill()

        flags['expiry'] = pd.to_datetime(flags['expiry'], utc=True)
        return flags[['symbol', 'expiry', 'gap']]

    # gonna need this later
    df = df.copy()
    df['dt'] = df.index

    # work with daily data
    daily_df = df.groupby('symbol').resample("D").last().dropna(how='all')
    daily_df.index = daily_df.index.droplevel()
    daily_df.sort_index(inplace=True)
    try:
        daily_df['diff'] = daily_df['close'].diff()
    except Exception as e:
        daily_df['diff'] = daily_df['last'].diff()

    # build flags
    flags = _continuous_contract_flags(daily_df)

    # resample back to original
    if "K" in resolution or "V" in resolution or "S" in resolution:
        flags = flags.resample('S').last().ffill(
        ).reindex(df.index.unique()).ffill()
    else:
        flags = flags.resample('T').last().ffill(
        ).reindex(df.index.unique()).ffill()
    flags['dt'] = flags.index

    # build contract
    contract = pd.merge(df, flags, how='left', on=[
        'dt', 'symbol']).ffill()
    contract.set_index('dt', inplace=True)

    contract = contract[contract.expiry_y == contract.expiry_x]
    contract['expiry'] = contract['expiry_y']
    contract.drop(['expiry_y', 'expiry_x'], axis=1, inplace=True)

    try:
        contract['open'] = contract['open'] + contract['gap']
        contract['high'] = contract['high'] + contract['gap']
        contract['low'] = contract['low'] + contract['gap']
        contract['close'] = contract['close'] + contract['gap']
        # contract['volume'] = df['volume'].resample("D").sum()
    except Exception as e:
        contract['last'] = contract['last'] + contract['gap']

    contract.drop(['gap'], axis=1, inplace=True)

    return contract
