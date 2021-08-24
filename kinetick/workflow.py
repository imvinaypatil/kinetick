#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# QTPyLib: Quantitative Trading Python Library
# https://github.com/ranaroussi/qtpylib
#
# Copyright 2016-2018 Ran Aroussi
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

_BARS_COLSMAP = {
    'open': 'open',
    'high': 'high',
    'low': 'low',
    'close': 'close',
    'volume': 'volume',
    'opt_price': 'opt_price',
    'opt_underlying': 'opt_underlying',
    'opt_dividend': 'opt_dividend',
    'opt_volume': 'opt_volume',
    'opt_iv': 'opt_iv',
    'opt_oi': 'opt_oi',
    'opt_delta': 'opt_delta',
    'opt_gamma': 'opt_gamma',
    'opt_vega': 'opt_vega',
    'opt_theta': 'opt_theta'
}
_TICKS_COLSMAP = {
    'bid': 'bid',
    'bidsize': 'bidsize',
    'ask': 'ask',
    'asksize': 'asksize',
    'last': 'last',
    'lastsize': 'lastsize',
    'opt_price': 'opt_price',
    'opt_underlying': 'opt_underlying',
    'opt_dividend': 'opt_dividend',
    'opt_volume': 'opt_volume',
    'opt_iv': 'opt_iv',
    'opt_oi': 'opt_oi',
    'opt_delta': 'opt_delta',
    'opt_gamma': 'opt_gamma',
    'opt_vega': 'opt_vega',
    'opt_theta': 'opt_theta'
}

# ---------------------------------------------


def validate_columns(df, kind="BAR", raise_errors=True):
    global _TICKS_COLSMAP, _BARS_COLSMAP
    # validate columns
    if "asset_class" not in df.columns:
        if raise_errors:
            raise ValueError('Column asset_class not found')
        return False

    is_option = "OPT" in list(df['asset_class'].unique())

    colsmap = _TICKS_COLSMAP if kind == "TICK" else _BARS_COLSMAP

    for el in colsmap:
        col = colsmap[el]
        if col not in df.columns:
            if "opt_" in col and is_option:
                if raise_errors:
                    raise ValueError('Column %s not found' % el)
                return False
            elif "opt_" not in col and not is_option:
                if raise_errors:
                    raise ValueError('Column %s not found' % el)
                return False
    return True


# =============================================
# data analyze methods
# =============================================

def analyze_portfolio(file):
    """ analyze portfolio (TBD) """
    pass
