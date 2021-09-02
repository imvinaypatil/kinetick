#!/usr/bin/env python

__version__ = '1.0.5'
__author__ = 'vin8tech'

import os
import sys

# make indicators available as pandas extensions
# import kinetick.lib.indicators as indicators
from . import *

# check min, python version
from . import instrument
from .lib import indicators
from .lib.brokers import Webull
from .lib.brokers import Zerodha

if sys.version_info < (3, 4):
    raise SystemError("Kinetick requires Python version >= 3.4")

path = {
    "library": os.path.dirname(os.path.realpath(__file__)),
    "caller": os.path.dirname(os.path.realpath(sys.argv[0]))
}

__all__ = [
    'blotter',
    'Zerodha',
    'bot',
    'models',
    'tests',
    'risk_assessor',
    'enums',
    'lib',
    'instrument'
    'algo',
    'broker',
    'Webull',
    'utils',
    'indicators',
    'factory'
    'strategies'
    'path'
]
