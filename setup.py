#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Kinetick: Simplifying Trading
(https://github.com/vin8tech/kinetick)
Simple, event-driven algorithmic trading system written in
Python 3, that supports backtesting and live trading using
webull market data and order execution through supported brokers: zerodha.
"""

from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='kinetick',
    version='1.0.5',
    description='Simplifying Trading',
    long_description=long_description,
    url='https://github.com/imvinaypatil/kinetick',
    author='vin8tech',
    author_email='',
    license='LGPL',
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Development Status :: 4 - Beta',

        'Operating System :: OS Independent',
        'Intended Audience :: Developers',
        'Topic :: Office/Business :: Financial',
        'Topic :: Office/Business :: Financial :: Investment',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',

        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    platforms=['any'],
    keywords='kinetick algotrading algo trading zerodha brokers stocks',
    packages=find_packages(exclude=['contrib', 'docs', 'tests', 'demo', 'demos', 'examples']),
    install_requires=[
        'python-dateutil>=2.5.3',
        'numpy>=1.11.1', 'pandas>=0.22.0',
        'pytz>=2016.6.1', 'requests>=2.10.0', 'pyzmq>=15.2.1',
        'mongoengine>=0.20.0',
        'python-telegram-bot>=12.7', 'paho-mqtt>=1.5.0',
        'TA-Lib>=0.4.18', 'webull'
    ],
    dependency_links=['git+git://github.com/imvinaypatil/webull.git@slave'],
    entry_points={
        'console_scripts': [
            'sample=sample:main',
        ],
    },

    include_package_data=True,
)
