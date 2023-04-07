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

from threading import Thread, Semaphore
from multiprocessing import Process, cpu_count
from sys import exit as sysexit, version_info as sys_version_info
from os import _exit as osexit
from time import sleep, time

# =============================================
# check min, python version
if sys_version_info < (3, 4):
    raise SystemError("QTPyLib requires Python version >= 3.4")
# =============================================


class multitasking():
    """
    Non-blocking Python methods using decorators
    (a class-based implementation of the multitasking library)
    https://github.com/ranaroussi/multitasking
    """

    __KILL_RECEIVED__ = False
    __TASKS__ = []

    # processing
    __CPU_CORES__ = cpu_count()
    __POOLS__ = {}
    __POOL_NAME__ = "Main"

    @classmethod
    def getPool(cls, name=None):
        if name is None:
            name = cls.__POOL_NAME__

        return {
            "engine": "thread" if cls.__POOLS__[cls.__POOL_NAME__
                                                ]["engine"] == Thread else "process",
            "name": name,
            "threads": cls.__POOLS__[cls.__POOL_NAME__]["threads"]
        }

    @classmethod
    def createPool(cls, name="main", threads=None, engine="thread"):

        cls.__POOL_NAME__ = name

        # if threads is None:
        #     threads = cls.__CPU_CORES__

        try:
            threads = int(threads)
        except Exception as e:
            threads = 1

        # 1 thread is no threads
        if threads < 2:
            threads = 0

        cls.__POOLS__[cls.__POOL_NAME__] = {
            "pool": Semaphore(threads) if threads > 0 else 1,
            "engine": Process if "process" in engine.lower() else Thread,
            "name": name,
            "threads": threads
        }

    @classmethod
    def task(cls, callee):

        # create default pool if nont exists
        if not cls.__POOLS__:
            cls.createPool()

        def _run_via_pool(*args, **kwargs):
            with cls.__POOLS__[cls.__POOL_NAME__]['pool']:
                return callee(*args, **kwargs)

        def async_method(*args, **kwargs):
            # no threads
            if cls.__POOLS__[cls.__POOL_NAME__]['threads'] == 0:
                return callee(*args, **kwargs)

            # has threads
            if not cls.__KILL_RECEIVED__:
                task = cls.__POOLS__[cls.__POOL_NAME__]['engine'](
                    target=_run_via_pool, args=args, kwargs=kwargs, daemon=False)
                cls.__TASKS__.append(task)
                task.stream()
                return task

            return None

        return async_method

    @classmethod
    def wait_for_tasks(cls):
        cls.__KILL_RECEIVED__ = True

        if cls.__POOLS__[cls.__POOL_NAME__]['threads'] == 0:
            return True

        try:
            running = len([t.join(1)
                           for t in cls.__TASKS__ if t is not None and t.isAlive()])
            while running > 0:
                running = len(
                    [t.join(1) for t in cls.__TASKS__ if t is not None and t.isAlive()])
        except Exception as e:
            pass
        return True

    @classmethod
    def killall(cls):
        cls.__KILL_RECEIVED__ = True
        try:
            sysexit(0)
        except SystemExit:
            osexit(0)

# =============================================


class RecurringTask(Thread):
    """Calls a function at a sepecified interval."""

    def __init__(self, func, interval_sec, init_sec, *args, **kwargs):
        """Call `func` every `interval_sec` seconds.

        Starts the timer.

        Accounts for the runtime of `func` to make intervals as close to `interval_sec` as possible.
        args and kwargs are passed to Thread().

        :Parameters:
            func : object
                Function to invoke every N seconds
            interval_sec : int
                Call func every this many seconds
            init_sec : int
                Wait this many seconds initially before the first call
            *args : mixed
                parameters sent to parent Thread class
            **kwargs : mixed
                parameters sent to parent Thread class
        """

        # threading.Thread.__init__(self, *args, **kwargs) # For some reason super() doesn't work
        super().__init__(*args, **kwargs)  # Works!
        self._func = func
        self.interval_sec = interval_sec
        self.init_sec = init_sec
        self._running = True
        self._functime = None  # Time the next call should be made

        self.start()

    def __repr__(self):
        return 'RecurringTask({}, {}, {})'.format(self._func, self.interval_sec, self.init_sec)

    def run(self):
        """Start the recurring task."""
        if self.init_sec:
            sleep(self.init_sec)
        self._functime = time()
        while self._running:
            start = time()
            self._func()
            self._functime += self.interval_sec
            if self._functime - start > 0:
                sleep(self._functime - start)

    def stop(self):
        """Stop the recurring task."""
        self._running = False
