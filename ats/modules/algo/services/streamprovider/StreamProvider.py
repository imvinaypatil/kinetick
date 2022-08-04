import abc
from builtins import function

import pandas


class StreamProvider(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def setTickHandler(self, handler: function[pandas.DataFrame]) -> None:
        pass

    @abc.abstractmethod
    def setBarHandler(self, handler: function[pandas.DataFrame]) -> None:
        pass

    @abc.abstractmethod
    def setQuoteHandler(self, handler: function[pandas.DataFrame]) -> None:
        pass

    @abc.abstractmethod
    def setOrderbookHandler(self, handler: function[pandas.DataFrame]) -> None:
        pass

