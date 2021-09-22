from abc import ABCMeta, abstractmethod


class DumbBot:
    _instance = None

    __metaclass__ = ABCMeta

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DumbBot, cls).__new__(cls)
        return cls._instance

    @abstractmethod
    def start(self, *args, **kwargs):
        pass

    @abstractmethod
    def stop(self, *args, **kwargs):
        pass

    @abstractmethod
    def send_message(self, *args, **kwargs):
        pass

    @abstractmethod
    def send_order(self, order, signal, callback, commands, **kwargs):
        callback(**kwargs)

    @abstractmethod
    def add_connected_listener(self, *args, **kwargs):
        pass

    @abstractmethod
    def add_command_handler(self, *args, **kwargs):
        pass
