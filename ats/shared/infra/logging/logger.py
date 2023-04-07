import logging
from typing import Any

from ats.shared.config.Appconfig import Appconfig


class Logger:

    def __init__(self):
        self.level = Appconfig.LOGLEVEL or logging.NOTSET
        self.logger = logging.Logger(name='ATS', level=self.level)

    def log(self, message: str, data: Any = None):
        self.logger.log(self.level, message, data)

    def info(self, message: str, data: Any = None):
        self.logger.info(message, data)

    def warn(self, message: str, data: Any = None):
        self.logger.warning(message, data)

    def debug(self, message: str, data: Any = None):
        self.logger.debug(message, data)

    def error(self, message: str, data: Any = None):
        self.logger.debug(message, data)


logger = Logger()
