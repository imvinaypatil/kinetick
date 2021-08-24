import os

__TOKEN__ = os.getenv("BOT_TOKEN")

if __TOKEN__:
    from .telegram_bot import TelegramBot as Bot
else:
    from .dummy import DumbBot as Bot
