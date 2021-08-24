import logging
import os
from abc import ABCMeta

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler

from kinetick.bot.dummy import DumbBot
from kinetick.utils.utils import rand_pass

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv('LOGLEVEL') or logging.INFO)

__TOKEN__ = os.getenv("BOT_TOKEN")


class TelegramBot(DumbBot):
    _instance = None

    __metaclass__ = ABCMeta

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(TelegramBot, cls).__new__(cls)
            TelegramBot.init(cls._instance)
        return cls._instance

    @staticmethod
    def init(self):
        self._chat_ids = set()
        self._verified_chat_id = None  # only verified user can place orders.
        self._password = rand_pass(6)
        self._on_connected_listeners = set()
        self._callbacks_store = {}
        if __TOKEN__:
            self.bot = Updater(__TOKEN__, use_context=True)
            self.bot.dispatcher.add_handler(CommandHandler("start", self._start_cmd_handler))
            self.bot.dispatcher.add_handler(CommandHandler("stop", self._stop_cmd_handler))
            self.bot.dispatcher.add_handler(CommandHandler("login", self._login_cmd_handler))
            self.bot.dispatcher.add_handler(CallbackQueryHandler(self._button))
            # dispatcher.add_handler(CommandHandler("help", help))

            logger.info("starting bot..")
            try:
                self.bot.start_polling()
                logger.debug("telegram bot started polling ...")
                logger.info("Use Login Pass to login into bot.")
                print(f"Login Pass: {self._password}")
            except Exception as e:
                logger.error("Failed to launch bot", e)

    def start(self, *args, **kwargs):
        pass

    def stop(self, *args, **kwargs):
        if self.bot:
            self.bot.stop()

    def send_message(self, msg):
        for user in self._chat_ids and self.bot:
            self.bot.bot.send_message(text=msg, chat_id=user, parse_mode='Markdown')

    def send_order(self, order, caller, callback=None):
        keyboard = [[InlineKeyboardButton("Limit", callback_data=caller + "$limit"),
                     InlineKeyboardButton("Cancel", callback_data=caller + "$cancel"),
                     InlineKeyboardButton("Market", callback_data=caller + "$market")]]

        reply_markup = InlineKeyboardMarkup(keyboard)

        tradingview_symbol = order.symbol
        tradingview_symbol = 'NIFTY' if tradingview_symbol == 'NSEI' else tradingview_symbol
        tradingview_symbol = 'BANKNIFTY' if tradingview_symbol == 'NSEBANK' else tradingview_symbol

        message = f'Symbol: #[{order.symbol}](https://tradingview.com/chart/?symbol={tradingview_symbol}) \n ' \
                  f'Signal: #{caller} \n Direction: *#{order.direction}* \n ' \
                  f'Strategy #{order.algo} \n' \
                  f'Entry Price:  {order.entry_price:.2f}  \n Quantity:  {order.quantity} \n ' \
                  f'Stoploss: {order.stop:.2f} \n Target: {order.target:.2f} \n ' \
                  f'Exit Price: {order.exit_price:.2f} \n'
        for user in self._chat_ids:
            self.bot.bot.send_message(text=message, chat_id=user, parse_mode='Markdown')
            if user == self._verified_chat_id:
                self.bot.bot.send_message(text=f'{order.symbol} call confirm?', reply_markup=reply_markup, chat_id=user)
        if callback is not None:
            if callable(callback):
                self._callbacks_store[caller] = callback

    def add_connected_listener(self, listener):
        self._on_connected_listeners.add(listener)

    def add_command_handler(self, command, handler, help_string=""):
        self.bot.dispatcher.add_handler(CommandHandler(command, handler))

    def _start_cmd_handler(self, update, context):
        update.message.reply_text('Hi {}, Use `/login <password>` command to start trading.'
                                  .format(update.message.from_user.first_name), parse_mode='Markdown')
        for listener in self._on_connected_listeners:
            listener(update.message.chat_id)

    def _login_cmd_handler(self, update, ctx):
        text = update.message.text.split()[-1]
        if text == self._password:
            self._chat_ids.add(update.message.chat_id)
            self._verified_chat_id = update.message.chat_id
            msg = "Successfully logged in. Happy trading!"
            self.bot.bot.send_message(text=msg, chat_id=self._verified_chat_id)
        else:
            msg = "Invalid password!"
            self.bot.bot.send_message(text=msg, chat_id=update.message.chat_id)

    def _stop_cmd_handler(self, update, ctx):
        chat_id = update.message.chat_id
        if chat_id in self._chat_ids:
            self._chat_ids.remove(chat_id)

    def _button(self, update, context):
        query = update.callback_query
        query.answer()
        if self._verified_chat_id == update.effective_chat.id:
            data, *cmd = query.data.split("$")
            cmd = cmd[-1] if len(cmd) > 0 else data
            callback = self._callbacks_store[data] if data in self._callbacks_store else None

            if callback is not None:
                del self._callbacks_store[data]
                try:
                    market = True if cmd == "market" else False
                    cancel = True if cmd == "cancel" else False
                    callback(market=market, cancel=cancel)
                except Exception as e:
                    logger.error('Error executing command', e)
            query.edit_message_text(text="Selected option: {}".format(cmd))
        else:
            query.edit_message_text(text="Can not execute order.")
