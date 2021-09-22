# Copyright 2021 vin8tech, vinay patil
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
__CHAT_ID__ = os.getenv("CHAT_ID")


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
        self._order_number = 0
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
        if __CHAT_ID__ is not None:
            self._chat_ids.add(int(__CHAT_ID__))
            self._verified_chat_id = int(__CHAT_ID__)

    def start(self, *args, **kwargs):
        pass

    def stop(self, *args, **kwargs):
        if self.bot:
            self.bot.stop()

    def send_message(self, msg):
        for user in self._chat_ids:
            self.bot.bot.send_message(text=msg, chat_id=user, parse_mode='Markdown')

    def send_order(self, position, signal, callback=None, commands: tuple = (), **kwargs):
        self._order_number += 1
        key = signal + str(self._order_number)
        keyboard = [list(map(lambda cmd: InlineKeyboardButton(str(cmd), callback_data=key + f"${cmd}"), commands))]
        # cmd used in _button handler to pass the selected option to callback.

        reply_markup = InlineKeyboardMarkup(keyboard)

        tradingview_symbol = position.symbol
        tradingview_symbol = 'NIFTY' if tradingview_symbol == 'NSEI' else tradingview_symbol
        tradingview_symbol = 'BANKNIFTY' if tradingview_symbol == 'NSEBANK' else tradingview_symbol

        message = f'Symbol: [{position.symbol}](https://tradingview.com/chart/?symbol={tradingview_symbol}) \n ' \
                  f'Signal: {signal} \n Direction: *#{position.direction}* \n ' \
                  f'Strategy #{position.algo} \n' \
                  f'Entry Price:  {position.entry_price:.2f}  \n Quantity:  {position.quantity} \n ' \
                  f'Stoploss: {position.stop:.2f} \n Target: {position.target:.2f} \n ' \
                  f'Exit Price: {position.exit_price:.2f} \n'
        for user in self._chat_ids:
            self.bot.bot.send_message(text=message, chat_id=user, parse_mode='Markdown')
            if user == self._verified_chat_id:
                self.bot.bot.send_message(text=f'{position.symbol} {position.direction} confirm?',
                                          reply_markup=reply_markup, chat_id=user)
        if callback is not None:
            if callable(callback):
                self._callbacks_store[key] = callback

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
                    callback(commands=(cmd, ))
                    query.edit_message_text(text="{} order request sent".format(cmd))
                except Exception as e:
                    logger.error('Error executing command %s', e)
                    query.edit_message_text(text="Error sending order request. Reason: {}".format(e))
        else:
            query.edit_message_text(text="Can not execute order.")
