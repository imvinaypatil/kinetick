import json
import time
from urllib.parse import urljoin

import dateutil.parser
import requests
import logging
import os

from kinetick.enums import SecurityType, PositionType

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=os.getenv('LOGLEVEL') or logging.INFO)

logger = logging.getLogger(__name__)


class Zerodha():
    # Constants
    # Products
    PRODUCT_MIS = "MIS"
    PRODUCT_CNC = "CNC"
    PRODUCT_NRML = "NRML"
    PRODUCT_CO = "CO"
    PRODUCT_BO = "BO"

    # Order types
    ORDER_TYPE_MARKET = "MARKET"
    ORDER_TYPE_LIMIT = "LIMIT"
    ORDER_TYPE_SLM = "SL-M"
    ORDER_TYPE_SL = "SL"

    # Varities
    VARIETY_REGULAR = "regular"
    VARIETY_BO = "bo"
    VARIETY_CO = "co"
    VARIETY_AMO = "amo"

    # Transaction type
    TRANSACTION_TYPE_BUY = "BUY"
    TRANSACTION_TYPE_SELL = "SELL"

    # Validity
    VALIDITY_DAY = "DAY"
    VALIDITY_IOC = "IOC"

    # Exchanges
    EXCHANGE_NSE = "NSE"
    EXCHANGE_BSE = "BSE"
    EXCHANGE_NFO = "NFO"
    EXCHANGE_CDS = "CDS"
    EXCHANGE_BFO = "BFO"
    EXCHANGE_MCX = "MCX"

    # Margins segments
    MARGIN_EQUITY = "equity"
    MARGIN_COMMODITY = "commodity"

    # Status constants
    STATUS_COMPLETE = "COMPLETE"
    STATUS_REJECTED = "REJECTED"
    STATUS_CANCELLED = "CANCELLED"

    # GTT order type
    GTT_TYPE_OCO = "two-leg"
    GTT_TYPE_SINGLE = "single"

    # GTT order status
    GTT_STATUS_ACTIVE = "active"
    GTT_STATUS_TRIGGERED = "triggered"
    GTT_STATUS_DISABLED = "disabled"
    GTT_STATUS_EXPIRED = "expired"
    GTT_STATUS_CANCELLED = "cancelled"
    GTT_STATUS_REJECTED = "rejected"
    GTT_STATUS_DELETED = "deleted"

    _routes = {
        "login": "/api/login",
        "twofa": "/api/twofa",
        "api.token": "/session/token",
        "api.token.invalidate": "/session/token",
        "api.token.renew": "/session/refresh_token",
        "user.profile": "/user/profile",
        "user.margins": "/user/margins",
        "user.margins.segment": "/user/margins/{segment}",

        "orders": "oms/orders",
        "trades": "oms/trades",

        "order.info": "oms/orders/{order_id}",
        "order.place": "oms/orders/{variety}",
        "order.modify": "oms/orders/{variety}/{order_id}",
        "order.cancel": "oms/orders/{variety}/{order_id}",
        "order.trades": "oms/orders/{order_id}/trades",

        "portfolio.positions": "oms/portfolio/positions",
        "portfolio.holdings": "oms/portfolio/holdings",
        "portfolio.positions.convert": "oms/portfolio/positions",

        # MF api endpoints
        "mf.orders": "/mf/orders",
        "mf.order.info": "/mf/orders/{order_id}",
        "mf.order.place": "/mf/orders",
        "mf.order.cancel": "/mf/orders/{order_id}",

        "mf.sips": "/mf/sips",
        "mf.sip.info": "/mf/sips/{sip_id}",
        "mf.sip.place": "/mf/sips",
        "mf.sip.modify": "/mf/sips/{sip_id}",
        "mf.sip.cancel": "/mf/sips/{sip_id}",

        "mf.holdings": "/mf/holdings",
        "mf.instruments": "/mf/instruments",

        "market.instruments.all": "/instruments",
        "market.instruments": "/instruments/{exchange}",
        "market.margins": "/margins/{segment}",
        "market.historical": "/instruments/historical/{instrument_token}/{interval}",
        "market.trigger_range": "/instruments/trigger_range/{transaction_type}",

        "market.quote": "/quote",
        "market.quote.ohlc": "/quote/ohlc",
        "market.quote.ltp": "/quote/ltp",

        # GTT endpoints
        "gtt": "/gtt/triggers",
        "gtt.place": "/gtt/triggers",
        "gtt.info": "/gtt/triggers/{trigger_id}",
        "gtt.modify": "/gtt/triggers/{trigger_id}",
        "gtt.delete": "/gtt/triggers/{trigger_id}"
    }

    def __init__(self, user_id, password, pin, debug=False):
        self.user_id = user_id,
        self.password = password,
        self.pin = pin
        self.apiKey = 'kitefront'
        self.base_url = "https://kite.zerodha.com"

        self._session = requests.Session()
        self._session.headers.update({'X-Kite-Version': '2.4.0'})
        self.debug = debug
        self.timeout = 60
        self.maxretry = 3

        self._session_expiry_hook = self.default_session_expiry_hook

        # ==== set default values. =====
        self._account = {}
        self.orders = {}  # TODO initialize with pending orders
        self.symbol_orders = {}
        # =====

    def login(self):
        res = self._post("login", {'user_id': self.user_id, 'password': self.password})
        time.sleep(1)
        res = self._session.post(self.base_url + self._routes["twofa"],
                                 {'user_id': res['user_id'], 'request_id': res['request_id'],
                                  'twofa_value': self.pin})
        data = res.json()
        if data['status'] == 'success':
            logger.info("logged into zerodha")
            self._session.headers.update({'Authorization': "enctoken " + res.cookies.get_dict()['enctoken']})
            return

    def default_session_expiry_hook(self, response, **kwargs):
        logger.info("Running session expiry hook")
        headers = kwargs['headers'] if 'headers' in kwargs else {}
        retryAttempts = headers["x-retry"] if "x-retry" in headers else 1
        if int(retryAttempts) <= self.maxretry:
            logger.info(f"Retrying request. Attempt: {retryAttempts}")
            self.login()
            headers["x-retry"] = str(int(retryAttempts) + 1)
            kwargs['headers'] = headers
            return self._request(**kwargs)
        logger.error("Maximum session retry attempts {} exceeded".format(self.maxretry))
        raise Exception(f"zerodha: maximum re-login attempts {self.maxretry} failed")

    def set_session_expiry_hook(self, method):
        """
        Set a callback hook for session (`TokenError` -- timeout, expiry etc.) errors.
        A callback method that handles session errors
        can be set here and when the client encounters
        a token error at any point, it'll be called.

        This callback, for instance, can log the user out of the UI,
        clear session cookies, or initiate a fresh login.
        """
        if not callable(method):
            raise TypeError("Invalid input type. Only functions are accepted.")

        self._session_expiry_hook = method

    def place_order(self, variety, tradingsymbol, transaction_type, quantity, product,
                    order_type, exchange='NSE', **kwargs):
        """
        :param variety:
        :param tradingsymbol:
        :param transaction_type:
        :param quantity:
        :param product:
        :param order_type:
        :param exchange:
        :param price:
        :param trigger_price:
        :param validity:
        :param disclosed_quantity:
        :param trigger_price:
        :param squareoff:
        :param stoploss:
        :param squareoff:
        :param trailing_stoploss:
        :param tag:
        :return:
        """
        params = {'variety': variety,
                  'tradingsymbol': tradingsymbol,
                  'transaction_type': transaction_type,
                  'quantity': quantity,
                  'product': product,
                  'order_type': order_type,
                  'exchange': exchange
                  }
        for param in kwargs:
            if param is not None:
                params[param] = kwargs[param]
        response = self._post("order.place", params)
        logger.info("Order Placed with parameters ", params)
        return response["order_id"]

    def modify_order(self,
                     variety,
                     order_id,
                     parent_order_id=None,
                     quantity=None,
                     price=None,
                     order_type=None,
                     trigger_price=None,
                     validity=None,
                     disclosed_quantity=None):
        """Modify an open order."""
        params = locals()
        del (params["self"])

        for k in list(params.keys()):
            if params[k] is None:
                del (params[k])

        return self._put("order.modify", params)["order_id"]

    def cancel_order(self, variety, order_id, parent_order_id=None):
        """Cancel an order."""
        return self._delete("order.cancel", {
            "order_id": order_id,
            "variety": variety,
            "parent_order_id": parent_order_id
        })["order_id"]

    def exit_order(self, order_id, variety=None, parent_order_id=None):
        """Exit order."""
        if variety is None:
            order = self.order_by_id(order_id)[-1]
            variety = order['variety']
            parent_order_id = order['parent_order_id']
        self.cancel_order(variety, order_id, parent_order_id=parent_order_id)

    def _format_response(self, data):
        """Parse and format responses."""

        if type(data) == list:
            _list = data
        elif type(data) == dict:
            _list = [data]

        for item in _list:
            # Convert date time string to datetime object
            for field in ["order_timestamp", "exchange_timestamp", "created", "last_instalment", "fill_timestamp",
                          "timestamp", "last_trade_time"]:
                if item.get(field) and len(item[field]) == 19:
                    item[field] = dateutil.parser.parse(item[field])

        return _list[0] if type(data) == dict else _list

    def orders(self):
        """Get list of orders."""
        return self._format_response(self._get("orders"))

    def order_by_id(self, order_id):
        """
        Get history of individual order.
        """
        return self._format_response(self._get("order.info", {"order_id": order_id}))

    def positions(self):
        """Retrieve the list of positions."""
        return self._get("portfolio.positions")

    def holdings(self):
        """Retrieve the list of equity holdings."""
        return self._get("portfolio.holdings")

    def _get(self, route, params=None):
        """Alias for sending a GET request."""
        return self._request(route, "GET", params)

    def _post(self, route, params=None):
        """Alias for sending a POST request."""
        return self._request(route, "POST", params)

    def _put(self, route, params=None):
        """Alias for sending a PUT request."""
        return self._request(route, "PUT", params)

    def _delete(self, route, params=None):
        """Alias for sending a DELETE request."""
        return self._request(route, "DELETE", params)

    def _request(self, route, method, parameters=None, headers=None):
        """Make an HTTP request."""
        if headers is None:
            headers = {}
        params = parameters.copy() if parameters else {}

        # Form a restful URL
        uri = self._routes[route].format(**params)
        url = urljoin(self.base_url, uri)

        if self.debug:
            logger.debug("Request: {method} {url} {params}".format(method=method, url=url, params=params))

        try:
            response = self._session.request(method,
                                             url,
                                             data=params if method in ["POST", "PUT"] else None,
                                             params=params if method in ["GET", "DELETE"] else None,
                                             headers=headers,
                                             # verify=not self.disable_ssl,
                                             allow_redirects=True,
                                             timeout=self.timeout)
        # Any requests lib related exceptions are raised here -
        # http://docs.python-requests.org/en/master/_modules/requests/exceptions/
        except Exception as e:
            raise e

        if self.debug:
            logger.debug("Response: {code} {content}".format(code=response.status_code, content=response.content))

        # Validate the content type.
        if "json" in response.headers["content-type"]:
            try:
                data = json.loads(response.content.decode("utf8"))
            except ValueError:
                raise Exception("Couldn't parse the JSON response received from the server: {content}".format(
                    content=response.content))

            # api error
            if data.get("error_type"):
                # Call session hook if its registered and TokenException is raised
                if self._session_expiry_hook and response.status_code == 403 and data["error_type"] == "TokenException":
                    return self._session_expiry_hook(
                        response, route=route, method=method, parameters=parameters, headers=headers)
                else:
                    # native Kite errors
                    # exp = getattr(ex, data["error_type"], ex.GeneralException)
                    raise Exception(data["message"])

            return data["data"]
        elif "csv" in response.headers["content-type"]:
            return response.content
        else:
            raise Exception("Unknown Content-Type ({content_type}) with response: ({content})".format(
                content_type=response.headers["content-type"],
                content=response.content))

    @property
    def account(self):
        return self._account

    def get_order_variety(self, sec_type, pos_type):
        if sec_type == SecurityType.OPTION:
            return Zerodha.VARIETY_REGULAR, Zerodha.PRODUCT_MIS
        if sec_type == SecurityType.STOCK and pos_type == PositionType.CO:
            return Zerodha.VARIETY_CO, Zerodha.PRODUCT_MIS
        if sec_type == SecurityType.STOCK and pos_type == PositionType.MIS:
            return Zerodha.VARIETY_REGULAR, Zerodha.PRODUCT_MIS
        return Zerodha.VARIETY_REGULAR, Zerodha.PRODUCT_CNC


if __name__ == "__main__":
    # kite.set_access_token()
    zerodha = Zerodha("", "", "", debug=True)
    zerodha.login()
    # print(zerodha.getOrders())
    order = zerodha.place_order(variety=Zerodha.VARIETY_CO, tradingsymbol='ACC',
                                transaction_type=Zerodha.TRANSACTION_TYPE_BUY, quantity=1,
                                product=Zerodha.PRODUCT_CO, order_type=Zerodha.ORDER_TYPE_LIMIT,
                                price=89.5)
    print(order)
