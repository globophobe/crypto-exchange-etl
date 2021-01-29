import re

from .constants import BCHUSD, ETHUSD, LTCUSD, XBTUSD, XRPUSD, uBTC

XBT_FUTURES_REGEX = re.compile(r"^XBT(\w)\d+$")


def calc_notional(x):
    if x.symbol == XBTUSD or XBT_FUTURES_REGEX.match(x.symbol):
        return x.volume / x.price
    elif x.symbol.startswith(ETHUSD) or x.symbol.startswith(BCHUSD):
        return x.volume * x.price * uBTC
    elif x.symbol.startswith(LTCUSD):
        return x.volume * x.price * uBTC * 2
    elif x.symbol == XRPUSD:
        return x.volume * x.price * uBTC / 20
    else:
        raise NotImplementedError
