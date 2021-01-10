import datetime
import random


def get_trade(
    symbol=None,
    timestamp=None,
    nanoseconds=None,
    price=None,
    slippage=None,
    notional=None,
    tick_rule=None,
):
    p = price or round(random.random() * 10, 2)
    n = notional or random.random() * 10
    volume = p * n
    data = {
        "timestamp": timestamp or datetime.datetime.now(),
        "nanoseconds": nanoseconds or 0,
        "price": p,
        "notional": n,
        "volume": volume,
        "tickRule": tick_rule or random.choice((1, -1)),
    }
    if symbol:
        data["symbol"] = symbol
    return data
