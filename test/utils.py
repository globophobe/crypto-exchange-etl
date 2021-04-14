import datetime
import random

import pandas as pd


def get_trade(
    symbol=None,
    timestamp=None,
    nanoseconds=None,
    price=None,
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
        "volume": volume,
        "notional": n,
        "tickRule": tick_rule or random.choice((1, -1)),
    }
    if symbol:
        data["symbol"] = symbol
    return data


def get_trades(
    ticks,
    symbol=None,
    prices=[],
    is_equal_timestamp=False,
    nanoseconds=None,
    notional=None,
):
    """Generate random trades"""
    trade = get_trade(
        symbol=symbol,
        price=prices[0] if len(prices) else None,
        nanoseconds=nanoseconds,
        notional=notional,
        tick_rule=ticks[0],
    )
    trades = [trade]
    for index, tick in enumerate(ticks[1:]):
        if len(prices):
            p = prices[index + 1]
        else:
            p = trade["price"] + (round(random.random() * 0.1, 2)) * tick
        t = get_trade(
            symbol=symbol,
            timestamp=trade["timestamp"] if is_equal_timestamp else None,
            nanoseconds=nanoseconds or 0,
            price=p,
            notional=notional,
            tick_rule=tick,
        )
        trades.append(t)
    return trades


def get_data_frame(all_trades):
    trades = []
    for trade in all_trades:
        prices = trade.pop("prices", [])
        ticks = trade.pop("ticks")
        trades += get_trades(ticks, prices=prices, **trade)
    return pd.DataFrame(trades)
