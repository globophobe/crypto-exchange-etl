import datetime
import random

import pandas as pd


def get_trade(
    symbol=None,
    timestamp=None,
    nanoseconds=0,
    price=None,
    notional=None,
    tick_rule=None,
    total_ticks=1,
):
    timestamp = timestamp or datetime.datetime.now()
    price = price or round(random.random() * 10, 2)
    notional = notional or random.random() * 10
    volume = price * notional
    tick_rule = tick_rule or random.choice((1, -1))
    data = {
        "timestamp": timestamp,
        "nanoseconds": nanoseconds,
        "price": price,
        "volume": volume,
        "notional": notional,
        "tickRule": tick_rule,
        "ticks": total_ticks,
    }
    if symbol:
        data["symbol"] = symbol
    return data


def get_price(price, tick_rule, jitter=0.0):
    change = random.random() * tick_rule * jitter
    return price + round(change, 2)  # Change in dollars


def get_trades(
    ticks,
    symbol=None,
    prices=[],
    is_equal_timestamp=False,
    nanoseconds=0,
    notional=None,
    total_ticks=1,
):
    """Generate random trades"""
    trades = []
    for index, tick in enumerate(ticks):
        # Price
        if index == 0:
            if len(prices):
                price = get_price(prices[index], tick)
            else:
                price = None
        else:
            price = get_price(trades[-1]["price"], tick, jitter=0.1)
        # Timestamp
        if len(trades) and is_equal_timestamp:
            timestamp = trades[0]["timestamp"]
        else:
            timestamp = None
        trades.append(
            get_trade(
                symbol=symbol,
                timestamp=timestamp,
                nanoseconds=nanoseconds,
                price=price,
                notional=notional,
                tick_rule=tick,
                total_ticks=total_ticks,
            )
        )
    return trades


def get_data_frame(data):
    trades = []
    for item in data:
        prices = item.pop("prices", [])
        ticks = item.pop("ticks")
        trades += get_trades(ticks, prices=prices, **item)
    return pd.DataFrame(trades)
