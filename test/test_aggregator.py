import datetime
import random

import pandas as pd
from cryptotick.aggregators.lib import aggregate_trades


def get_trade(
    timestamp=None,
    nanoseconds=None,
    price=None,
    notional=None,
    tick_rule=None,
    symbol=None,
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
        data.update({"symbol": symbol})
    return data


def get_trades(ticks, is_equal_timestamp=False, nanoseconds=None, symbol=None):
    """Generate random trades"""
    trade = get_trade(symbol=symbol, nanoseconds=nanoseconds, tick_rule=ticks[0])
    trades = [trade]
    for tick in ticks[1:]:
        t = get_trade(
            timestamp=trade["timestamp"] if is_equal_timestamp else None,
            nanoseconds=nanoseconds or 0,
            price=trade["price"] + (round(random.random() * 0.1, 2)) * tick,
            tick_rule=tick,
            symbol=symbol,
        )
        trades.append(t)
    return trades


def get_data_frame(all_trades):
    symbols = [trade["symbol"] for trade in all_trades if "symbol" in trade]
    if len(symbols):
        # All or None
        assert len(all_trades) == len(symbols)
        first_symbol = symbols[0]
        has_multiple_symbols = all([first_symbol == symbol for symbol in symbols])
    else:
        has_multiple_symbols = False
    trades = []
    for trade in all_trades:
        ticks = trade.pop("ticks")
        trades += get_trades(ticks, **trade)
    data_frame = pd.DataFrame(trades)
    return data_frame, has_multiple_symbols


def get_samples(trades):
    data_frame, has_multiple_symbols = get_data_frame(trades)
    return aggregate_trades(data_frame, has_multiple_symbols=has_multiple_symbols)


def test_equal_symbols_and_timestamps_and_ticks():
    trades = [
        {
            "symbol": "A",
            "is_equal_timestamp": True,
            "ticks": [1, 1],
        }
    ]
    samples = get_samples(trades)
    assert len(samples) == 1


def test_equal_symbols_and_timestamps_and_not_equal_ticks():
    trades = [
        {
            "symbol": "A",
            "is_equal_timestamp": True,
            "ticks": [1, -1],
        }
    ]
    samples = get_samples(trades)
    assert len(samples) == 2


def test_not_equal_symbols_and_equal_timestamps_and_ticks():
    trades = [
        {"symbol": "A", "is_equal_timestamp": True, "ticks": [1, 1]},
        {"symbol": "B", "is_equal_timestamp": True, "ticks": [1, 1]},
    ]
    samples = get_samples(trades)
    assert len(samples) == 2


def test_equal_ticks_and_equal_timestamp():
    trades = [{"ticks": [1, 1], "is_equal_timestamp": True}]
    samples = get_samples(trades)
    assert len(samples) == 1


def test_equal_ticks_and_not_equal_timestamp():
    trades = [{"ticks": [1, 1], "is_equal_timestamp": False}]
    samples = get_samples(trades)
    assert len(samples) == 2


def test_equal_ticks_and_equal_nanoseconds():
    trades = [
        {
            "ticks": [1, 1],
            "is_equal_timestamp": True,
            "nanoseconds": random.random() * 100,
        }
    ]
    samples = get_samples(trades)
    assert len(samples) == 1


def test_equal_ticks_and_not_equal_nanoseconds():
    trades = []
    nanoseconds = random.random() * 100
    for index, tick in enumerate([1, 1]):
        nanoseconds += index
        trade = {
            "ticks": [tick],
            "is_equal_timestamp": True,
            "nanoseconds": nanoseconds,
        }
        trades.append(trade)
    samples = get_samples(trades)
    assert len(samples) == 2


def test_slippage():
    now = datetime.datetime.now()
    trades = [
        get_trade(timestamp=now, price=1, notional=10, tick_rule=1),
        get_trade(timestamp=now, price=2, notional=10, tick_rule=1),
    ]
    data_frame = pd.DataFrame(trades)
    df = aggregate_trades(data_frame)
    assert df.loc[0].slippage == 10


def test_exponent():
    trades = [get_trade(price=i, notional=pow(10, i)) for i in range(0, 10)]
    data_frame = pd.DataFrame(trades[1:])
    df = aggregate_trades(data_frame)
    for row in df.itertuples():
        index = row.Index
        assert row.exponent == index + 1
