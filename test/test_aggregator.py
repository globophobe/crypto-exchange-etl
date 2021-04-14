import datetime
import random

import pandas as pd
from fintick.aggregators.trades.lib import aggregate_trades

from .utils import get_data_frame, get_trade


def get_samples(trades):
    return aggregate_trades(get_data_frame(trades))


def test_equal_symbols_and_timestamps_and_ticks():
    trades = [{"symbol": "A", "is_equal_timestamp": True, "ticks": [1, 1]}]
    samples = get_samples(trades)
    assert len(samples) == 1


def test_equal_symbols_and_timestamps_and_not_equal_ticks():
    trades = [{"symbol": "A", "is_equal_timestamp": True, "ticks": [1, -1]}]
    samples = get_samples(trades)
    assert len(samples) == 2


def test_not_equal_symbols_and_equal_timestamps_and_ticks():
    trades = [
        {"symbol": "A", "is_equal_timestamp": True, "ticks": [1, 1]},
        {"symbol": "B", "is_equal_timestamp": True, "ticks": [1, 1]},
    ]
    samples = get_samples(trades)
    assert len(samples) == 2


def test_not_equal_symbols_and_timestamps_and_equal_ticks():
    trades = [
        {"symbol": "A", "is_equal_timestamp": True, "ticks": [1, 1]},
        {"symbol": "A", "is_equal_timestamp": False, "ticks": [-1]},
        {"symbol": "B", "is_equal_timestamp": True, "ticks": [1, 1]},
        {"symbol": "B", "is_equal_timestamp": False, "ticks": [-1]},
    ]
    samples = get_samples(trades)
    assert len(samples) == 4


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


def test_vwap():
    now = datetime.datetime.utcnow()
    trades = [
        get_trade(timestamp=now, price=1, notional=1, tick_rule=1),
        get_trade(timestamp=now, price=2, notional=1, tick_rule=1),
        get_trade(timestamp=now, price=3, notional=1, tick_rule=1),
    ]
    data_frame = pd.DataFrame(trades)
    df = aggregate_trades(data_frame)
    assert df.loc[0].vwap == 2
