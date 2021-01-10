import datetime

import pandas as pd
from cryptotick.aggregators.lib import calc_exponent
from cryptotick.aggregators.renko.lib import aggregate_renko, get_initial_cache

from .utils import get_trade


def get_data_frame(all_trades, date=None):
    date = date or datetime.datetime.now().date()
    trades = []
    for trade in all_trades:
        t = get_trade(**trade)
        t["date"] = trade.get("date", date)
        t["slippage"] = trade.get("slippage", 0)
        t["exponent"] = calc_exponent(t["volume"])
        trades.append(t)
    return pd.DataFrame(trades)


def aggregate(data_frame, cache=None, box_size=1, top_n=10):
    if not cache:
        data_frame, cache = get_initial_cache(data_frame, box_size)
    return aggregate_renko(data_frame, cache, box_size, top_n=top_n)


def assert_levels(trades, expected):
    data_frame = get_data_frame(trades)
    data, cache = aggregate(data_frame)
    # First trade decides level, so is discarded
    assert len(data) == len(expected) - 1
    for index, row in enumerate(data):
        assert row["level"] == expected[index + 1]
        assert row["change"] == expected[index + 1] - expected[index]


def assert_merge_cache(trades, top_n=10):
    data_frame = get_data_frame(trades)
    aggregated_without_cache_data, _ = aggregate(data_frame, top_n=top_n)
    prior_data_frame = data_frame.loc[:2]
    cache_data_frame = data_frame.loc[3:]
    prior_data_frame.reset_index(drop=True, inplace=True)
    cache_data_frame.reset_index(drop=True, inplace=True)
    prior_data, cache = aggregate(prior_data_frame)
    next_day = pd.DataFrame([cache["nextDay"]])
    aggregated_with_cache_data, _ = aggregate(
        cache_data_frame, cache=cache, top_n=top_n
    )
    aggregated_with_cache_df = pd.DataFrame(prior_data + aggregated_with_cache_data)
    aggregated_without_cache_df = pd.DataFrame(aggregated_without_cache_data)
    # Assert dataframe, excluding topN
    columns = aggregated_with_cache_df.columns.difference(["topN"])
    assert aggregated_with_cache_df[columns].equals(
        aggregated_without_cache_df[columns]
    )
    # Assert topN
    aggregated_with_cache_topN = pd.DataFrame(
        [trade for topN in aggregated_with_cache_df.topN for trade in topN]
    )
    aggregated_without_cache_topN = pd.DataFrame(
        [trade for topN in aggregated_without_cache_df.topN for trade in topN]
    )
    assert aggregated_with_cache_topN.equals(aggregated_without_cache_topN)
    assert "nextDay" not in cache
    return aggregated_with_cache_data, next_day


def test_up_exact():
    trades = [{"price": 1}, {"price": 2}, {"price": 3}]
    expected = [1, 2, 3]
    assert_levels(trades, expected)


def test_down_exact():
    trades = [{"price": 1}, {"price": 2}, {"price": 3}, {"price": 2}]
    expected = [1, 2, 3]
    assert_levels(trades, expected)


def test_down_inexact():
    trades = [{"price": 1}, {"price": 2}, {"price": 3}, {"price": 1.99}]
    expected = [1, 2, 3, 2]
    assert_levels(trades, expected)


def test_down_inexact_then_exact():
    trades = [{"price": 1}, {"price": 2}, {"price": 3}, {"price": 1.99}, {"price": 2}]
    expected = [1, 2, 3, 2]
    assert_levels(trades, expected)


def test_up_more_than_1():
    trades = [{"price": 1}, {"price": 3}]
    expected = [1, 3]
    assert_levels(trades, expected)


def test_down_more_than_1():
    trades = [{"price": 3}, {"price": 1}]
    expected = [3, 1]
    assert_levels(trades, expected)


def test_merge_cache():
    trades = [
        {"price": 1},
        {"price": 2},
        {"price": 2},
        {"price": 3},
        {"price": 4},
    ]
    assert_merge_cache(trades)


def test_topN():
    trades = [
        {"price": 1},
        {"price": 1, "notional": 1},
        {"price": 2, "notional": 2},
    ]
    data_frame = get_data_frame(trades)
    data, _ = aggregate(data_frame)
    assert sum([t["notional"] for t in data[0]["topN"]]) == 3


def test_top1_with_cache():
    trades = [
        {"price": 1},
        {"price": 2},
        {"price": 2, "notional": 2},
        {"price": 3, "notional": 1},
        {"price": 4},
    ]
    data, _ = assert_merge_cache(trades, top_n=1)
    top = data[0]["topN"]
    assert len(top) == 1
    assert top[0]["notional"] == 2


def test_top2_with_cache():
    trades = [
        {"price": 1},
        {"price": 2},
        {"price": 2, "notional": 3},
        {"price": 2, "notional": 2},
        {"price": 3, "notional": 1},
        {"price": 4},
    ]
    data, _ = assert_merge_cache(trades, top_n=2)
    top = data[0]["topN"]
    assert len(top) == 2
    assert top[0]["timestamp"] < top[1]["timestamp"]
    assert top[0]["notional"] == 3
    assert top[1]["notional"] == 2
