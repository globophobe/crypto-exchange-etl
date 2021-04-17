from operator import itemgetter

import pandas as pd

from .constants import NOTIONAL


def get_timestamp_from_to(partition):
    timestamp_from = partition.replace(minute=0, second=0, microsecond=0)
    timestamp_to = timestamp_from + pd.Timedelta("1h")
    return timestamp_from, timestamp_to


def aggregate_rows(
    df,
    uid=None,
    timestamp=None,
    nanoseconds=None,
    open_price=None,
    top_n=0,
):
    first_row = df.iloc[0]
    last_row = df.iloc[-1]
    buy_side = df[df.tickRule == 1]
    high = df.price.max()
    low = df.price.min()
    if open_price:
        if open_price > high:
            high = open_price
        if open_price < low:
            low = open_price
    else:
        open_price = first_row.price
    data = {
        "timestamp": timestamp if timestamp else first_row.timestamp,
        "nanoseconds": nanoseconds if nanoseconds else first_row.nanoseconds,
        "open": open_price,
        "high": high,
        "low": low,
        "close": last_row.price,
        "volume": df.volume.sum(),
        "buyVolume": buy_side.volume.sum(),
        "notional": df.notional.sum(),
        "buyNotional": buy_side.notional.sum(),
        "ticks": df.ticks.sum(),
        "buyTicks": buy_side.ticks.sum(),
    }
    if uid:
        data["uid"] = uid  # For join
    if "symbol" in df.columns:
        assert len(df.symbol.unique()) == 1
        data["symbol"] = first_row.symbol
    if top_n:
        data["topN"] = get_top_n(df, top_n=top_n)
    return data


def get_top_n(data_frame, column=NOTIONAL, top_n=0):
    index = data_frame[column].astype(float).nlargest(top_n).index
    df = data_frame[data_frame.index.isin(index)]
    data = df.to_dict("records")
    data.sort(key=itemgetter("timestamp", "nanoseconds", "index"))
    for item in data:
        for key in list(item):

            if key not in (
                "timestamp",
                "nanoseconds",
                "price",
                "vwap",
                "volume",
                "notional",
                "ticks",
                "tickRule",
            ):
                del item[key]
    return data


def get_next_cache(data_frame, cache, top_n=10):
    next_day = aggregate_rows(data_frame, top_n=top_n)
    if "nextDay" in cache:
        previous_day = cache.pop("nextDay")
        cache["nextDay"] = merge_cache(previous_day, next_day, top_n=top_n)
    else:
        cache["nextDay"] = next_day
    return cache


def merge_cache(previous, current, top_n=10):
    # Price
    current["open"] = previous["open"]
    current["high"] = max(previous["high"], current["high"])
    current["low"] = min(previous["low"], current["low"])
    # Stats
    for key in (
        "volume",
        "buyVolume",
        "notional",
        "buyNotional",
        "ticks",
        "buyTicks",
    ):
        current[key] += previous[key]
    # Top N
    merged_top = previous["topN"] + current["topN"]
    if len(merged_top):
        # Sort by volume
        sorted(merged_top, key=lambda x: x[NOTIONAL], reverse=True)
        # Slice top_n
        m = merged_top[:top_n]
        # Sort by timestamp, nanoseconds
        sorted(m, key=itemgetter("timestamp", "nanoseconds", "index"))
        current["topN"] = m
    return current
