from operator import itemgetter
from uuid import uuid4

import pandas as pd
from fintick.utils import get_delta

from ..lib import get_top_n
from .constants import (
    DAILY,
    ERA_LENGTHS,
    MONTHLY,
    NOTIONAL,
    QUARTERLY,
    THRESH_ATTRS,
    WEEKLY,
    YEARLY,
)


def parse_thresh_attr(thresh_attr):
    thresh_attrs = ", ".join(THRESH_ATTRS)
    assert thresh_attr in THRESH_ATTRS, f"thresh_attr should be one of {thresh_attrs}"
    return thresh_attr


def parse_era_length(era_length):
    era_lengths = ", ".join(ERA_LENGTHS)
    assert era_length in ERA_LENGTHS, f"era_length should be one of {era_lengths}"
    return era_lengths


def get_initial_threshold_cache(thresh_attr):
    return {thresh_attr: 0}


def get_cache_for_era_length(date, era_length, thresh_attr, cache):
    next_date = get_delta(date, days=1)
    initial_cache = get_initial_threshold_cache(thresh_attr)
    # Reset cache for new era
    if era_length == DAILY:
        return initial_cache
    elif era_length == WEEKLY:
        if next_date.weekday() == 0:
            return initial_cache
    elif era_length == MONTHLY:
        if date.month != next_date.month:
            return initial_cache
    elif era_length == QUARTERLY:
        if pd.Timestamp(date).quarter != pd.Timestamp(next_date).quarter:
            return initial_cache
    elif era_length == YEARLY:
        if date.year != next_date.year:
            return initial_cache
    return cache


def get_next_cache(data_frame, cache, top_n=10):
    next_day = aggregate_rows(data_frame, top_n=top_n)
    if "nextDay" in cache:
        previous_day = cache.pop("nextDay")
        cache["nextDay"] = merge_cache(previous_day, next_day, top_n=top_n)
    else:
        cache["nextDay"] = next_day
    return cache


def aggregate_threshold(data_frame, cache, thresh_attr, thresh_value, top_n=10):
    start = 0
    samples = []
    for index, row in data_frame.iterrows():
        cache[thresh_attr] += row[thresh_attr]
        if cache[thresh_attr] >= thresh_value:
            df = data_frame.loc[start:index]
            sample = aggregate_rows(df, top_n=top_n)
            if "nextDay" in cache:
                previous = cache.pop("nextDay")
                sample = merge_cache(previous, sample, top_n=top_n)
            sample["uuid"] = str(uuid4())
            samples.append(sample)
            # Reinitialize cache
            cache[thresh_attr] = 0
            # Next index
            start = index + 1
    # Cache
    is_last_row = start == len(data_frame)
    if not is_last_row:
        df = data_frame.loc[start:]
        cache = get_next_cache(df, cache, top_n=top_n)
    return samples, cache


def aggregate_rows(df, top_n=10, freq_n=10):
    first_row = df.iloc[0]
    last_row = df.iloc[-1]
    buy_side = df[df.tickRule == 1]
    data = {
        "date": last_row.timestamp.date(),
        "timestamp": last_row.timestamp,
        "nanoseconds": last_row.nanoseconds,
        "open": first_row.price,
        "high": df.price.max(),
        "low": df.price.min(),
        "close": last_row.price,
        "slippage": df.slippage.sum(),
        "buySlippage": buy_side.slippage.sum(),
        "volume": df.volume.sum(),
        "buyVolume": buy_side.volume.sum(),
        "notional": df.notional.sum(),
        "buyNotional": buy_side.notional.sum(),
        "ticks": len(df),
        "buyTicks": len(buy_side),
        "topN": get_top_n(df, top_n=top_n),
    }
    return data


def merge_cache(previous, current, top_n=10):
    # Price
    current["open"] = previous["open"]
    current["high"] = max(previous["high"], current["high"])
    current["low"] = min(previous["low"], current["low"])
    # Stats
    for key in (
        "slippage",
        "buySlippage",
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
