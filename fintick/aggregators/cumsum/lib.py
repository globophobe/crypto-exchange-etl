from uuid import uuid4

import pandas as pd

from ..lib import aggregate_rows, get_next_cache, merge_cache
from .constants import (
    CUMSUM_ATTRS,
    DAILY,
    ERA_LENGTHS,
    MONTHLY,
    QUARTERLY,
    WEEKLY,
    YEARLY,
)


def parse_cumsum_attr(cumsum_attr):
    cumsum_attrs = ", ".join(CUMSUM_ATTRS)
    assert cumsum_attr in CUMSUM_ATTRS, f"cumsum_attr should be one of {cumsum_attrs}"
    return cumsum_attr


def parse_era_length(era_length):
    era_lengths = ", ".join(ERA_LENGTHS)
    assert era_length in ERA_LENGTHS, f"era_length should be one of {era_lengths}"
    return era_lengths


def get_initial_cumsum_cache(cumsum_attr, timestamp):
    return {"era": timestamp, cumsum_attr: 0}


def get_cache_for_era_length(cache, timestamp, era_length, cumsum_attr):
    date = cache["era"].date()
    next_date = timestamp.date()
    initial_cache = get_initial_cumsum_cache(cumsum_attr, timestamp)
    # Reset cache for new era
    if era_length == DAILY:
        if date != next_date:
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


def merge_cumsum_cache(previous, current, top_n=0):
    current["open"] = previous["open"]
    current["high"] = max(previous["high"], current["high"])
    current["low"] = min(previous["low"], current["low"])
    return merge_cache(previous, current, top_n=top_n)


def aggregate_cumsum(data_frame, cache, cumsum_attr, cumsum_value, top_n=0):
    start = 0
    samples = []
    for index, row in data_frame.iterrows():
        cache[cumsum_attr] += row[cumsum_attr]
        if cache[cumsum_attr] >= cumsum_value:
            df = data_frame.loc[start:index]
            sample = aggregate_rows(df, uid=str(uuid4()), top_n=top_n)
            if "nextDay" in cache:
                previous = cache.pop("nextDay")
                sample = merge_cache(previous, sample, top_n=top_n)
            samples.append(sample)
            # Reinitialize cache
            cache[cumsum_attr] = 0
            # Next index
            start = index + 1
    # Cache
    is_last_row = start == len(data_frame)
    if not is_last_row:
        df = data_frame.loc[start:]
        cache = get_next_cache(df, cache, top_n=top_n)
    return samples, cache
