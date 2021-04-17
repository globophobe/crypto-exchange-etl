from uuid import uuid4

import pandas as pd
from fintick.utils import get_delta

from ..lib import aggregate_rows, get_next_cache, merge_cache
from .constants import (
    DAILY,
    ERA_LENGTHS,
    MONTHLY,
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


def aggregate_threshold(data_frame, cache, thresh_attr, thresh_value, top_n=0):
    start = 0
    samples = []
    for index, row in data_frame.iterrows():
        cache[thresh_attr] += row[thresh_attr]
        if cache[thresh_attr] >= thresh_value:
            df = data_frame.loc[start:index]
            sample = aggregate_rows(df, uid=str(uuid4()), top_n=top_n)
            if "nextDay" in cache:
                previous = cache.pop("nextDay")
                sample = merge_cache(previous, sample, top_n=top_n)
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
