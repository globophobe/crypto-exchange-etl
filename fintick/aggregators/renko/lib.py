import numpy as np

from ..lib import aggregate_rows, get_next_cache, merge_cache


def get_level(price, box_size):
    return int(price / box_size) * box_size


def get_log_level(price, box_size):
    log_price = np.log1p(price)
    return int(log_price / box_size) * box_size


def get_initial_cache(data_frame, box_size, level_func=get_level):
    row = data_frame.loc[0]
    # First trade decides level, so is discarded
    df = data_frame.loc[1:]
    df.reset_index(drop=True, inplace=True)
    cache = {
        "timestamp": row.timestamp,
        "level": level_func(row.price, box_size),
        "direction": None,
    }
    return df, cache


def update_cache(data_frame, cache, sample):
    # Cache
    cache["timestamp"] = sample["timestamp"]
    cache["level"] = sample["level"]
    cache["direction"] = np.sign(sample["change"])
    return cache


def get_bounds(cache, box_size, reversal=1):
    level = cache["level"]
    direction = cache["direction"]
    if direction == 1:
        high = level + box_size
        low = level - (box_size * reversal)
    elif direction == -1:
        high = level + (box_size * reversal)
        low = level - box_size
    else:
        high = level + box_size
        low = level - box_size
    return high, low


def get_change(cache, high, low, price, box_size, level_func=get_level):
    level = cache["level"]
    p = price if level_func == get_level else np.log1p(price)
    higher = p >= high
    lower = p < low
    if higher or lower:
        current_level = level_func(price, box_size)
        change = current_level - level
        # Did price break below threshold?
        if lower:
            # Is there a remainder?
            if p % box_size != 0:
                change += box_size
                current_level += box_size
        return current_level, change
    return level, 0


def aggregate_renko(data_frame, cache, box_size, top_n=10, level_func=get_level):
    start = 0
    samples = []
    high, low = get_bounds(cache, box_size)
    for index, row in data_frame.iterrows():
        level, change = get_change(
            cache, high, low, row.price, box_size, level_func=level_func
        )
        if change:
            sample = aggregate_rows(
                data_frame,
                level,
                start,
                stop=index,
                top_n=top_n,
                extra={"level": level},
            )
            if "nextDay" in cache:
                # Next day is today's previous
                previous_day = cache.pop("nextDay")
                sample = merge_cache(previous_day, sample, top_n=top_n)
            # Positive or negative change
            sample["change"] = change
            # Is new level higher or lower than previous?
            assert_higher_or_lower(level, cache)
            # Is price bounded by the next higher or lower level?
            assert_bounds(row, cache, box_size)
            # Next index
            start = index + 1
            # Update cache
            cache = update_cache(data_frame, cache, sample)
            high, low = get_bounds(cache, box_size)
            samples.append(sample)
    # Cache
    is_last_row = start == len(data_frame)
    if not is_last_row:
        cache = get_next_cache(data_frame, cache, start, top_n=top_n)
    return samples, cache


def assert_higher_or_lower(level, cache):
    assert level < cache["level"] or level > cache["level"]


def assert_bounds(row, cache, box_size):
    high, low = get_bounds(cache, box_size)
    assert low <= cache["level"] <= high
    # Maybe float
    assert np.isclose(high, low + (box_size * 2))
