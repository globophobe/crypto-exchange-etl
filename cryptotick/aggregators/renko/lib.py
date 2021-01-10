from operator import itemgetter

import numpy as np


def get_value_display(value):
    if value % 1 == 0:
        return str(int(value))
    else:
        return str(value).replace(".", "d")


def get_initial_cache(data_frame, box_size):
    row = data_frame.loc[0]
    # First trade decides level, so is discarded
    df = data_frame.loc[1:]
    df.reset_index(drop=True, inplace=True)
    cache = {
        "timestamp": row.timestamp,
        "level": get_level(row.price, box_size),
        "direction": None,
    }
    return df, cache


def update_cache(data_frame, cache, sample):
    # Cache
    cache["timestamp"] = sample["timestamp"]
    cache["level"] = sample["level"]
    cache["direction"] = np.sign(sample["change"])
    return cache


def get_next_cache(data_frame, cache, start, top_n=10):
    next_day = aggregate_rows(data_frame, cache["level"], start, top_n=top_n)
    if "nextDay" in cache:
        previous_day = cache.pop("nextDay")
        cache["nextDay"] = merge_cache(previous_day, next_day, top_n=top_n)
    else:
        cache["nextDay"] = next_day
    return cache


def get_level(price, box_size):
    return int(price / box_size) * box_size  # Integer


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


def get_change(cache, high, low, price, box_size):
    higher = price >= high
    lower = price < low
    level = cache["level"]
    if higher or lower:
        current_level = get_level(price, box_size)
        change = current_level - level
        # Did price break below threshold?
        if price < low:
            # Is there a remainder?
            if price % box_size != 0:
                # If not, decrement change
                change += box_size
                current_level += box_size
        return current_level, change
    return level, 0


def aggregate_renko(data_frame, cache, box_size, top_n=10):
    start = 0
    samples = []
    high, low = get_bounds(cache, box_size)
    for index, row in data_frame.iterrows():
        level, change = get_change(cache, high, low, row.price, box_size)
        if change:
            sample = aggregate_rows(data_frame, level, start, stop=index, top_n=top_n)
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


def merge_cache(previous, current, top_n=10):
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
        sorted(merged_top, key=lambda x: x["volume"], reverse=True)
        # Slice top_n
        m = merged_top[:top_n]
        # Sort by timestamp, nanoseconds
        sorted(m, key=itemgetter("timestamp", "nanoseconds"))
        current["topN"] = m
    return current


def aggregate_rows(data_frame, level, start, stop=None, top_n=10):
    df = data_frame.loc[start:stop]
    row = df.iloc[-1]
    buy_side = df[df.tickRule == 1]
    data = {
        "date": row.timestamp.date(),
        "timestamp": row.timestamp,
        "nanoseconds": row.nanoseconds,
        "price": row.price,
        "level": level,  # Maybe decremented
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


def get_top_n(data_frame, top_n=10):
    if top_n:
        top_n = data_frame.nlargest(top_n, "volume")
        top = top_n.to_dict("records")
        for record in top:
            for key in list(record):
                if key not in (
                    "timestamp",
                    "nanoseconds",
                    "price",
                    "slippage",
                    "volume",
                    "notional",
                    "exponent",
                    "tickRule",
                ):
                    del record[key]
        top.sort(key=itemgetter("timestamp", "nanoseconds"))
        return top
    return []


def assert_higher_or_lower(level, cache):
    assert level < cache["level"] or level > cache["level"]


def assert_bounds(row, cache, box_size):
    high, low = get_bounds(cache, box_size)
    assert low <= cache["level"] <= high
    assert high == low + (box_size * 2)
