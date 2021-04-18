import pandas as pd
import pendulum

from ..lib import aggregate_rows


def get_initial_candle_cache(data_frame):
    cache = {}
    if "symbol" in data_frame.columns:
        for symbol in data_frame.symbol.unique():
            df = data_frame[data_frame.symbol == symbol]
            cache[symbol] = df.loc[0].price
    else:
        cache["open"] = data_frame.loc[0].price
    return cache


def get_range(timeframe):
    total_seconds = timeframe.total_seconds()
    one_minute = 60.0
    one_hour = 3600.0
    if total_seconds < one_minute:
        unit = "seconds"
        step = total_seconds
    elif total_seconds >= one_minute and total_seconds <= one_hour:
        unit = "minutes"
        step = total_seconds / one_minute
    elif total_seconds > one_hour:
        unit = "hours"
        step = total_seconds / one_hour
    else:
        raise NotImplementedError
    step = int(step)
    assert total_seconds <= one_hour, f"{step} {unit} not supported"
    assert 60 % step == 0, f"{step} not divisible by 60"
    return unit, step


def aggregate_candles(
    data_frame, cache, timestamp_from, timestamp_to, timeframe, top_n=0
):
    samples = []
    period = pendulum.period(timestamp_from, timestamp_to)
    unit, step = get_range(timeframe)
    for index, start in enumerate(period.range(unit, step)):
        end = start + pd.Timedelta(f"{step} {unit}")
        df = data_frame[(data_frame.timestamp >= start) & (data_frame.timestamp < end)]
        # Maybe trades
        if len(df):
            if "symbol" in df.columns:
                symbols = df.symbol.unique()
                assert len(symbols) == 1
                open_price = cache[symbols[0]]
            else:
                open_price = cache["open"]
            sample = aggregate_rows(
                df,
                # Open timestamp, or won't be in partition
                timestamp=start,
                open_price=open_price,
                top_n=top_n,
            )
            cache["open"] = sample["close"]
            samples.append(sample)
    data = pd.DataFrame(samples) if not top_n else samples
    return data, cache
