from ..thresh.lib import aggregate_rows, get_next_cache, merge_cache


def aggregate_threshold_join(data_frame, cache, timestamps, top_n=10):
    samples = []
    for index, timestamp in enumerate(timestamps):
        if index > 0:
            last_timestamp = timestamps[index - 1]
            df = data_frame[
                (data_frame.timestamp > last_timestamp["timestamp"])
                & (data_frame.timestamp <= timestamp["timestamp"])
            ]
        else:
            df = data_frame[data_frame.timestamp <= timestamp["timestamp"]]
        # Maybe first sample
        if not len(df):
            if "nextDay" in cache:
                sample = cache.pop("nextDay")
                sample["date"] = data_frame.iloc[0].timestamp.date()
            else:
                raise NotImplementedError
        else:
            sample = aggregate_rows(df, top_n=top_n)
        if "nextDay" in cache:
            previous = cache.pop("nextDay")
            sample = merge_cache(previous, sample, top_n=top_n)
        sample["uuid"] = timestamp["uuid"]
        samples.append(sample)
    # Cache
    if len(timestamps):
        df = data_frame[data_frame.timestamp >= timestamp["timestamp"]]
        if len(df):
            cache = get_next_cache(df, cache, top_n=top_n)
    return samples, cache
