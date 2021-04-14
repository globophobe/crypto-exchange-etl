import pandas as pd


def is_sample(data_frame):
    # For speed, short-circuit
    ticks = data_frame.tickRule.unique()
    is_equal_tick = len(ticks) == 1
    if is_equal_tick:
        tick_rule = ticks[0]
        if tick_rule == 1:
            is_monotonic = data_frame.price.is_monotonic
        else:
            is_monotonic = data_frame.price.is_monotonic_decreasing
        if is_monotonic:
            is_equal_timestamp = len(data_frame.timestamp.unique()) == 1
            if is_equal_timestamp:
                is_equal_nanoseconds = len(data_frame.nanoseconds.unique()) == 1
                if is_equal_nanoseconds:
                    is_equal_tick = len(data_frame.tickRule.unique()) == 1
                    if is_equal_tick:
                        if "symbol" in data_frame.columns:
                            is_equal_symbol = len(data_frame.symbol.unique()) == 1
                            if is_equal_symbol:
                                return False
                        else:
                            return False
    return True


def aggregate_trades(data_frame):
    idx = 0
    samples = []
    total_rows = len(data_frame) - 1
    # Were there two or more trades?
    if len(data_frame) > 1:
        for row in data_frame.itertuples():
            index = row.Index
            last_index = index - 1
            if index > 0:
                is_last_iteration = index == total_rows
                df = data_frame.loc[last_index:index]
                # Is this the last iteration?
                if is_last_iteration:
                    df = data_frame.loc[idx:index]
                    # If equal, one sample
                    if not is_sample(df):
                        # Aggregate from idx to end of data frame
                        sample = data_frame.loc[idx:]
                        samples.append(aggregate_trade(sample))
                    # Otherwise, two samples.
                    else:
                        # Aggregate from idx to last_index
                        sample = data_frame.loc[idx:last_index]
                        samples.append(aggregate_trade(sample))
                        # Append last row.
                        sample = data_frame.loc[index:]
                        assert len(sample) == 1
                        samples.append(aggregate_trade(sample))
                # Is the last row equal to the current row?
                elif is_sample(df):
                    # Aggregate from idx to last_index
                    sample = data_frame.loc[idx:last_index]
                    aggregated_sample = aggregate_trade(sample)
                    samples.append(aggregated_sample)
                    idx = index
    # Only one trade in data_frame
    elif len(data_frame) == 1:
        aggregated_sample = aggregate_trade(data_frame)
        samples.append(aggregated_sample)
    return pd.DataFrame(samples)


def aggregate_trade(data_frame):
    last_row = data_frame.iloc[-1]
    timestamp = last_row.timestamp
    last_price = last_row.price
    ticks = len(data_frame)
    # Is there more than 1 trade to aggregate?
    if ticks > 1:
        first_row = data_frame.iloc[0]
        first_price = first_row.price
        volume = data_frame.volume.sum()
        notional = data_frame.notional.sum()
        # Did price change?
        if first_price != last_price:
            vwap = volume / notional
        else:
            vwap = last_price
    else:
        vwap = last_price
        volume = last_row.volume
        notional = last_row.notional
    data = {
        "timestamp": timestamp,
        "nanoseconds": last_row.nanoseconds,
        "price": last_price,
        "vwap": vwap,
        "volume": volume,
        "notional": notional,
        "ticks": ticks,
        "tickRule": last_row.tickRule,
    }
    if "symbol" in data_frame.columns:
        data.update({"symbol": last_row.symbol})
    return data
