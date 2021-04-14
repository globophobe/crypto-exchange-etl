import pandas as pd


def is_sample(data_frame, first_index, last_index):
    first_row = data_frame.loc[first_index]
    last_row = data_frame.loc[last_index]
    # For speed, short-circuit
    if first_row.timestamp == last_row.timestamp:
        if first_row.nanoseconds == last_row.nanoseconds:
            if first_row.tickRule == last_row.tickRule:
                if "symbol" in data_frame.columns:
                    if first_row.symbol == last_row.symbol:
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
                # Is this the last iteration?
                if is_last_iteration:
                    # If equal, one sample
                    if not is_sample(data_frame, idx, index):
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
                elif is_sample(data_frame, last_index, index):
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
