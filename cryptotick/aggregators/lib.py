import math


def is_sample(last_row, row, has_multiple_symbols):
    if has_multiple_symbols:
        is_equal_symbol = last_row.symbol == row.symbol
    else:
        is_equal_symbol = True
    is_equal_timestamp = last_row.timestamp == row.timestamp
    is_equal_nanoseconds = last_row.nanoseconds == row.nanoseconds
    is_equal_tick = last_row.tickRule == row.tickRule
    return (
        not is_equal_symbol
        or not is_equal_timestamp
        or not is_equal_nanoseconds
        or not is_equal_tick
    )


def aggregate_trades(data_frame, has_multiple_symbols=False):
    idx = 0
    samples = []
    total_rows = len(data_frame) - 1
    for row in data_frame.itertuples():
        index = row.Index
        last_index = index - 1
        if index > 0:
            is_last_iteration = index == total_rows
            last_row = data_frame.loc[last_index]
            # Is this the last iteration?
            if is_last_iteration:
                last_row = data_frame.loc[idx]
                next_row = data_frame.loc[index]
                # If equal, one sample.
                if not is_sample(last_row, next_row, has_multiple_symbols):
                    # Aggregate from idx to end of data frame.
                    sample = data_frame.loc[idx:]
                    samples.append(aggregate_trade(sample, has_multiple_symbols))
                # Otherwise, two samples.
                else:
                    # Aggregate from idx to last_index
                    sample = data_frame.loc[idx:last_index]
                    samples.append(aggregate_trade(sample, has_multiple_symbols))
                    # Append last row.
                    sample = data_frame.loc[index:]
                    assert len(sample) == 1
                    samples.append(aggregate_trade(sample, has_multiple_symbols))
            # Is the last row equal to the current row?
            elif is_sample(last_row, row, has_multiple_symbols):
                # Aggregate from idx to last_index
                sample = data_frame.loc[idx:last_index]
                aggregated_sample = aggregate_trade(sample, has_multiple_symbols)
                samples.append(aggregated_sample)
                idx = index
    return samples


def aggregate_trade(data_frame, has_multiple_symbols):
    last_row = data_frame.iloc[-1]
    timestamp = last_row.timestamp
    notional = data_frame.notional.sum()
    slippage = calculate_slippage(data_frame, notional)
    data = {
        "date": timestamp.date(),
        "timestamp": timestamp,
        "nanoseconds": last_row.nanoseconds,
        "price": last_row.price,
        "slippage": slippage,
        "volume": data_frame.volume.sum(),
        "notional": notional,
        "tickRule": last_row.tickRule,
    }
    if has_multiple_symbols:
        data.update({"symbol": last_row.symbol})
    return data


def calculate_slippage(data_frame, notional):
    first_row = data_frame.iloc[0]
    if len(data_frame) > 2:
        expected = first_row.price * notional
        vwap = volume_weighted_average_price(data_frame)
        actual = vwap * notional
        slippage = abs(expected - actual)
    else:
        slippage = 0
    return slippage


def volume_weighted_average_price(data_frame):
    vwap = data_frame.volume.cumsum() / data_frame.notional.cumsum()
    return vwap.iloc[-1]


def calc_exponent(volume, divisor=10, decimal_places=1):
    if volume > 0:
        is_round = volume % math.pow(divisor, decimal_places) == 0
        if is_round:
            decimal_places += 1
            stop_execution = False
            while not stop_execution:
                is_round = volume % math.pow(divisor, decimal_places) == 0
                if is_round:
                    decimal_places += 1
                else:
                    stop_execution = True
            return decimal_places - 1
        else:
            return 0
    else:
        # WTF Bybit!
        return 0


def calculate_exponent(data_frame):
    data_frame["exponent"] = data_frame.volume.apply(calc_exponent)
    return data_frame
