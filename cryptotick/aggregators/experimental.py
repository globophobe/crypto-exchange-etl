from itertools import tee

QUANTILES = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]


def quantile_iterator(quantiles):
    a, b = tee(quantiles)
    next(b, None)
    return zip(a, b)


def get_histogram(data_frame, quantiles=QUANTILES):
    histogram = []
    for index, quantiles in enumerate(quantile_iterator(quantiles)):
        is_last = index + 2 == len(quantiles)  # b/c iterate by 2
        lower_quantile, upper_quantile = quantiles
        lower_q = data_frame.notional.quantile(lower_quantile)
        upper_q = data_frame.notional.quantile(upper_quantile)
        lower_bound = data_frame.notional >= lower_q
        if is_last:
            upper_bound = data_frame.notional <= upper_q
        else:
            upper_bound = data_frame.notional < upper_q
        df = data_frame[lower_bound & upper_bound]
        buy_side = df[df.tickRule == 1]
        if len(df):
            value = {
                "lower": lower_quantile,
                "upper": upper_quantile,
                "slippage": df.slippage.sum(),
                "buySlippage": buy_side.slippage.sum(),
                "volume": df.volume.sum(),
                "buyVolume": buy_side.volume.sum(),
                "notional": df.notional.sum(),
                "buyNotional": buy_side.notional.sum(),
                "ticks": len(df),
                "buyTicks": len(buy_side),
            }
            histogram.append(value)
    return histogram
