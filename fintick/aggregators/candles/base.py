from decimal import Decimal

import pandas as pd

from ..base import BaseCacheAggregator
from .lib import aggregate_candles, get_initial_candle_cache


class BaseCandleAggregator(BaseCacheAggregator):
    def __init__(self, source_table, timeframe="1m", top_n=0, **kwargs):
        destination_table = f"{source_table}_{timeframe}"
        if top_n:
            destination_table += f"_top{top_n}"
        super().__init__(source_table, destination_table, **kwargs)
        self.timeframe = pd.Timedelta(timeframe)
        self.top_n = top_n

    def get_initial_cache(self, data_frame):
        cache = get_initial_candle_cache(data_frame)
        return data_frame, cache

    def get_cache(self, data_frame):
        data_frame, data = super().get_cache(data_frame)
        data["open"] = Decimal(data["open"])
        return data_frame, data

    def process_data_frame(self, data_frame, cache):
        if self.futures:
            samples = []
            for symbol in data_frame.symbol.unique():
                df = data_frame[data_frame.symbol == symbol]
                data, cache = aggregate_candles(
                    df,
                    cache,
                    self.timestamp_from,
                    self.timestamp_to,
                    self.timeframe,
                    top_n=self.top_n,
                )
                samples.append(data)
            if all([isinstance(sample, pd.DataFrame) for sample in samples]):
                data = pd.concat(samples)
            else:
                data = samples
        else:
            data, cache = aggregate_candles(
                data_frame,
                cache,
                self.timestamp_from,
                self.timestamp_to,
                self.timeframe,
                top_n=self.top_n,
            )
        return data, cache
