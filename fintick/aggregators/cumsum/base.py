import pandas as pd

from ..base import BaseCacheAggregator
from .lib import (
    aggregate_cumsum,
    get_cache_for_era_length,
    get_initial_cumsum_cache,
    parse_cumsum_attr,
    parse_era_length,
)

# Notional 1500, topN 150


class BaseCumsumAggregator(BaseCacheAggregator):
    def __init__(
        self,
        source_table,
        cumsum_attr,
        cumsum_value,
        era_length="M",
        top_n=10,
        **kwargs,
    ):
        self.thresh_attr = parse_cumsum_attr(cumsum_attr)
        self.thresh_value = int(cumsum_value)
        self.era_length = parse_era_length(era_length)
        destination_table = (
            f"{source_table}_{self.thresh_attr}{self.thresh_value}{self.era_length}"
        )
        if top_n:
            destination_table += f"_top{top_n}"
        super().__init__(source_table, destination_table, **kwargs)
        self.top_n = top_n

    def get_initial_cache(self, data_frame):
        cache = get_initial_cumsum_cache(self.thresh_attr)
        return data_frame, cache

    def get_cache(self, data_frame):
        data_frame, data = super().get_cache(data_frame)
        # Reinitialize cache for new era
        data = get_cache_for_era_length(
            data, self.timestamp_from, self.era_length, self.cumsum_attr
        )
        return data_frame, data

    def process_data_frame(self, data_frame, cache):
        if self.futures:
            samples = []
            for symbol in data_frame.symbol.unique():
                df = data_frame[data_frame.symbol == symbol]

                data, cache = aggregate_cumsum(
                    df, cache, self.cumsum_attr, self.cumsum_value, top_n=self.top_n
                )
                samples.append(data)
            if all([isinstance(sample, pd.DataFrame) for sample in samples]):
                data = pd.concat(samples)
            else:
                data = samples
        else:
            data, cache = aggregate_cumsum(
                data_frame, cache, self.cumsum_attr, self.cumsum_value, top_n=self.top_n
            )
        return data, cache