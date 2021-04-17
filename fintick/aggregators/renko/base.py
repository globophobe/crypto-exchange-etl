import pandas as pd

from ...bqloader import MULTIPLE_SYMBOL_RENKO_SCHEMA, SINGLE_SYMBOL_RENKO_SCHEMA
from ..base import BaseCacheAggregator
from .lib import aggregate_renko, get_initial_cache


class BaseRenkoAggregator(BaseCacheAggregator):
    def __init__(self, source_table, box_size, reversal=1, top_n=0, **kwargs):
        destination_table = f"{source_table}_renko_{box_size}"
        if top_n:
            destination_table += f"_top{top_n}"
        super().__init__(source_table, destination_table, **kwargs)
        self.box_size = float(box_size)
        self.reversal = reversal
        self.top_n = top_n

    @property
    def schema(self):
        if self.has_multiple_symbols:
            return MULTIPLE_SYMBOL_RENKO_SCHEMA
        else:
            return SINGLE_SYMBOL_RENKO_SCHEMA

    def get_initial_cache(self, data_frame):
        return get_initial_cache(data_frame, self.box_size)

    def process_data_frame(self, data_frame, cache):
        if self.has_multiple_symbols:
            samples = []
            for symbol in data_frame.symbol.unique():
                df = data_frame[data_frame.symbol == symbol]
                data, cache = aggregate_renko(
                    df,
                    cache,
                    self.box_size,
                    top_n=self.top_n,
                )
                samples.append(data)
            if all([isinstance(sample, pd.DataFrame) for sample in samples]):
                data = pd.concat(samples)
            else:
                data = samples
        else:
            data, cache = aggregate_renko(
                data_frame,
                cache,
                self.box_size,
                top_n=self.top_n,
            )
        return data, cache
