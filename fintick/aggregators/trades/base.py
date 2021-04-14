from ...bqloader import (
    MULTIPLE_SYMBOL_AGGREGATE_SCHEMA,
    MULTIPLE_SYMBOL_SCHEMA,
    SINGLE_SYMBOL_AGGREGATE_SCHEMA,
    SINGLE_SYMBOL_SCHEMA,
)
from ..base import BaseAggregator
from .lib import aggregate_trades


class BaseTradeAggregator(BaseAggregator):
    def __init__(self, source_table, **kwargs):
        destination_table = source_table + "_aggregated"
        super().__init__(source_table, destination_table, **kwargs)

    @property
    def source_schema(self):
        if self.has_multiple_symbols:
            return MULTIPLE_SYMBOL_SCHEMA
        else:
            return SINGLE_SYMBOL_SCHEMA

    @property
    def schema(self):
        if self.has_multiple_symbols:
            return MULTIPLE_SYMBOL_AGGREGATE_SCHEMA
        else:
            return SINGLE_SYMBOL_AGGREGATE_SCHEMA

    def process_data_frame(self, data_frame):
        df = aggregate_trades(data_frame)
        df["index"] = df.index
        return df[self.columns]
