from ..base import BaseAggregator
from .lib import (
    aggregate_threshold,
    get_cache_for_era_length,
    get_initial_threshold_cache,
    parse_era_length,
    parse_thresh_attr,
)

# Notional 1500, topN 150


class ThresholdAggregator(BaseAggregator):
    def __init__(
        self,
        source_table,
        destination_table,
        thresh_attr,
        thresh_value,
        era_length="M",
        top_n=10,
        **kwargs,
    ):
        super().__init__(source_table, destination_table, **kwargs)
        self.thresh_attr = parse_thresh_attr(thresh_attr)
        self.thresh_value = int(thresh_value)
        self.era_length = parse_era_length(era_length)
        self.top_n = top_n

    def get_initial_cache(self, data_frame):
        cache = get_initial_threshold_cache(self.thresh_attr)
        return data_frame, cache

    def get_cache_for_era(self, date, cache):
        return get_cache_for_era_length(date, self.era_length, self.thresh_attr, cache)

    def process_data_frame(self, data_frame, cache):
        return aggregate_threshold(
            data_frame, cache, self.thresh_attr, self.thresh_value, top_n=self.top_n
        )
