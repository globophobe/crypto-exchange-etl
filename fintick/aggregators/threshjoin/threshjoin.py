from ...fscache import FirestoreCache
from ...utils import date_range
from ..base import BaseAggregator
from .base import ThresholdMixin
from .lib import aggregate_threshold_join


class ThresholdJoinAggregator(ThresholdMixin, BaseAggregator):
    def __init__(
        self,
        source_table,
        destination_table,
        timestamp_cache,
        top_n=10,
        *args,
        **kwargs,
    ):
        super().__init__(source_table, destination_table, **kwargs)

        self.timestamp_cache = FirestoreCache(timestamp_cache)
        self.top_n = top_n

    def get_initial_cache(self, data_frame):
        cache = {}
        return data_frame, cache

    def get_timestamps(self, document):
        cache = self.timestamp_cache.get(document)
        if not cache:
            print(f"{self.log_prefix}: {document} no timestamps")
        else:
            return [
                {
                    "uuid": candle["uuid"],
                    "timestamp": candle["timestamp"],
                    "nanoseconds": candle["nanoseconds"],
                }
                for candle in cache["candles"]
            ]

    def main(self):
        for date in date_range(self.date_from, self.date_to):
            self.date = date
            document = date.isoformat()
            if self.firestore_source.has_data(document):
                if not self.firestore_destination.has_data(document):
                    timestamps = self.get_timestamps(document)
                    data_frame = self.get_data_frame()
                    data_frame, cache = self.get_cache(data_frame)
                    data_frame = self.preprocess_data_frame(data_frame, cache)
                    data, cache = self.process_data_frame(data_frame, cache, timestamps)
                    self.write(data, cache)
                elif self.verbose:
                    print(f"{self.log_prefix}: {document} OK")
        print(
            f"{self.log_prefix}: "
            f"{self.date_from.isoformat()} to {self.date_to.isoformat()} OK"
        )

    def process_data_frame(self, data_frame, cache, timestamps):
        return aggregate_threshold_join(data_frame, cache, timestamps, top_n=self.top_n)
