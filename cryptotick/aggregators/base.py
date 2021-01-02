from ..cryptotick import CryptoExchangeETL
from ..fscache import FirestoreCache
from ..utils import get_delta


class BaseAggregator(CryptoExchangeETL):
    def __init__(
        self,
        table_name,
        date_from=None,
        date_to=None,
        has_multiple_symbols=False,
        post_aggregation=[],
        verbose=False,
    ):
        self.table_name = table_name
        self.has_multiple_symbols = has_multiple_symbols
        self.post_aggregation = post_aggregation
        self.verbose = verbose

        min_date = self.get_min_date()
        self.initialize_dates(min_date, date_from, date_to)

    @property
    def log_prefix(self):
        raise NotImplementedError

    def get_destination_suffix(self, sep):
        raise NotImplementedError

    def get_min_date(self):
        data = self.firestore_source.get_one()
        if data:
            for candle in data["candles"]:
                if candle:
                    return candle["open"]["timestamp"].date()

    @property
    def yesterday(self):
        return get_delta(self.date, days=-1)

    @property
    def firestore_source(self):
        collection = self.get_source(sep="-")
        return FirestoreCache(collection)

    @property
    def firestore_destination(self):
        collection = self.get_destination(sep="-")
        return FirestoreCache(collection)

    @property
    def cache(self):
        return getattr(self, "_cache", None)

    @cache.setter
    def cache(self, data):
        self._cache = data
