from ..bqloader import get_schema_columns
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

    def get_source(self, sep="_"):
        return self.table_name.replace("_", sep)

    def get_destination(self, sep="_"):
        name = self.get_source(sep=sep)
        return f"{name}{sep}aggregated"

    @property
    def log_prefix(self):
        name = self.get_destination(sep=" ")
        return name[0].capitalize() + name[1:]  # Capitalize first letter

    @property
    def schema(self):
        raise NotImplementedError

    @property
    def columns(self):
        return get_schema_columns(self.schema)

    def get_min_date(self):
        data = self.firestore_source.get_one()
        if data:
            if self.has_multiple_symbols:
                dates = []
                for key, symbol in data.items():
                    if isinstance(symbol, dict):
                        if "candles" in symbol:
                            for candle in symbol["candles"]:
                                # Maybe no trades
                                if len(candle):
                                    date = candle["open"]["timestamp"].date()
                                    dates.append(date)
                                    break
                return min(dates)
            else:
                for candle in data["candles"]:
                    # Maybe no trades
                    if len(candle):
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

    def init_cache(self, data_frame):
        raise NotImplementedError

    @property
    def cache(self):
        return getattr(self, "_cache", None)

    @cache.setter
    def cache(self, data):
        self._cache = data
