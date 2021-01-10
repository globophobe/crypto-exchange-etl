from ..bqloader import get_schema_columns
from ..cryptotick import CryptoExchangeETL
from ..fscache import FirestoreCache
from ..utils import get_delta


class BaseAggregator(CryptoExchangeETL):
    def __init__(
        self,
        table_name,
        basename=None,
        date_from=None,
        date_to=None,
        require_cache=False,
        has_multiple_symbols=False,
        verbose=False,
    ):
        self.table_name = table_name
        self.basename = basename
        self.require_cache = require_cache
        self.has_multiple_symbols = has_multiple_symbols
        self.verbose = verbose

        min_date = self.get_min_date()
        self.initialize_dates(min_date, date_from, date_to)

    def get_source(self, sep="_"):
        return self.table_name.replace("_", sep)

    def get_basename(self, sep="_"):
        if self.basename:
            name = self.basename.replace("_", sep)
        else:
            name = self.get_source(sep=sep)
        return name

    def get_destination(self, sep="_"):
        basename = self.get_basename(sep=sep)
        return f"{basename}{sep}aggregated"

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

    @property
    def firestore_source(self):
        collection = self.get_source(sep="-")
        return FirestoreCache(collection)

    @property
    def firestore_destination(self):
        collection = self.get_destination(sep="-")
        return FirestoreCache(collection)

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

    def get_cache(self, data_frame):
        document = get_delta(self.date, days=-1).isoformat()
        data = self.firestore_destination.get(document)
        # Is cache required, and no data?
        if self.require_cache and not data:
            # Is date greater than min date?
            if self.date > self.date_from:
                date = self.date.isoformat()
                assert data, f"Cache does not exist, {date}"
        if not data:
            # First row of dataframe may be discarded
            data_frame, data = self.get_initial_cache(data_frame)
        return data_frame, data

    def get_initial_cache(self, data_frame):
        raise NotImplementedError
