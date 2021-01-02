from ...bqloader import get_table_name
from ...fscache import FirestoreCache, get_collection_name
from ...utils import publish
from .base import BaseBitmexETL
from .constants import BITMEX, MIN_DATE


class BitmexPerpetualETL(BaseBitmexETL):
    def __init__(
        self,
        symbols,
        date_from=None,
        date_to=None,
        aggregate=False,
        post_aggregation=[],
        verbose=False,
    ):
        # Multiple symbols.
        self.symbols = symbols
        super().__init__(
            BITMEX,
            self.symbols[0],
            MIN_DATE,
            date_from=date_from,
            date_to=date_to,
            aggregate=aggregate,
            post_aggregation=post_aggregation,
            verbose=verbose,
        )

    @property
    def log_prefix(self):
        symbols = " ".join(self.symbols)
        return f"{self.exchange_display} {symbols}"

    def has_data(self, date):
        """Firestore cache for each symbol, all symbols have data."""
        document = date.isoformat()
        for symbol in self.symbols:
            collection = get_collection_name(self.exchange, suffix=symbol)
            if not FirestoreCache(collection).has_data(document):
                return False
        if self.verbose:
            print(f"{self.log_prefix}: {document} OK")
        return True

    def process_dataframe(self, data_frame):
        """Filter dataframe by symbol, and write filtered to BigQuery table."""
        data_frame = self.parse_dataframe(data_frame)
        if len(data_frame):
            for symbol in self.symbols:
                # Filter symbol.
                df = data_frame[data_frame["symbol"] == symbol]
                if len(df):
                    self.symbol = symbol
                    self.write(df)
                else:
                    print(f"{self.log_prefix} {symbol}: No data")
        else:
            print(f"{self.log_prefix}: No data")

    def aggregate_trigger(self):
        for symbol in self.symbols:
            table_name = get_table_name(BITMEX, suffix=symbol)
            data = {
                "table_name": table_name,
                "date": self.date_from,
                "post_aggregation": self.post_aggregation,
            }
            publish("trade-aggregator", data)
