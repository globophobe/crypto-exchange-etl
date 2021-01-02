from ...bqloader import MULTIPLE_SYMBOL_AGGREGATE_SCHEMA, get_table_name
from ...cryptotick import FuturesETL
from ...utils import publish
from .api import get_active_futures, get_expired_futures
from .base import BaseBitmexETL
from .constants import BITMEX, MIN_DATE, URL


class BitmexFuturesETL(BaseBitmexETL, FuturesETL):
    def __init__(
        self,
        root_symbol,
        date_from=None,
        date_to=None,
        schema=MULTIPLE_SYMBOL_AGGREGATE_SCHEMA,
        aggregate=False,
        post_aggregation=[],
        verbose=False,
    ):
        self.exchange = BITMEX
        self.initialize_dates(MIN_DATE, date_from, date_to)
        self.root_symbol = root_symbol
        self.symbols = self.get_symbols(root_symbol)
        self.symbol = self.symbols[0]["symbol"]
        self.schema = schema
        self.aggregate = aggregate
        self.post_aggregation = post_aggregation
        self.verbose = verbose

    def get_symbols(self, root_symbol):
        active_futures = get_active_futures(
            root_symbol, date_from=self.date_from, verbose=False
        )
        expired_futures = get_expired_futures(
            root_symbol,
            date_from=self.date_from,
            verbose=False,
        )
        return active_futures + expired_futures

    def get_suffix(self, sep="-"):
        return f"{self.root_symbol}USD{sep}futures"

    def get_url(self, date):
        date_string = date.strftime("%Y%m%d")
        return f"{URL}{date_string}.csv.gz"

    def has_data(self, date):
        # No active symbols 2016-10-01 to 2016-10-25.
        return super().has_data(date)

    def process_dataframe(self, data_frame):
        # Filter symbols.
        query = " | ".join([f'symbol == "{s["symbol"]}"' for s in self.active_symbols])
        data_frame = data_frame.query(query)
        if len(data_frame):
            df = self.parse_dataframe(data_frame)
            if len(df):
                self.write(df)
            else:
                print(f"{self.log_prefix}: No data")
        else:
            print(f"{self.log_prefix}: No data")

    def aggregate_trigger(self):
        table_name = get_table_name(BITMEX, suffix=self.get_suffix)
        data = {
            "table_name": table_name,
            "date": self.date_from,
            "has_multiple_symbols": True,
            "post_aggregation": self.post_aggregation,
        }
        publish("trade-aggregator", data)
