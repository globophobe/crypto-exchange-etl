import httpx
import pandas as pd

from ...cryptotickdata import CryptoTickDailyS3Mixin, CryptoTickSequentialIntegerMixin
from .api import get_bybit_api_timestamp, get_trades
from .constants import BYBIT, MAX_RESULTS, S3_URL


class BybitMixin:
    @property
    def exchange(self):
        return BYBIT

    def get_uid(self, trade):
        return str(trade["id"])

    def get_timestamp(self, trade):
        return get_bybit_api_timestamp(trade)

    def get_nanoseconds(self, trade):
        return 0  # No nanoseconds

    def get_price(self, trade):
        return trade["price"]

    def get_volume(self, trade):
        return trade["qty"]

    def get_notional(self, trade):
        return self.get_volume(trade) / self.get_price(trade)

    def get_tick_rule(self, trade):
        return 1 if trade["side"] == "Buy" else -1

    def get_index(self, trade):
        return trade["id"]


class BybitRESTMixin(CryptoTickSequentialIntegerMixin, BybitMixin):
    def get_pagination_id(self, data=None):
        pagination_id = super().get_pagination_id(data=data)
        # Bybit API is seriously donkey balls
        if pagination_id is not None:
            pagination_id = pagination_id - MAX_RESULTS
            assert pagination_id > 0
        return pagination_id

    def iter_api(self, symbol, pagination_id, log_prefix):
        return get_trades(symbol, self.timestamp_from, pagination_id, log_prefix)


class BybitDailyS3Mixin(CryptoTickDailyS3Mixin):
    def get_url(self, date):
        directory = f"{S3_URL}{self.symbol}/"
        response = httpx.get(directory)
        if response.status_code == 200:
            return f"{S3_URL}{self.symbol}/{self.symbol}{date.isoformat()}.csv.gz"
        else:
            print(f"{self.exchange.capitalize()} {self.symbol}: No data")

    @property
    def get_columns(self):
        return ("trdMatchID", "timestamp", "price", "size", "tickDirection")

    def parse_dataframe(self, data_frame):
        # No false positives.
        # Source: https://pandas.pydata.org/pandas-docs/stable/user_guide/
        # indexing.html#returning-a-view-versus-a-copy
        pd.options.mode.chained_assignment = None
        # Bybit is reversed.
        data_frame = data_frame.iloc[::-1]
        data_frame["index"] = data_frame.index.values[::-1]
        data_frame["timestamp"] = pd.to_datetime(data_frame["timestamp"], unit="s")
        data_frame = data_frame.rename(columns={"trdMatchID": "uid", "size": "volume"})
        return super().parse_dataframe(data_frame)
