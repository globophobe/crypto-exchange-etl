from decimal import Decimal

from ...cryptotickdata import CryptoTickSequentialIntegerMixin
from .api import get_binance_api_timestamp, get_trades
from .constants import BINANCE


class BinanceMixin(CryptoTickSequentialIntegerMixin):
    @property
    def exchange(self):
        return BINANCE

    def iter_api(self, symbol, pagination_id, log_prefix):
        return get_trades(symbol, self.timestamp_from, pagination_id, log_prefix)

    def get_uid(self, trade):
        return str(trade["id"])

    def get_timestamp(self, trade):
        return get_binance_api_timestamp(trade)

    def get_nanoseconds(self, trade):
        # TODO: Verify if there are every nanoseconds
        return self.get_timestamp(trade).nanosecond

    def get_price(self, trade):
        return Decimal(trade["price"])

    def get_volume(self, trade):
        return self.get_price(trade) * self.get_notional(trade)

    def get_notional(self, trade):
        return Decimal(trade["qty"])

    def get_tick_rule(self, trade):
        # If isBuyerMaker is true, order was filled by sell order
        return 1 if not trade["isBuyerMaker"] else -1

    def get_index(self, trade):
        return int(trade["id"])

    def get_is_complete(self, trades):
        data = self.firestore_cache.get_one(
            where=["open.index", "==", trades[0]["index"] + 1]
        )
        return data is not None

    def assert_data_frame(self, data_frame, trades):
        # Duplicates.
        assert len(data_frame["uid"].unique()) == len(trades)
        # Missing orders.
        expected = len(trades) - 1
        diff = data_frame["index"].diff().dropna()
        assert abs(diff.sum()) == expected
