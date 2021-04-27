from decimal import Decimal

from ...fintick import FinTickSequentialIntegerMixin
from .api import get_bitflyer_api_timestamp, get_trades
from .constants import BITFLYER


class BitflyerMixin(FinTickSequentialIntegerMixin):
    @property
    def exchange(self):
        return BITFLYER

    def iter_api(self, symbol, pagination_id, log_prefix):
        return get_trades(symbol, self.timestamp_from, pagination_id, self.log_prefix)

    def get_uid(self, trade):
        return str(trade["id"])

    def get_timestamp(self, trade):
        return get_bitflyer_api_timestamp(trade)

    def get_nanoseconds(self, trade):
        return self.get_timestamp(trade).nanosecond

    def get_price(self, trade):
        return Decimal(trade["price"])

    def get_volume(self, trade):
        return self.get_price(trade) * self.get_notional(trade)

    def get_notional(self, trade):
        return Decimal(trade["size"])

    def get_tick_rule(self, trade):
        return 1 if trade["side"] == "BUY" else -1

    def get_index(self, trade):
        return int(trade["id"])

    def get_is_complete(self, trades):
        data = self.firestore_cache.get_one(
            where=["open.index", "==", trades[0]["index"] + 1]
        )
        return data is not None
