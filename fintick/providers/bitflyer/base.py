from decimal import Decimal

from ...controllers import (
    FinTickIntegerPaginationMixin,
    FinTickNonSequentialIntegerMixin,
)
from .api import get_bitflyer_api_timestamp, get_trades
from .constants import BITFLYER


class BitflyerMixin(FinTickIntegerPaginationMixin, FinTickNonSequentialIntegerMixin):
    """Bitflyer pagination is both by integer, and non-sequential"""

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
