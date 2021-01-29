from .api import format_ftx_api_timestamp, get_ftx_api_timestamp, get_trades
from .constants import FTX


class FTXMixin:
    @property
    def exchange(self):
        return FTX

    @property
    def exchange_display(self):
        return self.exchange.upper()

    def get_pagination_id(self, data=None):
        return format_ftx_api_timestamp(self.timestamp_to)

    def iter_api(self, symbol, pagination_id, log_prefix):
        return get_trades(symbol, self.timestamp_from, pagination_id, log_prefix)

    def get_uid(self, trade):
        return str(trade["id"])

    def get_timestamp(self, trade):
        return get_ftx_api_timestamp(trade)

    def get_nanoseconds(self, trade):
        return 0  # No nanoseconds

    def get_price(self, trade):
        return float(trade["price"])

    def get_volume(self, trade):
        return self.get_price(trade) * self.get_notional(trade)

    def get_notional(self, trade):
        return float(trade["size"])

    def get_tick_rule(self, trade):
        return 1 if trade["side"] == "buy" else -1

    def get_index(self, trade):
        return int(trade["id"])
