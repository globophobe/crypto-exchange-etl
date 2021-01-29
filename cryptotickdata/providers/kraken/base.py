from ...cryptotickdata import CryptoTickSequentialIntegerMixin
from .api import get_kraken_api_timestamp, get_trades
from .constants import KRAKEN


class KrakenMixin(CryptoTickSequentialIntegerMixin):
    """
    From websocket docs https://docs.kraken.com/websockets/#message-trade

    price	decimal	Price
    volume	decimal	Volume
    time	decimal	Time, seconds since epoch
    side	string	Triggering order side, buy/sell
    orderType	string	Triggering order type market/limit
    misc	string	Miscellaneous
    """

    @property
    def exchange(self):
        return KRAKEN

    def iter_api(self, symbol, pagination_id):
        return get_trades(symbol, self.timestamp_from, pagination_id, self.log_prefix)

    def get_uid(self, trade):
        return ""  # No uid

    def get_timestamp(self, trade):
        return get_kraken_api_timestamp(trade)

    def get_nanoseconds(self, trade):
        return 0

    def get_price(self, trade):
        return float(trade[0])

    def get_volume(self, trade):
        return self.get_price(trade) * self.get_notional(trade)

    def get_notional(self, trade):
        return float(trade[1])

    def get_tick_rule(self, trade):
        return 1 if trade[3] == 'b' else -1

    def get_index(self, trade):
        return 0  # No index
