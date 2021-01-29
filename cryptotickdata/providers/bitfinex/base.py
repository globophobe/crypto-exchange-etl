import numpy as np

from ...cryptotickdata import CryptoTickNonSequentialIntegerMixin
from .api import get_bitfinex_api_timestamp, get_trades
from .constants import BITFINEX


class BitfinexMixin(CryptoTickNonSequentialIntegerMixin):
    """
    Details: https://docs.bitfinex.com/reference#rest-public-trades

    ID	int	ID of the trade
    MTS	int	millisecond time stamp
    ±AMOUNT	float	How much was bought (positive) or sold (negative).
    PRICE	float	Price at which the trade was executed (trading tickers only)
    RATE	float	Rate at which funding transaction occurred (funding tickers only)
    PERIOD	int	Amount of time the funding transaction was for (funding tickers only)
    """

    @property
    def exchange(self):
        return BITFINEX

    def get_pagination_id(self, data=None):
        return self.timestamp_to.timestamp() * 1000

    def iter_api(self, symbol, pagination_id, log_prefix):
        return get_trades(symbol, self.timestamp_from, pagination_id, log_prefix)

    def get_uid(self, trade):
        return trade[0]

    def get_timestamp(self, trade):
        return get_bitfinex_api_timestamp(trade)

    def get_nanoseconds(self, trade):
        return 0

    def get_price(self, trade):
        return float(trade[3])

    def get_volume(self, trade):
        return float(trade[3]) * float(abs(trade[2]))

    def get_notional(self, trade):
        return float(abs(trade[2]))

    def get_tick_rule(self, trade):
        # Buy side indicates a down-tick because the maker was a buy order and
        # their order was removed. Conversely, sell side indicates an up-tick.
        return np.sign(trade[2])

    def get_index(self, trade):
        return trade[0]

    def get_is_complete(self, trades):
        data = self.firestore_cache.get_one(
            where=["open.index", "==", trades[0]["index"] + 1]
        )
        return data is not None
