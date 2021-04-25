from decimal import Decimal

import numpy as np

from ...fintick import FinTickNonSequentialIntegerMixin
from .api import get_bitfinex_api_timestamp, get_trades
from .constants import BITFINEX


class BitfinexMixin(FinTickNonSequentialIntegerMixin):
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

    @property
    def symbol(self):
        return self.api_symbol[1:]  # API symbol prepended with t

    def get_pagination_id(self, data=None):
        return int(self.timestamp_to.timestamp() * 1000)  # Millisecond

    def iter_api(self, symbol, pagination_id, log_prefix):
        return get_trades(symbol, self.timestamp_from, pagination_id, log_prefix)

    def get_uid(self, trade):
        return str(trade[0])

    def get_timestamp(self, trade):
        return get_bitfinex_api_timestamp(trade)

    def get_nanoseconds(self, trade):
        return self.get_timestamp(trade).nanosecond

    def get_price(self, trade):
        return Decimal(trade[3])

    def get_volume(self, trade):
        return self.get_price(trade) * self.get_notional(trade)

    def get_notional(self, trade):
        return abs(Decimal(trade[2]))

    def get_tick_rule(self, trade):
        # Buy side indicates a down-tick because the maker was a buy order and
        # their order was removed. Conversely, sell side indicates an up-tick.
        return np.sign(trade[2])

    def get_index(self, trade):
        return trade[0]

    def get_data_frame(self, trades):
        # Websocket sends trades in order, by incrementing non sequential integer
        # REST API returns results unsorted
        # Sort by uid, reversed
        trades.sort(key=lambda x: x["index"], reverse=True)
        return super().get_data_frame(trades)
