from ...cryptotickdata import (
    CryptoTick,
    CryptoTickHourlyMixin,
    CryptoTickMultiSymbolREST,
)
from .base import BitmexDailyMultiSymbolS3Mixin, BitmexMixin


class BitmexFuturesHourlyPartition(
    CryptoTickHourlyMixin, BitmexMixin, CryptoTickMultiSymbolREST
):
    pass


class BitmexFuturesDailyPartition(BitmexDailyMultiSymbolS3Mixin, CryptoTick):
    def has_data(self, date):
        # No active symbols 2016-10-01 to 2016-10-25.
        return super().has_data(date)
