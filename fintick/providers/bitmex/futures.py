from ...controllers import FinTick, FinTickHourlyMixin, FinTickMultiSymbolREST
from .base import BitmexDailyMultiSymbolS3Mixin, BitmexMixin


class BitmexFuturesHourlyPartition(
    FinTickHourlyMixin, BitmexMixin, FinTickMultiSymbolREST
):
    pass


class BitmexFuturesDailyPartition(BitmexDailyMultiSymbolS3Mixin, FinTick):
    def has_data(self, date):
        # No active symbols 2016-10-01 to 2016-10-25.
        return super().has_data(date)
