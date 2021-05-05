from ...controllers import FinTick, FinTickHourlyMixin, FinTickREST
from .base import BitmexDailyS3Mixin, BitmexMixin, BitmexRESTMixin


class BitmexPerpetualHourlyPartition(FinTickHourlyMixin, BitmexRESTMixin, FinTickREST):
    pass


class BitmexPerpetualDailyPartition(BitmexDailyS3Mixin, BitmexMixin, FinTick):
    pass
