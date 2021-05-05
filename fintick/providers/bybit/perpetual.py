from ...controllers import FinTick, FinTickHourlyMixin, FinTickREST
from .base import BybitDailyS3Mixin, BybitMixin, BybitRESTMixin


class BybitPerpetualHourlyPartition(FinTickHourlyMixin, BybitRESTMixin, FinTickREST):
    pass


class BybitPerpetualDailyPartition(BybitDailyS3Mixin, BybitMixin, FinTick):
    pass
