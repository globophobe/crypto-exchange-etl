from ...cryptotickdata import CryptoTick, CryptoTickHourlyMixin, CryptoTickREST
from .base import BybitDailyS3Mixin, BybitMixin, BybitRESTMixin


class BybitPerpetualHourlyPartition(
    CryptoTickHourlyMixin, BybitRESTMixin, CryptoTickREST
):
    pass


class BybitPerpetualDailyPartition(BybitDailyS3Mixin, BybitMixin, CryptoTick):
    pass
