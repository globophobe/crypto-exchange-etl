from ...cryptotickdata import CryptoTick, CryptoTickHourlyMixin, CryptoTickREST
from .base import BitmexDailyS3Mixin, BitmexMixin, BitmexRESTMixin

# ETH Slippage?!


class BitmexPerpetualHourlyPartition(
    CryptoTickHourlyMixin, BitmexRESTMixin, CryptoTickREST
):
    pass


class BitmexPerpetualDailyPartition(BitmexDailyS3Mixin, BitmexMixin, CryptoTick):
    pass
