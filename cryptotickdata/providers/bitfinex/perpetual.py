from ...cryptotickdata import (
    CryptoTickDailyMixin,
    CryptoTickHourlyMixin,
    CryptoTickREST,
)
from .base import BitfinexMixin


class BitfinexPerpetualHourlyPartition(
    CryptoTickHourlyMixin, BitfinexMixin, CryptoTickREST
):
    pass


class BitfinexPerpetualDailyPartition(
    CryptoTickDailyMixin, BitfinexMixin, CryptoTickREST
):
    pass
