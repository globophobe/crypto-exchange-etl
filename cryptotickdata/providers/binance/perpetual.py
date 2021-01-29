from ...cryptotickdata import (
    CryptoTickDailyMixin,
    CryptoTickHourlyMixin,
    CryptoTickREST,
)
from .base import BinanceMixin


class BinancePerpetualHourlyPartition(
    CryptoTickHourlyMixin, BinanceMixin, CryptoTickREST
):
    pass


class BinancePerpetualDailyPartition(
    CryptoTickDailyMixin, BinanceMixin, CryptoTickREST
):
    pass
