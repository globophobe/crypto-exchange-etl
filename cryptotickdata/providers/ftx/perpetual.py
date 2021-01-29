from ...cryptotickdata import (
    CryptoTickDailyMixin,
    CryptoTickHourlyMixin,
    CryptoTickNonSequentialIntegerMixin,
    CryptoTickREST,
)
from .base import FTXMixin


class FTXHourlyPartition(
    CryptoTickHourlyMixin, CryptoTickNonSequentialIntegerMixin, FTXMixin, CryptoTickREST
):
    pass


class FTXDailyPartition(
    CryptoTickDailyMixin, CryptoTickNonSequentialIntegerMixin, FTXMixin, CryptoTickREST
):
    pass
