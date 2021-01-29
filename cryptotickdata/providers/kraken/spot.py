from ...cryptotickdata import (
    CryptoTickDailyMixin,
    CryptoTickHourlyMixin,
    CryptoTickREST,
)
from .base import KrakenMixin


class KrakenSpotHourlyPartition(CryptoTickHourlyMixin, KrakenMixin, CryptoTickREST):
    pass


class KrakenSpotDailyPartition(CryptoTickDailyMixin, KrakenMixin, CryptoTickREST):
    pass
