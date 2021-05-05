from ...controllers import FinTickDailyMixin, FinTickHourlyMixin, FinTickREST
from .base import BitfinexMixin


class BitfinexPerpetualHourlyPartition(FinTickHourlyMixin, BitfinexMixin, FinTickREST):
    pass


class BitfinexPerpetualDailyPartition(FinTickDailyMixin, BitfinexMixin, FinTickREST):
    pass
