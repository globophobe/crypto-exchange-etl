from ...fintick import FinTickDailyMixin, FinTickHourlyMixin, FinTickREST
from .base import FTXMixin


class FTXHourlyPartition(FinTickHourlyMixin, FTXMixin, FinTickREST):
    pass


class FTXDailyPartition(FinTickDailyMixin, FTXMixin, FinTickREST):
    pass
