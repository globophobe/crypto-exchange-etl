from ...controllers import (
    FinTick,
    FinTickDailyMixin,
    FinTickDailyPartitionFromHourlyMixin,
    FinTickDailySequentialIntegerMixin,
    FinTickHourlyMixin,
    FinTickREST,
)
from .base import BinanceMixin


class BinancePerpetualHourlyPartition(FinTickHourlyMixin, BinanceMixin, FinTickREST):
    pass


class BinanceDailyPartitionFromHourly(
    FinTickDailyPartitionFromHourlyMixin, BinanceMixin, FinTick
):
    pass


class BinancePerpetualDailyPartition(
    FinTickDailySequentialIntegerMixin,
    FinTickDailyMixin,
    BinanceMixin,
    FinTickREST,
):
    pass
