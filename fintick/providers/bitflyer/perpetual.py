from ...controllers import (
    FinTick,
    FinTickDailyMixin,
    FinTickDailyPartitionFromHourlyMixin,
    FinTickDailySequentialIntegerPaginationMixin,
    FinTickHourlyMixin,
    FinTickREST,
)
from .base import BitflyerMixin


class BitflyerHourlyPartition(FinTickHourlyMixin, BitflyerMixin, FinTickREST):
    pass


class BitflyerDailyPartitionFromHourly(
    FinTickDailyPartitionFromHourlyMixin, BitflyerMixin, FinTick
):
    pass


class BitflyerDailyPartition(
    FinTickDailySequentialIntegerPaginationMixin,
    FinTickDailyMixin,
    BitflyerMixin,
    FinTickREST,
):
    pass
