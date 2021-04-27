from ...fintick import FinTickHourlyMixin, FinTickREST
from .base import BitflyerMixin


class BitflyerHourlyPartition(FinTickHourlyMixin, BitflyerMixin, FinTickREST):
    pass


class BitflyerDailyPartition(BitflyerMixin, FinTickREST):
    pass
