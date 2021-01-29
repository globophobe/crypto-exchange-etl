import datetime

from ...cryptotickdata import (
    CryptoTick,
    CryptoTickDailyHourlyMixin,
    CryptoTickDailyMixin,
    CryptoTickDailyPartitionFromHourlyMixin,
    CryptoTickHourlyMixin,
    CryptoTickREST,
)
from ...utils import parse_period_from_to
from .base import CoinbaseMixin
from .constants import BTCUSD, ETHUSD


class CoinbaseHourlyPartition(CryptoTickHourlyMixin, CoinbaseMixin, CryptoTickREST):
    pass


class CoinbaseDailyPartitionFromHourly(
    CryptoTickDailyPartitionFromHourlyMixin, CoinbaseMixin, CryptoTick
):
    def process_data_frame(self, data_frame):
        if len(data_frame):
            first_trade = data_frame.iloc[0]
            first_index = int(first_trade["index"])  # B/C int64
            data = self.firestore_cache.get_one(
                where=["close.index", "==", first_index - 1]
            )
            assert data["close"]["index"] == first_index - 1
            assert data["close"]["timestamp"] < first_trade["timestamp"]
        return data_frame


class CoinbaseDailyPartition(
    CryptoTickDailyMixin, CryptoTickDailyHourlyMixin, CoinbaseMixin, CryptoTickREST
):
    def get_pagination_id(self, data=None):
        timestamp_from, _, _, date_to = parse_period_from_to()
        # Maybe hourly
        if self.partition == date_to:
            hourly_data = self.get_hourly_document(timestamp_from)
            document = self.get_document_name(self.partition)
            assert (
                hourly_data and hourly_data["open"]
            ), f'No "pagination_id" for {document}'
            return hourly_data["open"]["index"]
        return super().get_pagination_id(data)

    def assert_data_frame(self, data_frame, trades):
        # Duplicates.
        assert len(data_frame["uid"].unique()) == len(trades)
        # Missing orders.
        expected = len(trades) - 1
        if self.api_symbol == BTCUSD:
            # There was a missing order for BTC-USD on 2019-04-11.
            if self.partition == datetime.date(2019, 4, 11):
                expected = len(trades)
        if self.api_symbol == ETHUSD:
            # There were 22 missing orders for ETH-USD on 2020-09-04.
            if self.partition == datetime.date(2020, 9, 4):
                expected = len(trades) + 21
        diff = data_frame["index"].diff().dropna()
        assert abs(diff.sum()) == expected

    def get_is_complete(self, trades):
        now = datetime.datetime.utcnow()
        is_current_partition = self.partition == self.get_partition(now)
        if not is_current_partition:
            if len(trades):
                last_index = trades[0]["index"]
                timestamp_from, _, _, date_to = parse_period_from_to()
                # Maybe hourly
                if self.partition == date_to:
                    data = self.get_hourly_document(timestamp_from)
                # Maybe no trades in last partition
                else:
                    data = self.firestore_cache.get_one(
                        where=["open.index", "==", last_index + 1]
                    )
                assert data["open"]["index"] == last_index + 1
            return True
