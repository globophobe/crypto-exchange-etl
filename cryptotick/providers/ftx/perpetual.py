from ...bqloader import get_table_name
from ...utils import get_delta, publish
from .base import BaseFTXETL
from .constants import FTX


class FTXPerpetualETL(BaseFTXETL):
    def get_pagination_id(self, data=None):
        if data:
            for candle in data["candles"]:
                if candle:
                    date = candle["open"]["timestamp"].date()
                    yesterday = get_delta(date, days=-1)
                    return self.get_timestamp(yesterday)

    def assert_is_complete(self, data, trades):
        if data:
            for candle in data["candles"]:
                if candle:
                    assert candle["open"]["index"] > trades[0]["index"]
                    break

    def aggregate_trigger(self):
        table_name = get_table_name(FTX, suffix=self.get_suffix)
        data = {
            "table_name": table_name,
            "date": self.date_from.isoformat(),
        }
        publish("trade-aggregator", data)
