import httpx

from ...utils import date_range, publish
from .perpetual import BybitPerpetualETL


class BybitPerpetualETLTrigger(BybitPerpetualETL):
    def main(self):
        for date in date_range(self.date_from, self.date_to, reverse=True):
            if not self.has_data(date):
                url = self.get_url(date)
                response = httpx.head(url)
                if response.status_code == 200:
                    data = {
                        "symbol": self.symbol,
                        "date": self.date.isoformat(),
                        "aggregate": self.aggregate,
                        "post_aggregation": self.post_aggregation,
                    }
                    publish("bybit-perpetual", data)
