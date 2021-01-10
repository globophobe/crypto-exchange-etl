import httpx

from ...utils import date_range, publish
from .futures import BitmexFuturesETL
from .perpetual import BitmexPerpetualETL


class BitmexPerpetualETLTrigger(BitmexPerpetualETL):
    def main(self):
        for date in date_range(self.date_from, self.date_to, reverse=True):
            if not self.has_data(date):
                url = self.get_url(date)
                response = httpx.head(url)
                if response.status_code == 200:
                    data = {
                        "symbols": " ".join(self.symbols),
                        "date": self.date.isoformat(),
                        "aggregate": self.aggregate,
                    }
                    publish("bitmex-perpetual", data)


class BitmexFuturesETLTrigger(BitmexFuturesETL):
    def main(self):
        for date in date_range(self.date_from, self.date_to, reverse=True):
            if not self.has_data(date):
                url = self.get_url(date)
                response = httpx.head(url)
                if response.status_code == 200:
                    data = {
                        "root_symbol": self.root_symbol,
                        "date": self.date.isoformat(),
                        "aggregate": self.aggregate,
                    }
                    publish("bitmex-futures", data)
