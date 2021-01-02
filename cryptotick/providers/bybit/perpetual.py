import datetime

import httpx
import pandas as pd

from ...bqloader import get_table_name
from ...cryptotick import S3CryptoExchangeETL
from ...s3downloader import calculate_notional
from ...utils import publish
from .constants import BYBIT, URL


def calc_notional(x):
    return x["volume"] / x["price"]


class BybitPerpetualETL(S3CryptoExchangeETL):
    def __init__(
        self,
        symbol,
        date_from=None,
        date_to=None,
        aggregate=False,
        post_aggregation=[],
        verbose=False,
    ):
        exchange = BYBIT
        min_date = datetime.date(2019, 10, 1)
        super().__init__(
            exchange,
            symbol,
            min_date,
            date_from=date_from,
            date_to=date_to,
            aggregate=aggregate,
            post_aggregation=post_aggregation,
            verbose=verbose,
        )

    def get_url(self, date):
        directory = f"{URL}{self.symbol}/"
        response = httpx.get(directory)
        if response.status_code == 200:
            return f"{URL}{self.symbol}/{self.symbol}{date.isoformat()}.csv.gz"
        else:
            print(f"{self.exchange.capitalize()} {self.symbol}: No data")

    def parse_dataframe(self, data_frame):
        # No false positives.
        # Source: https://pandas.pydata.org/pandas-docs/stable/user_guide/
        # indexing.html#returning-a-view-versus-a-copy
        pd.options.mode.chained_assignment = None
        # Bybit is reversed.
        data_frame = data_frame.iloc[::-1]
        data_frame["index"] = data_frame.index.values[::-1]
        data_frame["timestamp"] = pd.to_datetime(data_frame["timestamp"], unit="s")
        data_frame = super().parse_dataframe(data_frame)
        data_frame = calculate_notional(data_frame, calc_notional)
        return data_frame

    def calculate_notional(self, data_frame):
        return calculate_notional(data_frame, calc_notional)

    def aggregate_trigger(self):
        table_name = get_table_name(BYBIT, suffix=self.symbol)
        data = {
            "table_name": table_name,
            "date": self.date_from,
            "post_aggregation": self.post_aggregation,
        }
        publish("trade-aggregator", data)
