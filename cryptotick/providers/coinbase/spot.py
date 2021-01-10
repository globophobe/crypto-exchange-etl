import datetime
import json

from ciso8601 import parse_datetime

from ...bqloader import get_table_name
from ...cryptotick import RESTExchangeETL
from ...utils import publish
from .constants import (
    BTCUSD,
    COINBASE,
    ETHUSD,
    MAX_REQUESTS_PER_SECOND,
    MAX_RESULTS,
    MIN_DATE,
    URL,
)


class CoinbaseSpotETL(RESTExchangeETL):
    def __init__(
        self,
        api_symbol=BTCUSD,
        date_from=None,
        date_to=None,
        aggregate=False,
        verbose=False,
    ):
        exchange = COINBASE
        min_date = MIN_DATE

        self.api_symbol = api_symbol
        symbol = api_symbol.replace("-", "")

        super().__init__(
            exchange,
            symbol,
            min_date,
            date_from=date_from,
            date_to=date_to,
            aggregate=aggregate,
            verbose=verbose,
        )

        self.max_requests_per_second = MAX_REQUESTS_PER_SECOND
        self.pagination_id = None

    @property
    def url(self):
        url = f"{URL}/products/{self.api_symbol}/trades"
        if self.pagination_id:
            return f"{url}?after={self.pagination_id}"
        return url

    @property
    def can_paginate(self):
        return self.pagination_id is None or int(self.pagination_id) >= MAX_RESULTS

    def get_pagination_id(self, data):
        if data:
            for candle in data["candles"]:
                if candle:
                    return candle["open"]["index"]

    def parse_response(self, response):
        """
        Pagination details: https://docs.pro.coinbase.com/#pagination
        """
        # Coinbase says cursor pagination can be unintuitive at first.
        # After gets data older than cb-after pagination id.
        data = json.loads(response.content)
        trades = self.parse_data(data)
        # Update pagination_id
        self.pagination_id = response.headers.get("cb-after", None)
        return self.update(trades)

    def parse_timestamp(self, trade):
        return parse_datetime(trade["time"])

    def get_tick_rule(self, trade):
        # Buy side indicates a down-tick because the maker was a buy order and
        # their order was removed. Conversely, sell side indicates an up-tick.
        return 1 if trade["side"] == "sell" else -1

    def get_index(self, trade):
        return int(trade["trade_id"])

    def write(self, trades):
        super().write(trades)
        self.pagination_id = trades[-1]["index"]

    def assert_data_frame(self, data_frame, trades):
        # Duplicates.
        assert len(data_frame["index"].unique()) == len(trades)
        # Missing orders.
        expected = len(trades) - 1
        if self.api_symbol == BTCUSD:
            # There was a missing order for BTC-USD on 2019-04-11.
            if self.date == datetime.date(2019, 4, 11):
                expected = len(trades)
        if self.api_symbol == ETHUSD:
            # There were 22 missing orders for ETH-USD on 2020-09-04.
            if self.date == datetime.date(2020, 9, 4):
                expected = len(trades) + 21
        diff = data_frame["index"].diff().dropna()
        assert abs(diff.sum()) == expected

    def assert_is_complete(self, data, trades):
        assert self.get_pagination_id(data) == trades[0]["index"] + 1

    def aggregate_trigger(self):
        table_name = get_table_name(COINBASE, suffix=self.symbol)
        data = {"table_name": table_name, "date": self.date_from.isoformat()}
        publish("trade-aggregator", data)
