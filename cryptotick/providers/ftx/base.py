import datetime
import json

from ciso8601 import parse_datetime

from ...cryptotick import RESTExchangeETL
from ...utils import get_delta
from .constants import BTCPERP, FTX, MAX_REQUESTS_PER_SECOND, MAX_RESULTS, MIN_DATE, URL


class BaseFTXETL(RESTExchangeETL):
    def __init__(
        self, api_symbol=BTCPERP, date_from=None, date_to=None, steps=[], verbose=False
    ):
        exchange = FTX
        min_date = MIN_DATE

        self.api_symbol = api_symbol
        symbol = api_symbol.replace("-", "")

        super().__init__(
            exchange,
            symbol,
            min_date,
            date_from=date_from,
            date_to=date_to,
            steps=[],
            verbose=verbose,
        )

        self.max_requests_per_second = MAX_REQUESTS_PER_SECOND
        self.pagination_id = None

    @property
    def exchange_display(self):
        return self.exchange.upper()

    @property
    def url(self):
        url = f"{URL}/markets/{self.api_symbol}/trades?limit={MAX_RESULTS}"
        if self.pagination_id:
            url += f"&end_time={self.pagination_id}"
        return url

    def get_pagination_id(self, data=None):
        if data:
            for key in data:
                try:
                    value = data["open"]
                except KeyError:
                    try:
                        value = data[key]["open"]
                    except TypeError:
                        value = None
                finally:
                    if value:
                        date = value["timestamp"].date()
                        yesterday = get_delta(date, days=-1)
                        return self.get_timestamp(yesterday)

    def get_timestamp(self, date):
        d = datetime.datetime.combine(date, datetime.time.max)
        d = d.replace(tzinfo=datetime.timezone.utc)
        return d.timestamp()

    def parse_response(self, response):
        data = json.loads(response.content)
        if data["success"]:
            trades = self.parse_data(data["result"])
            if len(trades):
                end_time = trades[-1]["timestamp"].replace(tzinfo=datetime.timezone.utc)
                self.pagination_id = end_time.timestamp()
                return self.update(trades)

    def parse_timestamp(self, data):
        return parse_datetime(data["time"])

    def get_tick_rule(self, trade):
        return 1 if trade["side"] == "buy" else -1

    def get_index(self, trade):
        return int(trade["id"])

    def update(self, trades):
        # Maybe duplicates
        ids = [trade["index"] for trade in self.trades]
        t = [trade for trade in trades if trade["index"] not in ids]
        # Maybe more than 100 trades with same timestamp.
        if len(trades) == MAX_RESULTS and not len(t):
            pagination_id = self.pagination_id - 1e-6
            self.pagination_id = round(pagination_id, 6)
        return super().update(t)

    def write(self, trades):
        super().write(trades)
        yesterday = get_delta(self.date, days=-1)
        self.pagination_id = self.get_timestamp(yesterday)

    def assert_data_frame(self, data_frame, trades):
        # Assert incrementing ids
        diff = data_frame["index"].diff().dropna()
        assert all([value < 0 for value in diff.values])
