import datetime
import time

import httpx
import pandas as pd
from ciso8601 import parse_datetime
from google.api_core.exceptions import ServiceUnavailable

from .bqloader import (
    SINGLE_SYMBOL_SCHEMA,
    BigQueryLoader,
    get_schema_columns,
    get_table_name,
)
from .fscache import FirestoreCache, firestore_data, get_collection_name
from .s3downloader import (
    HistoricalDownloader,
    calculate_tick_rule,
    row_to_json,
    set_columns,
    set_types,
    strip_nanoseconds,
    utc_timestamp,
)
from .utils import date_range, get_delta


class CryptoExchangeETL:
    def __init__(
        self,
        exchange,
        symbol,
        min_date,
        date_from=None,
        date_to=None,
        aggregate=False,
        schema=SINGLE_SYMBOL_SCHEMA,
        verbose=False,
    ):
        self.exchange = exchange
        self.symbol = symbol
        self.schema = schema
        self.aggregate = aggregate
        self.verbose = verbose

        self.initialize_dates(min_date, date_from, date_to)

    def initialize_dates(self, min_date, date_from, date_to):
        today = datetime.datetime.utcnow().date()

        if date_from:
            self.date_from = parse_datetime(date_from).date()
        else:
            self.date_from = min_date

        if date_to:
            self.date_to = parse_datetime(date_to).date()
        else:
            self.date_to = today

        assert self.date_from <= self.date_to <= today

        # State.
        self.date = self.date_to

    @property
    def exchange_display(self):
        return self.exchange.capitalize()

    def get_suffix(self, sep="_"):
        return self.symbol

    @property
    def log_prefix(self):
        return f"{self.exchange_display} {self.symbol}"

    @property
    def firestore_cache(self):
        suffix = self.get_suffix()
        collection = get_collection_name(self.exchange, suffix=suffix)
        return FirestoreCache(collection)

    def has_data(self, date):
        document = date.isoformat()
        if self.firestore_cache.has_data(document):
            if self.verbose:
                print(f"{self.log_prefix}: {document} OK")
            return True

    def iter_hours(self, step=1):
        hour = datetime.datetime.combine(self.date, datetime.datetime.min.time())
        steps = 24 / step
        assert steps % 1 == 0
        for i in range(int(steps)):
            next_hour = hour + datetime.timedelta(hours=1)
            yield hour.replace(tzinfo=datetime.timezone.utc), next_hour.replace(
                tzinfo=datetime.timezone.utc
            )
            hour = next_hour

    def get_firebase_data(self, data_frame):
        data = {"candles": []}
        for hour, next_hour in self.iter_hours():
            df = data_frame[
                (data_frame["timestamp"] >= hour)
                & (data_frame["timestamp"] < next_hour)
            ]
            if len(df):
                open_price = df.head(1).iloc[0]
                low_price = df.loc[df["price"].idxmin()]
                high_price = df.loc[df["price"].idxmax()]
                close_price = df.tail(1).iloc[0]
                buy_side = df[df["tickRule"] == 1]
                volume = float(df["volume"].sum())
                buy_volume = float(buy_side["volume"].sum())
                notional = float(df["notional"].sum())
                buy_notional = float(buy_side["notional"].sum())
                ticks = len(df)
                buy_ticks = len(buy_side)
                candle = {
                    "open": firestore_data(row_to_json(open_price)),
                    "low": firestore_data(row_to_json(low_price)),
                    "high": firestore_data(row_to_json(high_price)),
                    "close": firestore_data(row_to_json(close_price)),
                    "volume": volume,
                    "buyVolume": buy_volume,
                    "notional": notional,
                    "buyNotional": buy_notional,
                    "ticks": ticks,
                    "buyTicks": buy_ticks,
                }
            # Maybe no trades.
            else:
                candle = {}
            data["candles"].append(candle)
        return data

    def set_firebase(self, data, attr="firestore_cache", is_complete=False, retry=5):
        document = self.date.isoformat()
        # If dict, assume correct
        if isinstance(data, pd.DataFrame):
            data = self.get_firebase_data(data)
        data["ok"] = is_complete
        # Retry n times
        r = retry - 1
        try:
            getattr(self, attr).set(document, data)
        except ServiceUnavailable as exception:
            if r == 0:
                raise exception
            else:
                time.sleep(1)
                self.set_firebase(data, attr=attr, is_complete=is_complete, retry=r)
        else:
            print(f"{self.log_prefix}: {self.date.isoformat()} OK")

    def get_response(self, retry=5):
        e = None
        # Retry n times.
        for i in range(retry):
            try:
                return httpx.get(self.url)
            except Exception as exception:
                e = exception
                time.sleep(i + 1)
        raise e


class RESTExchangeETL(CryptoExchangeETL):
    def __init__(
        self,
        exchange,
        symbol,
        min_date,
        date_from=None,
        date_to=None,
        schema=SINGLE_SYMBOL_SCHEMA,
        aggregate=False,
        verbose=False,
    ):
        super().__init__(
            exchange,
            symbol,
            min_date,
            date_from=date_from,
            date_to=date_to,
            schema=schema,
            aggregate=aggregate,
            verbose=verbose,
        )
        self.pagination_id = None
        self.trades = []

    @property
    def url(self):
        raise NotImplementedError

    @property
    def can_paginate(self):
        return True

    def get_pagination_id(self, data):
        raise NotImplementedError

    def set_pagination_id(self, data):
        raise NotImplementedError

    def has_data(self, date):
        document = date.isoformat()
        data = self.firestore_cache.get(document)
        if data:
            today = datetime.datetime.utcnow().date()
            # Today's data will not be complete.
            if date == today:
                ok = True
            # Previous dates should be complete.
            else:
                ok = data.get("ok", False)
            if ok:
                # Pagination
                self.pagination_id = self.get_pagination_id(data)
                if self.verbose:
                    print(f"{self.log_prefix}: {document} OK")
            return ok

    def main(self):
        for date in date_range(self.date_from, self.date_to, reverse=True):
            self.date = date
            self.trades = [t for t in self.trades if t["date"] == date]
            if not self.has_data(date) and self.can_paginate:
                stop_execution = False
                while not stop_execution:
                    start_time = time.time()
                    for i in range(self.max_requests_per_second):
                        stop_execution = self.get_data()
                        if stop_execution:
                            break
                    if not stop_execution:
                        elapsed = time.time() - start_time
                        if elapsed < 1:
                            diff = 1 - elapsed
                            time.sleep(diff)

        if self.aggregate:
            self.aggregate_trigger()

        print(
            f"{self.log_prefix}: {self.date_from.isoformat()} to "
            f"{self.date_to.isoformat()} OK"
        )

    def aggregate_trigger(self):
        # Intended for GCP
        raise NotImplementedError

    def get_data(self):
        response = self.get_response()
        if response:
            if response.status_code == 200:
                return self.parse_response(response)
            else:
                raise Exception(f"{response.status_code}: {response.content}")
        else:
            raise Exception(f"{self.log_prefix}: No response")

    def get_response(self, retry=5):
        e = None
        # Retry n times.
        for i in range(retry):
            try:
                return httpx.get(self.url)
            except Exception as exception:
                e = exception
                time.sleep(i + 1)
        raise e

    def parse_response(self, response):
        raise NotImplementedError

    def parse_data(self, data):
        trades = []
        for d in data:
            timestamp = self.parse_timestamp(d)
            date = timestamp.date()
            trade = {
                "date": date,
                "timestamp": timestamp,
                "nanoseconds": 0,  # No nanoseconds.
                "price": self.get_price(d),
                "volume": self.get_volume(d),
                "notional": self.get_notional(d),
                "tickRule": self.get_tick_rule(d),
                "index": self.get_index(d),
            }
            trades.append(trade)
        return trades

    def parse_timestamp(self, data):
        raise NotImplementedError

    def get_price(self, trade):
        return float(trade["price"])

    def get_volume(self, trade):
        return float(trade["price"]) * float(trade["size"])

    def get_notional(self, trade):
        return float(trade["size"])

    def get_tick_rule(self, trade):
        raise NotImplementedError

    def get_index(self, trade):
        raise NotImplementedError

    def update(self, trades):
        if trades:
            start = trades[-1]
            date = start["date"]
            index = start["index"]
            # Verbose
            if self.verbose:
                timestamp = start["timestamp"].replace(tzinfo=None).isoformat()
                if not start["timestamp"].microsecond:
                    timestamp += ".000000"
                print(f"{self.log_prefix}: {timestamp} {index}")
            if self.date == date:
                self.trades += [t for t in trades if t["date"] == self.date]
            else:
                # No days without trades.
                assert len(pd.date_range(start=date, end=self.date)) == 2
                data = self.trades + [t for t in trades if t["date"] == self.date]
                if len(data):
                    self.write(data)
                return True

    def write(self, trades):
        start = trades[-1]
        stop = trades[0]
        assert start["date"] == stop["date"]
        # Dataframe
        columns = get_schema_columns(self.schema)
        data_frame = pd.DataFrame(trades, columns=columns)
        data_frame = set_types(data_frame)
        self.assert_data_frame(data_frame, trades)
        # Previous.
        document = get_delta(self.date, days=1).isoformat()
        data = self.firestore_cache.get(document)
        is_complete = data is not None
        # Assert last trade of the day
        if is_complete:
            self.assert_is_complete(data, trades)
        # BigQuery
        suffix = self.get_suffix(sep="_")
        table_name = get_table_name(self.exchange, suffix=suffix)
        bigquery_loader = BigQueryLoader(table_name, self.date)
        bigquery_loader.write_table(self.schema, data_frame)
        # Firebase
        data_frame = data_frame.iloc[::-1]  # Reverse data frame
        self.set_firebase(data_frame, is_complete=is_complete)

    def assert_data_frame(self, data_frame, trades):
        pass

    def assert_is_complete(self, data, trades):
        pass


class S3CryptoExchangeETL(CryptoExchangeETL):
    def get_url(self, date):
        raise NotImplementedError

    def main(self):
        for date in date_range(self.date_from, self.date_to, reverse=True):
            date_display = date.isoformat()
            if not self.has_data(date):
                url = self.get_url(date)
                if self.verbose:
                    print(f"{self.log_prefix}: downloading {date_display}")
                data_frame = HistoricalDownloader(url).main()
                if data_frame is not None:
                    self.process_dataframe(data_frame)
            # Next
            self.date = get_delta(date, days=-1)

        if self.aggregate:
            self.aggregate_trigger()

        print(
            f"{self.log_prefix}: {self.date_from.isoformat()} to "
            f"{self.date_to.isoformat()} OK"
        )

    def aggregate_trigger(self):
        # Intended for GCP
        raise NotImplementedError

    def process_dataframe(self, data_frame):
        data_frame = self.parse_dataframe(data_frame)
        if len(data_frame):
            self.write(data_frame)
        else:
            print(f"{self.log_prefix}: No data")

    def parse_dataframe(self, data_frame):
        # Transforms
        data_frame = utc_timestamp(data_frame)
        data_frame = strip_nanoseconds(data_frame)
        data_frame = set_columns(data_frame)
        data_frame = calculate_tick_rule(data_frame)
        return data_frame

    def write(self, data_frame):
        # Types
        data_frame = set_types(data_frame)
        # Columns
        columns = get_schema_columns(self.schema)
        data_frame = data_frame[columns]
        # BigQuery
        suffix = self.get_suffix(sep="_")
        table_name = get_table_name(self.exchange, suffix=suffix)
        bigquery_loader = BigQueryLoader(table_name, self.date)
        bigquery_loader.write_table(self.schema, data_frame)
        # Firebase
        self.set_firebase(data_frame, is_complete=True)


class FuturesETL(CryptoExchangeETL):
    def get_symbols(self, root_symbol):
        raise NotImplementedError

    def get_suffix(self, sep="-"):
        raise NotImplementedError

    @property
    def log_prefix(self):
        suffix = self.get_suffix(" ")
        return f"{self.exchange_display} {suffix}"

    @property
    def active_symbols(self):
        return [
            s
            for s in self.symbols
            if s["listing"].date() <= self.date <= s["expiry"].date()
        ]

    def has_symbols(self, data):
        return all([data.get(s["symbol"], None) for s in self.active_symbols])

    def get_symbol_data(self, symbol):
        return [s for s in self.symbols if s["symbol"] == symbol][0]

    def has_data(self, date):
        """Firestore cache with keys for each symbol, all symbols have data."""
        document = date.isoformat()
        if not self.active_symbols:
            print(f"{self.log_prefix}: No data")
            return True
        else:
            data = self.firestore_cache.get(document)
            if data:
                ok = data.get("ok", False)
                if ok and self.has_symbols(data):
                    print(f"{self.log_prefix}: {document} OK")
                    return True

    def get_firebase_data(self, data_frame):
        data = {}
        for s in self.active_symbols:
            symbol = s["symbol"]
            # API data
            d = self.get_symbol_data(symbol)
            # Dataframe
            df = data_frame[data_frame["symbol"] == symbol]
            # Maybe symbol
            if len(df):
                data[symbol] = super().get_firebase_data(df)
                data[symbol]["listing"] = d["listing"].replace(
                    tzinfo=datetime.timezone.utc
                )
                data[symbol]["expiry"] = d["expiry"].replace(
                    tzinfo=datetime.timezone.utc
                )
                # for key in ("listing", "expiry"):
                #     value = data[symbol][key]
                #     if not hasattr(value, "_nanosecond"):
                #         setattr(value, "_nanosecond", 0)
            else:
                df[symbol] = {}
        return data
