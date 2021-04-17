import datetime
import time
from operator import eq

import pandas as pd
import pendulum
from google.api_core.exceptions import ServiceUnavailable
from google.cloud import bigquery

from .bqloader import (
    MULTIPLE_SYMBOL_SCHEMA,
    SINGLE_SYMBOL_ORDER_BY,
    SINGLE_SYMBOL_SCHEMA,
    BigQueryDaily,
    BigQueryHourly,
    get_schema_columns,
    get_table_id,
)
from .fscache import FirestoreCache, firestore_data, get_collection_name
from .s3downloader import (
    HistoricalDownloader,
    assert_type_decimal,
    calculate_notional,
    calculate_tick_rule,
    row_to_json,
    set_dtypes,
    strip_nanoseconds,
    utc_timestamp,
)
from .utils import parse_period_from_to


class FinTick:
    def __init__(
        self,
        api_symbol,
        period_from=None,
        period_to=None,
        verbose=False,
    ):
        self.api_symbol = api_symbol
        self.period_from = period_from
        self.period_to = period_to
        self.verbose = verbose

    @property
    def schema(self):
        return SINGLE_SYMBOL_SCHEMA

    @property
    def exchange(self):
        raise NotImplementedError

    @property
    def exchange_display(self):
        return self.exchange.capitalize()

    @property
    def symbol(self):
        return self.api_symbol.replace("-", "").replace("/", "")

    def get_suffix(self, sep="_"):
        return self.symbol

    @property
    def log_prefix(self):
        return f"{self.exchange_display} {self.symbol}"

    def get_partition_decorator(self, value):
        raise NotImplementedError

    @property
    def firestore_cache(self):
        suffix = self.get_suffix(sep="-")
        collection = get_collection_name(self.exchange, suffix=suffix)
        return FirestoreCache(collection)

    def get_document_name(self, partition):
        raise NotImplementedError

    def get_last_document_name(self, partition):
        raise NotImplementedError

    def get_document(self, partition):
        document = self.get_document_name(partition)
        return self.firestore_cache.get(document)

    def get_last_document(self, partition):
        document = self.get_last_document_name(partition)
        return self.firestore_cache.get(document)

    def is_data_OK(self, data):
        if data:
            ok = data.get("ok", False)
            if ok and self.verbose:
                document = self.get_document_name(self.partition)
                print(f"{self.log_prefix}: {document} OK")
            return ok

    def get_valid_trades(self, trades, operator=eq):
        unique = set()
        valid_trades = []
        for trade in trades:
            is_unique = trade["uid"] not in unique
            is_within_partition = (
                self.timestamp_from <= trade["timestamp"] <= self.timestamp_to
            )
            if is_unique and is_within_partition:
                valid_trades.append(trade)
                unique.add(trade["uid"])
        return valid_trades

    def get_firebase_data(self, df):
        if len(df):
            open_price = df.head(1).iloc[0]
            # Can't use idxmin/idxmax with Decimal type
            low_price = df.loc[df["price"].astype(float).idxmin()]
            high_price = df.loc[df["price"].astype(float).idxmax()]
            close_price = df.tail(1).iloc[0]
            buy_side = df[df["tickRule"] == 1]
            volume = df["volume"].sum()
            buy_volume = buy_side["volume"].sum()
            notional = df["notional"].sum()
            buy_notional = buy_side["notional"].sum()
            ticks = len(df)
            buy_ticks = len(buy_side)
            return firestore_data(
                {
                    "open": row_to_json(open_price),
                    "low": row_to_json(low_price),
                    "high": row_to_json(high_price),
                    "close": row_to_json(close_price),
                    "volume": volume,
                    "buyVolume": buy_volume,
                    "notional": notional,
                    "buyNotional": buy_notional,
                    "ticks": ticks,
                    "buyTicks": buy_ticks,
                }
            )
        return {}

    def set_firebase(self, data, attr="firestore_cache", is_complete=False, retry=5):
        document = self.get_document_name(self.partition)
        # If dict, assume correct
        if isinstance(data, pd.DataFrame):
            data = self.get_firebase_data(data)
        data["ok"] = is_complete
        try:
            getattr(self, attr).set(document, data)
        except ServiceUnavailable as e:
            if retry > 0:
                time.sleep(1)
                retry -= 1
                self.set_firebase(data, attr=attr, is_complete=is_complete, retry=retry)
            else:
                raise e
        else:
            print(f"{self.log_prefix}: {document} OK")

    def get_bigquery_loader(self, table_id, partition_value):
        raise NotImplementedError

    def iter_partition(self):
        raise NotImplementedError

    def main(self):
        raise NotImplementedError

    def write(self):
        raise NotImplementedError


class FinTickREST(FinTick):
    def get_pagination_id(self, data=None):
        raise NotImplementedError

    def main(self):
        for partition in self.iter_partition():
            data = self.get_document(partition)
            if not self.is_data_OK(data):
                pagination_id = self.get_pagination_id(data)
                trade_data, is_last_iteration = self.iter_api(
                    self.api_symbol, pagination_id, self.log_prefix
                )
                trades = self.parse_data(trade_data)
                valid_trades = self.get_valid_trades(trades)
                # Are there any trades?
                if len(valid_trades):
                    data_frame = self.get_data_frame(valid_trades)
                    is_complete = self.get_is_complete(valid_trades)
                    self.write(data_frame, is_complete=is_complete)
                # No trades
                else:
                    self.set_firebase({}, is_complete=True)
                # Complete
                if is_last_iteration:
                    break

    def iter_api(self, symbol, pagination_id, log_prefix):
        raise NotImplementedError

    def parse_data(self, data):
        return [
            {
                "uid": self.get_uid(trade),
                "timestamp": self.get_timestamp(trade),
                "nanoseconds": self.get_nanoseconds(trade),
                "price": self.get_price(trade),
                "volume": self.get_volume(trade),
                "notional": self.get_notional(trade),
                "tickRule": self.get_tick_rule(trade),
                "index": self.get_index(trade),
            }
            for trade in data
        ]

    def get_uid(self, trade):
        raise NotImplementedError

    def get_timestamp(self, trade):
        raise NotImplementedError

    def get_price(self, trade):
        raise NotImplementedError

    def get_volume(self, trade):
        raise NotImplementedError

    def get_notional(self, trade):
        raise NotImplementedError

    def get_tick_rule(self, trade):
        raise NotImplementedError

    def get_index(self, trade):
        raise NotImplementedError

    def get_is_complete(self, trades):
        data = self.get_last_document(self.partition)
        return data is not None

    def assert_data_frame(self, data_frame, trades):
        # Are trades unique?
        assert len(data_frame) == len(data_frame.uid.unique())
        assert_type_decimal(data_frame, ("price", "volume", "notional"))
        # Assert timestamp, per symbol
        if self.schema == MULTIPLE_SYMBOL_SCHEMA:
            for symbol in data_frame.symbol.unique():
                df = data_frame[data_frame.symbol == symbol]
                self.assert_timestamps(df)
        else:
            self.assert_timestamps(data_frame)

    def assert_timestamps(self, data_frame):
        df = data_frame.sort_values(["timestamp", "nanoseconds", "index"])
        assert (
            self.timestamp_from
            <= df.iloc[0].timestamp
            <= df.iloc[-1].timestamp
            <= self.timestamp_to
        )

    def get_data_frame(self, trades):
        columns = get_schema_columns(self.schema)
        data_frame = pd.DataFrame(trades, columns=columns)
        self.assert_data_frame(data_frame, trades)
        return data_frame

    def write(self, data_frame, is_complete=False):
        # BigQuery
        suffix = self.get_suffix(sep="_")
        table_id = get_table_id(self.exchange, suffix=suffix)
        partition_decorator = self.get_partition_decorator(self.partition)
        bigquery_loader = self.get_bigquery_loader(table_id, partition_decorator)
        bigquery_loader.write_table(self.schema, data_frame)
        # Firebase
        data_frame = data_frame.iloc[::-1]  # Reverse data frame
        self.set_firebase(data_frame, is_complete=is_complete)


class FinTickSequentialIntegerMixin:
    """Binance, ByBit, and Coinbase REST API"""

    def get_pagination_id(self, data=None):
        pagination_id = None
        # Assert pagination_id, if not current partition
        now = datetime.datetime.utcnow()
        is_current_partition = self.partition == self.get_partition(now)
        if not is_current_partition:
            last_partition = self.get_last_partition(self.partition)
            last_document = self.get_document_name(last_partition)
            last_data = self.get_document(last_partition)
            assert last_data, f"No data for {last_document}"
            was_current_partition = last_partition == self.get_partition(now)
            if not was_current_partition:
                assert last_data["ok"] is True
            if "open" in last_data:
                pagination_id = int(last_data["open"]["index"])
            else:
                last = self.firestore_cache.get_one(order_by="open.index")
                if "open" in last:
                    pagination_id = int(last["open"]["index"])
            document = self.get_document_name(self.partition)
            assert pagination_id, f'No "pagination_id" for {document}'
        return pagination_id

    def assert_data_frame(self, data_frame, trades):
        # Duplicates.
        assert len(data_frame["uid"].unique()) == len(trades)
        # Missing orders.
        expected = len(trades) - 1
        diff = data_frame["index"].diff().dropna()
        assert abs(diff.sum()) == expected


class FinTickNonSequentialIntegerMixin:
    """Bitfinex and FTX REST API"""

    def assert_data_frame(self, data_frame, trades):
        super().assert_data_frame(data_frame, trades)
        if self.schema == MULTIPLE_SYMBOL_SCHEMA:
            for symbol in data_frame.symbol.unique():
                df = data_frame[data_frame.symbol == symbol]
                self.assert_incrementing_id(df)
        else:
            self.assert_incrementing_id(data_frame)

    def assert_incrementing_id(self, data_frame):
        # Assert incrementing ids
        diff = data_frame["index"].diff().dropna()
        assert all([value < 0 for value in diff.values])


class FinTickMultiSymbolREST(FinTickREST):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.symbols = self.get_symbols()

    @property
    def schema(self):
        return MULTIPLE_SYMBOL_SCHEMA

    @property
    def active_symbols(self):
        raise NotImplementedError

    def get_symbols(self):
        raise NotImplementedError

    def is_data_OK(self, data):
        """Firestore cache with keys for each symbol, all symbols have data."""
        if not self.active_symbols:
            return True
        else:
            return super().is_data_OK(data)

    def main(self):
        for partition in self.iter_partition():
            data = self.get_document(partition)
            if not self.is_data_OK(data):
                trades = []
                is_last_iteration = []
                # Iterate symbols
                for active_symbol in self.active_symbols:
                    pagination_id = self.get_pagination_id()
                    symbol = active_symbol["symbol"]
                    log_prefix = f"{self.exchange_display} {symbol}"
                    t, is_last = self.iter_api(
                        active_symbol["api_symbol"], pagination_id, log_prefix
                    )
                    trades += self.parse_data(
                        t, active_symbol["symbol"], active_symbol["expiry"]
                    )
                    is_last_iteration.append(is_last)
                valid_trades = self.get_valid_trades(trades)
                # Are there any trades?
                if len(valid_trades):
                    data_frame = self.get_data_frame(valid_trades)
                    is_complete = self.get_is_complete(valid_trades)
                    self.write(data_frame, is_complete=is_complete)
                # No trades
                else:
                    self.set_firebase({}, is_complete=True)
                # Complete
                if all(is_last_iteration):
                    break

    def parse_data(self, data, symbol, expiry):
        return [
            {
                "uid": self.get_uid(trade),
                "symbol": symbol,
                "expiry": expiry,
                "timestamp": self.get_timestamp(trade),
                "nanoseconds": self.get_nanoseconds(trade),
                "price": self.get_price(trade),
                "volume": self.get_volume(trade),
                "notional": self.get_notional(trade),
                "tickRule": self.get_tick_rule(trade),
                "index": self.get_index(trade),
            }
            for trade in data
        ]

    def get_firebase_data(self, data_frame):
        data = {}
        for symbol in data_frame.symbol.unique():
            df = data_frame[data_frame.symbol == symbol]
            data[symbol] = super().get_firebase_data(df)
            for s in self.symbols:
                if s["symbol"] == symbol:
                    for key in ("api_symbol", "expiry"):
                        data[symbol][key] = s[key]
                    if "listing" in s:
                        data[symbol]["listing"] = s["listing"]
        return data


class FinTickHourlyMixin:
    def get_suffix(self, sep="_"):
        return f"{self.symbol}{sep}hot"

    def get_document_name(self, timestamp):
        return timestamp.strftime("%Y-%m-%dT%H")  # Date, plus hour

    def get_last_document_name(self, timestamp):
        last_partition = self.get_last_partition(timestamp)
        return self.get_document_name(last_partition)

    def get_partition(self, timestamp):
        return timestamp.replace(
            minute=0, second=0, microsecond=0, tzinfo=datetime.timezone.utc
        )

    def get_last_partition(self, timestamp):
        timestamp += pd.Timedelta("1h")
        return timestamp

    def get_partition_decorator(self, timestamp):
        return timestamp.strftime("%Y%m%d%H")  # Partition by hour

    def get_bigquery_loader(self, table_id, partition_decorator):
        return BigQueryHourly(table_id, partition_decorator)

    @property
    def partition_iterator(self):
        period = pendulum.period(self.period_to, self.period_from)  # Reverse order
        return period.range("hours")

    def iter_partition(self):
        for partition in self.partition_iterator:
            self.timestamp_from = partition
            self.timestamp_to = (
                partition + pd.Timedelta("1 hour") - datetime.timedelta(microseconds=1)
            )
            self.partition = partition
            yield partition


class FinTickDailyMixin:
    def get_document_name(self, date):
        return date.isoformat()  # Date

    def get_last_document_name(self, date):
        last_partition = self.get_last_partition(date)
        return self.get_document_name(last_partition)

    def get_last_partition(self, date):
        date += pd.Timedelta("1d")
        return date

    def get_partition(self, timestamp):
        return timestamp.date()

    def get_partition_decorator(self, date):
        return date.strftime("%Y%m%d")  # Partition by date

    def get_bigquery_loader(self, table_id, partition_decorator):
        return BigQueryDaily(table_id, partition_decorator)

    @property
    def partition_iterator(self):
        period = pendulum.period(self.period_to, self.period_from)  # Reverse order
        return period.range("days")

    def iter_partition(self):
        for partition in self.partition_iterator:
            self.timestamp_from = datetime.datetime.combine(
                partition, datetime.datetime.min.time()
            ).replace(microsecond=0, tzinfo=datetime.timezone.utc)
            self.timestamp_to = datetime.datetime.combine(
                partition, datetime.datetime.max.time()
            ).replace(tzinfo=datetime.timezone.utc)
            self.partition = partition
            yield partition


class FinTickMultiSymbolHourlyMixin(FinTickHourlyMixin):
    @property
    def active_symbols(self):
        return [
            s
            for s in self.symbols
            if s["expiry"] - datetime.timedelta(microseconds=1) >= self.partition
        ]


class FinTickMultiSymbolDailyMixin(FinTickDailyMixin):
    @property
    def active_symbols(self):
        return [
            s
            for s in self.symbols
            if (s["expiry"] - datetime.timedelta(microseconds=1)).date()
            >= self.partition
        ]


class FinTickDailyS3Mixin(FinTickDailyMixin):
    """BitMEX and ByBit S3"""

    def get_url(self, partition):
        raise NotImplementedError

    @property
    def get_columns(self):
        raise NotImplementedError

    def main(self):
        for partition in self.iter_partition():
            document = self.get_document_name(partition)
            data = self.get_document(partition)
            if not self.is_data_OK(data):
                url = self.get_url(partition)
                if self.verbose:
                    print(f"{self.log_prefix}: downloading {document}")
                data_frame = HistoricalDownloader(url, columns=self.get_columns).main()
                if data_frame is not None:
                    df = self.filter_dataframe(data_frame)
                    if len(df):
                        df = self.parse_dataframe(df)
                        if len(df):
                            self.write(df)
                    else:
                        if self.verbose:
                            print(f"{self.log_prefix}: Done")
                        break
                else:
                    if self.verbose:
                        print(f"{self.log_prefix}: Done")
                    break

    def filter_dataframe(self, data_frame):
        if "symbol" in data_frame.columns:
            return data_frame[data_frame.symbol == self.symbol]
        return data_frame

    def parse_dataframe(self, data_frame):
        data_frame = set_dtypes(data_frame)
        data_frame = utc_timestamp(data_frame)
        data_frame = strip_nanoseconds(data_frame)
        data_frame = calculate_notional(data_frame)
        data_frame = calculate_tick_rule(data_frame)
        return data_frame

    def write(self, data_frame, is_complete=False):
        # Columns
        columns = get_schema_columns(self.schema)
        data_frame = data_frame[columns]
        # BigQuery
        suffix = self.get_suffix(sep="_")
        table_id = get_table_id(self.exchange, suffix=suffix)
        partition_decorator = self.get_partition_decorator(self.partition)
        bigquery_loader = self.get_bigquery_loader(table_id, partition_decorator)
        bigquery_loader.write_table(self.schema, data_frame)
        # Firebase
        self.set_firebase(data_frame, is_complete=True)


class FinTickDailyHourlyMixin:
    def get_hourly_document(self, timestamp):
        document_name = timestamp.strftime("%Y-%m-%dT%H")
        collection = f"{self.firestore_cache.collection}-hot"
        return FirestoreCache(collection).get(document_name)


class FinTickDailyPartitionFromHourlyMixin(FinTickDailyMixin, FinTickDailyHourlyMixin):
    def main(self):
        for partition in self.iter_partition():
            data = self.get_document(partition)
            if not self.is_data_OK(data):
                period = pendulum.period(self.timestamp_from, self.timestamp_to)
                documents = [
                    self.get_hourly_document(timestamp)
                    for timestamp in period.range("hours")
                ]
                has_data = all([document and document["ok"] for document in documents])
                if has_data:
                    data_frame = self.load_data_frame()
                    self.write(data_frame)
                    self.clean_firestore()

    def get_bigquery_loader(self, table_id, partition_decorator):
        return BigQueryDaily(table_id, partition_decorator)

    def load_data_frame(self):
        suffix = self.get_suffix(sep="_")
        table_id = get_table_id(self.exchange, suffix=f"{suffix}_hot")
        sql = f"""
            SELECT * FROM {table_id}
            WHERE {self.where_clause}
            ORDER BY {self.order_by}
        """
        partition_decorator = self.get_partition_decorator(self.partition)
        bigquery_loader = self.get_bigquery_loader(table_id, partition_decorator)
        return bigquery_loader.read_table(sql, self.job_config)

    @property
    def job_config(self):
        return bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("date", "DATE", self.partition),
            ]
        )

    @property
    def order_by(self):
        return SINGLE_SYMBOL_ORDER_BY

    @property
    def where_clause(self):
        return "date(timestamp) = @date"

    def write(self, data_frame, is_complete=True):
        # BigQuery
        suffix = self.get_suffix(sep="_")
        table_id = get_table_id(self.exchange, suffix=suffix)
        partition_decorator = self.get_partition_decorator(self.partition)
        bigquery_loader = self.get_bigquery_loader(table_id, partition_decorator)
        bigquery_loader.write_table(self.schema, data_frame)
        # Firebase
        self.set_firebase(data_frame, is_complete=is_complete)

    def clean_firestore(self):
        pass


class FinTickDailyPartitionFromHourlySequentialIntegerMixin(
    FinTickDailyPartitionFromHourlyMixin
):
    """
    Binance and Coinbase REST APIs are slow to iterate. As a result, cloud functions
    may timeout iterating for the daily partition.

    However, the trade uid of both exchanges is a sequential integer id. If complete, the
    daily partition can be copied from hourly partitions.
    """

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

    def get_is_complete(self, trades):
        now = datetime.datetime.utcnow()
        assert not self.partition == self.get_partition(now)
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

    def assert_data_frame(self, data_frame, trades):
        super().assert_data_frame(data_frame, trades)
        if len(data_frame):
            first_trade = data_frame.iloc[0]
            first_index = int(first_trade["index"])  # B/C int64
            # Last partition
            data = self.firestore_cache.get_one(
                where=["close.index", "==", first_index - 1]
            )
            # Is current partition contiguous with last partition?
            assert data["close"]["index"] == first_index - 1
            assert data["close"]["timestamp"] < first_trade["timestamp"]
