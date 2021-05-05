import datetime
import time
from operator import eq

import pandas as pd
from google.api_core.exceptions import ServiceUnavailable

from ..bqloader import (
    MULTIPLE_SYMBOL_SCHEMA,
    SINGLE_SYMBOL_SCHEMA,
    get_schema_columns,
    get_table_id,
)
from ..fscache import FirestoreCache, firestore_data, get_collection_name
from ..s3downloader import assert_type_decimal, row_to_json
from ..utils import normalize_symbol


class FinTick:
    def __init__(
        self,
        api_symbol: str,
        period_from: str = None,
        period_to: str = None,
        futures: bool = False,
        verbose: bool = False,
    ):
        self.api_symbol = api_symbol
        self.period_from = period_from
        self.period_to = period_to
        self.futures = futures
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
        return normalize_symbol(self.api_symbol)

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


class FinTickIntegerPaginationMixin:
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


class FinTickSequentialIntegerMixin(FinTickIntegerPaginationMixin):
    """Binance, ByBit, and Coinbase REST API"""

    def assert_data_frame(self, data_frame, trades):
        # Duplicates.
        assert len(data_frame["uid"].unique()) == len(trades)
        # Missing orders.
        expected = len(trades) - 1
        diff = data_frame["index"].diff().dropna()
        assert abs(diff.sum()) == expected


class FinTickNonSequentialIntegerMixin:
    """Bitfinex, Bitflyer, and FTX REST API"""

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
