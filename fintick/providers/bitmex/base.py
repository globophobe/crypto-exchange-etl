import datetime
from decimal import Decimal

import numpy as np
import pandas as pd

from ...bqloader import MULTIPLE_SYMBOL_SCHEMA
from ...fintick import FinTickDailyS3Mixin
from .api import (
    format_bitmex_api_timestamp,
    get_active_futures,
    get_bitmex_api_timestamp,
    get_expired_futures,
    get_trades,
)
from .constants import BITMEX, S3_URL
from .lib import calculate_index


class BitmexMixin:
    @property
    def exchange(self):
        return BITMEX

    def get_uid(self, trade):
        return str(trade["trdMatchID"])

    def get_timestamp(self, trade):
        return get_bitmex_api_timestamp(trade)

    def get_nanoseconds(self, trade):
        return self.get_timestamp(trade).nanosecond

    def get_price(self, trade):
        return trade["price"]

    def get_volume(self, trade):
        foreign_notional = trade["foreignNotional"]
        if isinstance(foreign_notional, int):
            return Decimal(foreign_notional)
        return foreign_notional

    def get_notional(self, trade):
        return self.get_volume(trade) / self.get_price(trade)

    def get_tick_rule(self, trade):
        return 1 if trade["side"] == "Buy" else -1

    def get_index(self, trade):
        return np.nan  # No index, set per partition


class BitmexRESTMixin(BitmexMixin):
    def get_pagination_id(self, data=None):
        return format_bitmex_api_timestamp(self.timestamp_to)

    def iter_api(self, symbol, pagination_id, log_prefix):
        return get_trades(symbol, self.timestamp_from, pagination_id, log_prefix)

    def get_data_frame(self, trades):
        data_frame = super().get_data_frame(trades)
        # REST API is reversed
        data_frame["index"] = data_frame.index.values[::-1]
        return data_frame


class BitmexDailyS3Mixin(FinTickDailyS3Mixin, BitmexMixin):
    def get_url(self, date):
        date_string = date.strftime("%Y%m%d")
        return f"{S3_URL}{date_string}.csv.gz"

    @property
    def get_columns(self):
        return (
            "trdMatchID",
            "symbol",
            "timestamp",
            "price",
            "tickDirection",
            "foreignNotional",
        )

    def parse_dataframe(self, data_frame):
        # No false positives.
        # Source: https://pandas.pydata.org/pandas-docs/stable/user_guide/
        # indexing.html#returning-a-view-versus-a-copy
        pd.options.mode.chained_assignment = None
        # Reset index.
        data_frame = calculate_index(data_frame)
        # Timestamp
        data_frame["timestamp"] = pd.to_datetime(
            data_frame["timestamp"], format="%Y-%m-%dD%H:%M:%S.%f"
        )
        # BitMEX XBTUSD size is volume. However, quanto contracts are not
        data_frame = data_frame.rename(
            columns={"trdMatchID": "uid", "foreignNotional": "volume"}
        )
        data_frame = calculate_index(data_frame)
        return super().parse_dataframe(data_frame)


class BitmexDailyMultiSymbolS3Mixin(BitmexDailyS3Mixin):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.symbols = self.get_symbols()

    def get_symbols(self):
        active_futures = get_active_futures(
            self.symbol,
            date_from=self.period_from,
            date_to=self.period_to,
            verbose=self.verbose,
        )
        expired_futures = get_expired_futures(
            self.symbol,
            date_from=self.period_from,
            date_to=self.period_to,
            verbose=self.verbose,
        )
        return active_futures + expired_futures

    @property
    def schema(self):
        return MULTIPLE_SYMBOL_SCHEMA

    def get_suffix(self, sep="-"):
        return f"{self.symbol}USD{sep}futures"

    @property
    def log_prefix(self):
        suffix = self.get_suffix(" ")
        return f"{self.exchange_display} {suffix}"

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

    def filter_dataframe(self, data_frame):
        query = " | ".join([f'symbol == "{s["symbol"]}"' for s in self.active_symbols])
        return data_frame.query(query)
