import datetime

import pandas as pd

from ...bqloader import MULTIPLE_SYMBOL_SCHEMA
from ...cryptotickdata import CryptoTickDailyS3Mixin
from ...s3downloader import calculate_index, calculate_notional
from .api import (
    format_bitmex_api_timestamp,
    get_active_futures,
    get_bitmex_api_timestamp,
    get_expired_futures,
    get_trades,
)
from .constants import BCHUSD, BITMEX, ETHUSD, LTCUSD, S3_URL, XBTUSD, XRPUSD, uBTC
from .lib import calc_notional


class BitmexMixin:
    @property
    def exchange(self):
        return BITMEX

    def get_uid(self, trade):
        return str(trade["trdMatchID"])

    def get_timestamp(self, trade):
        return get_bitmex_api_timestamp(trade)

    def get_nanoseconds(self, trade):
        return 0  # No nanoseconds

    def get_price(self, trade):
        return float(trade["price"])

    def get_volume(self, trade):
        return float(trade["size"])

    def get_notional(self, trade):
        volume = self.get_volume(trade)
        price = self.get_price(trade)
        if self.symbol == XBTUSD:
            return volume / price
        elif self.symbol.startswith(ETHUSD) or self.symbol.startswith(BCHUSD):
            return volume * price * uBTC
        elif self.symbol.startswith(LTCUSD):
            return volume * price * uBTC * 2
        elif self.symbol == XRPUSD:
            return volume * price * uBTC / 20
        else:
            raise NotImplementedError

    def get_tick_rule(self, trade):
        return 1 if trade["side"] == "Buy" else -1

    def get_index(self, trade):
        return 0  # No index


class BitmexRESTMixin(BitmexMixin):
    def get_pagination_id(self, data=None):
        return format_bitmex_api_timestamp(self.timestamp_to)

    def iter_api(self, symbol, pagination_id, log_prefix):
        return get_trades(symbol, self.timestamp_from, pagination_id, log_prefix)


class BitmexDailyS3Mixin(CryptoTickDailyS3Mixin, BitmexMixin):
    def get_url(self, date):
        date_string = date.strftime("%Y%m%d")
        return f"{S3_URL}{date_string}.csv.gz"

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
        data_frame = super().parse_dataframe(data_frame)
        # Notional after other transforms.
        data_frame = calculate_notional(data_frame, calc_notional)
        return data_frame


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
