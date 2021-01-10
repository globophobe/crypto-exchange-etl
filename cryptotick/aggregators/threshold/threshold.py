import datetime
from collections import OrderedDict

import pandas as pd
from google.cloud import bigquery

from ...bqloader import BAR_SCHEMA, BigQueryLoader, get_table_id, get_table_name
from ...fscache import FirestoreCache
from .base import BaseAggregator


class ThresholdAggregator(BaseAggregator):
    def __init__(
        self,
        exchange,
        symbol,
        date,
        thresh_attr,
        thresh_value,
        date_from=None,
        date_to=None,
        verbose=False,
    ):
        super().__init__(
            exchange, symbol, date_from=date_from, date_to=date_to, verbose=verbose
        )

        self.thresh_attr = thresh_attr
        self.thresh_value = thresh_value

    def bar_type(self, sep="-"):
        parts = [self.thresh_attr, self.thresh_value]
        if self.is_adaptive:
            parts.append("adaptive")
        return f"{sep}".join(parts)

    def is_cache_valid(self, cache):
        return all([cache.get(key, None) is not None for key in self.keys])

    def is_cache_concat(self, cache):
        keys = [c for c in self.columns if c not in ("date",)]
        return all([key in cache for key in keys])

    def concat_cache(self, data_frame, cache):
        # If valid, concat.
        if self.is_cache_concat(cache):
            df = pd.DataFrame([cache], columns=self.columns)
            data_frame = pd.concat([df, data_frame])
            data_frame.reset_index(drop=True, inplace=True)
        return data_frame

    def apply_data_frame(self, data_frame):
        data_frame["ticks"] = 1
        return data_frame[self.columns]

    def get_sample(self, data_frame, start, stop=None):
        # Previous row because open price.
        s = start - 1
        sample = data_frame.loc[s:stop]
        return sample

    def aggregate_rows(self, data_frame, row, cache, start, stop=None):
        # Timestamp
        open_row = row if stop else data_frame.loc[start]
        timestamp = open_row.timestamp.replace(tzinfo=datetime.timezone.utc)
        sample = self.get_sample(data_frame, start, stop=stop)
        s = sample.iloc[1:] if start > 0 else sample
        data = {
            "date": timestamp.date(),
            "timestamp": timestamp,
            "nanoseconds": open_row.nanoseconds,
            "volume": s["volume"].sum(),
            "notional": s["notional"].sum(),
            "ticks": s["ticks"].sum(),
        }
        if stop is not None:
            data["date"] = row.date
            assert timestamp >= cache["timestamp"]
        return data

    def update_cache(self, data_frame, row, cache, start):
        # There was a remainder.
        if (start) < data_frame.shape[0]:
            data = self.aggregate_rows(data_frame, row, cache, start)
            cache.update(data)
        # There wasn't a remainder. Last row was sample.
        else:
            for key in list(cache.keys()):
                if key not in self.keys:
                    del cache[key]
        return cache

    @property
    def log_prefix(self):
        return f"{self.exchange_display} {self.symbol} bars"

    def get_destination_suffix(self, sep):
        return f"{self.symbol}{sep}bars"

    def init_cache(self):
        return {"timestamp": self.timestamp, self.thresh_attr: 0}

    def is_cache_valid(self, cache):
        return all([cache.get(key, None) is not None for key in self.keys])

    def is_cache_concatenate(self, cache):
        keys = [c for c in self.columns if c not in ("date",)]
        return all([key in cache for key in keys])

    def concatenate_cache(self, data_frame, cache):
        # If valid, concat.
        if self.is_cache_concatenate(cache):
            df = pd.DataFrame([cache], columns=self.columns)
            data_frame = pd.concat([df, data_frame])
            data_frame.reset_index(drop=True, inplace=True)
        return data_frame

    def apply_data_frame(self, data_frame):
        for column in ("open", "high", "low", "close"):
            data_frame[column] = data_frame["price"]
        data_frame["ticks"] = 1
        return data_frame[self.columns]

    def main(self):
        exchange = self.exchange.capitalize()
        date_string = self.date.isoformat()
        if self.has_data():
            firestore_cache = FirestoreCache(self.collection)
            if not firestore_cache.has_data():
                self.get_bars(firestore_cache)
            print(f"{exchange}: {date_string} {self.collection} OK")
        else:
            print(f"{exchange} {self.symbol}: {date_string} no historical data")

    def wtf(self):
        table_name = get_table_name(self.exchange, suffix="aggregated")
        table_id = get_table_id(table_name)

    def get_bars(self, firestore_cache):
        bigquery_loader = BigQueryLoader(self.table_name, self.date)
        sql, job_config = self.get_query_params()
        data_frame = bigquery_loader.read_table(sql, job_config)
        # Get cache.
        if len(data_frame):
            Bars = self.get_bar_class()
            args = (self.date, self.thresh_attr, self.thresh_value)
            kwargs = {"reversal": self.reversal} if self.reversal else {}
            bars = Bars(*args, **kwargs)
            data_frame = bars.apply_data_frame(data_frame)
            if firestore_cache.is_initial:
                cache = bars.init_cache(data_frame)
            else:
                cache = FirestoreCache(self.collection).get(self.yesterday.isoformat())
                data_frame = bars.concat_cache(data_frame, cache)
            assert bars.is_cache_valid(cache)
            df, cache = bars.aggregate(data_frame, cache)
            # Maybe write.
            if len(df):
                bigquery_loader.write_table(bars.schema, df)
            # Set cache.
            firestore_cache.set(cache)
        # Data exists. However, depending on filters, may be empty data_frame.
        else:
            firestore_cache.set({})

    def aggregate(self, data_frame, cache):
        start = 0
        samples = []
        for index, row in data_frame.itertuples():
            # Do not add cache to itself.
            if index > 0 or not self.is_cache_concatenate(cache):
                value = row[self.thresh_attr]
                value = abs(value) if self.is_absolute else value
                cache[self.thresh_attr] += value
            should_sample = cache[self.thresh_attr] >= self.thresh_value
            if should_sample:
                sample = self.aggregate_rows(data_frame, row, cache, start, stop=index)
                samples.append(sample)
                cache["timestamp"] = sample["timestamp"]
                for attr in ("volume", "notional", "ticks"):
                    cache[attr] = 0
                start = index + 1
        cache = self.update_cache(data_frame, row, cache, start)
        df = pd.DataFrame(samples, columns=self.columns)
        return df, cache

    def aggregate_rows(self, data_frame, row, cache, start, stop=None):
        # Timestamp
        open_row = row if stop else data_frame.loc[start]
        timestamp = open_row.timestamp.replace(tzinfo=datetime.timezone.utc)
        sample = self.get_sample(data_frame, start, stop=stop)
        s = sample.iloc[1:] if start > 0 else sample
        # Data
        data = {
            "date": timestamp.date(),
            "timestamp": timestamp,
            "nanoseconds": open_row.nanoseconds,
            "volume": s["volume"].sum(),
            "notional": s["notional"].sum(),
            "ticks": s["ticks"].sum(),
        }
        if stop is not None:
            data["date"] = row.date
            assert timestamp >= cache["timestamp"]
        # Sample
        sample = self.get_sample(data_frame, start, stop=stop)
        s = sample.loc[start:] if start > 0 else sample
        first_row = sample.iloc[0]
        # Is row from cache?
        if self.timestamp.date() == first_row.timestamp.date():
            # If no, close.
            open_price = first_row.close
        else:
            # Otherwise, open.
            open_price = first_row.open
        data["open"] = open_price
        data["low"] = s.low.min()
        data["high"] = s.high.max()
        data["close"] = row.close
        if stop is not None:
            return OrderedDict(
                [(key, data[key]) for key in self.columns if key not in ("symbol",)]
            )
        else:
            return data

    def get_sample(self, data_frame, start, stop=None):
        # Previous row because open price.
        s = start - 1
        sample = data_frame.loc[s:stop]
        return sample

    def update_cache(self, data_frame, row, cache, start):
        value = cache[self.thresh_attr]
        # There was a remainder.
        if (start) < data_frame.shape[0]:
            data = self.aggregate_rows(data_frame, row, cache, start)
            cache.update(data)
        # There wasn't a remainder. Last row was sample.
        else:
            for key in list(cache.keys()):
                if key not in self.keys:
                    del cache[key]
        # Likely, floating point differences.
        assert round(cache[self.thresh_attr], 2) == round(value, 2)
        return cache

    def get_bars(self, firestore_cache):
        bigquery_loader = BigQueryLoader(self.table_name, self.date)
        sql, job_config = self.get_query_params()
        data_frame = bigquery_loader.read_table(sql, job_config)
        # Get cache.
        if len(data_frame):
            Bars = self.get_bar_class()
            args = (self.date, self.thresh_attr, self.thresh_value)
            kwargs = {"reversal": self.reversal} if self.reversal else {}
            bars = Bars(*args, **kwargs)
            data_frame = bars.apply_data_frame(data_frame)
            if firestore_cache.is_initial:
                cache = bars.init_cache(data_frame)
            else:
                cache = InformationBarCache(self.collection, self.yesterday).get()
                data_frame = bars.concat_cache(data_frame, cache)
            assert bars.is_cache_valid(cache)
            df, cache = bars.aggregate(data_frame, cache)
            # Maybe write.
            if len(df):
                bigquery_loader.write_table(bars.schema, df)
            # Set cache.
            firestore_cache.set(cache)
        # Data exists. However, depending on filters, may be empty data_frame.
        else:
            firestore_cache.set({})

    def get_query_params(self):
        table_name = get_table_name(self.exchange, suffix="aggregated")
        table_id = get_table_id(table_name)
        query_parameters = (
            # Query by partition.
            bigquery.ScalarQueryParameter("date", "DATE", self.date),
            bigquery.ScalarQueryParameter("symbol", "STRING", self.symbol),
            bigquery.ScalarQueryParameter("min_volume", "INTEGER", self.min_volume),
            bigquery.ScalarQueryParameter("max_volume", "INTEGER", self.max_volume),
            bigquery.ScalarQueryParameter("min_exponent", "INTEGER", self.min_exponent),
            bigquery.ScalarQueryParameter("max_exponent", "INTEGER", self.max_exponent),
            bigquery.ScalarQueryParameter("min_notional", "FLOAT", self.min_notional),
            bigquery.ScalarQueryParameter("max_notional", "FLOAT", self.max_notional),
            bigquery.ScalarQueryParameter("tick_rule", "INTEGER", self.tick_rule),
        )
        job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
        max_exp = "=" if self.max_exponent == 0 else "<="
        where_clauses = " AND ".join(
            [
                clause[0]
                for clause in (
                    ("date = @date", self.date),
                    ("symbol = @symbol", self.symbol),
                    ("volume >= @min_volume", self.min_volume),
                    ("volume <= @max_volume", self.max_volume),
                    ("exponent >= @min_exponent", self.min_exponent),
                    (f"exponent {max_exp} @max_exponent", self.max_exponent),
                    ("notional >= @min_notional", self.min_notional),
                    ("notional <= @max_notional", self.max_notional),
                    ("tickRule = @tick_rule", self.tick_rule),
                )
                if clause[1] is not None
            ]
        )
        sql = f"""
            SELECT
            symbol, date, timestamp, nanoseconds, price, volume, notional
            FROM {table_id}
            WHERE {where_clauses}
            ORDER BY symbol, timestamp, nanoseconds, index;
        """
        return sql, job_config

    def is_cache_valid(self, cache):
        return all([cache.get(key, None) is not None for key in self.keys])

    def is_cache_concat(self, cache):
        keys = [c for c in self.columns if c not in ("date",)]
        return all([key in cache for key in keys])

    def concat_cache(self, data_frame, cache):
        # If valid, concat.
        if self.is_cache_concat(cache):
            df = pd.DataFrame([cache], columns=self.columns)
            data_frame = pd.concat([df, data_frame])
            data_frame.reset_index(drop=True, inplace=True)
        return data_frame

    def apply_data_frame(self, data_frame):
        data_frame["ticks"] = 1
        data_frame["buyVolume"] = data_frame.apply(
            lambda x: x.volume if x.tickRule == 1 else 0, axis=1
        )
        data_frame["sellVolume"] = data_frame.apply(
            lambda x: x.volume if x.tickRule == -1 else 0, axis=1
        )
        data_frame["buyNotional"] = data_frame.apply(
            lambda x: x.volume / x.price if x.tickRule == 1 else 0, axis=1
        )
        data_frame["sellNotional"] = data_frame.apply(
            lambda x: x.volume / x.price if x.tickRule == -1 else 0, axis=1
        )
        data_frame["buyTicks"] = data_frame.apply(
            lambda x: x.ticks if x.tickRule == 1 else 0, axis=1
        )
        data_frame["sellTicks"] = data_frame.apply(
            lambda x: x.ticks if x.tickRule == -1 else 0, axis=1
        )
        return data_frame[self.columns]

    def get_sample(self, data_frame, start, stop=None):
        s = start - 1
        if stop is not None:
            sample = data_frame.loc[s:stop]
        else:
            sample = data_frame.loc[s:]
        return sample

    def aggregate_rows(self, data_frame, row, cache, start, stop=None):
        # Timestamp
        timestamp = row.timestamp if stop else data_frame.loc[start].timestamp
        timestamp = timestamp.replace(tzinfo=datetime.timezone.utc)
        sample = self.get_sample(data_frame, start, stop=stop)
        s = sample.loc[1:] if start > 0 else sample
        data = {
            "timestamp": timestamp,
            "buyVolume": s["buyVolume"].sum(),
            "sellVolume": s["sellVolume"].sum(),
            "buyNotional": s["buyNotional"].sum(),
            "sellNotional": s["sellNotional"].sum(),
            "buyTicks": s["buyTicks"].sum(),
            "sellTicks": s["sellTicks"].sum(),
        }
        if stop is not None:
            data["date"] = row.date
            assert timestamp >= cache["timestamp"]
        return data

    def update_cache(self, data_frame, row, cache, start):
        # There was a remainder.
        if (start) < data_frame.shape[0]:
            data = self.aggregate_rows(data_frame, row, cache, start)
            cache.update(data)
        # There wasn't a remainder. Last row was sample.
        else:
            for key in list(cache.keys()):
                if key not in self.keys:
                    del cache[key]
        return cache
