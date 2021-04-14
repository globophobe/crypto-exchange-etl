import datetime

import pandas as pd
import pendulum
from firebase_admin import firestore
from google.cloud import bigquery

from ..bqloader import (
    MULTIPLE_SYMBOL_AGGREGATE_SCHEMA,
    MULTIPLE_SYMBOL_BAR_SCHEMA,
    MULTIPLE_SYMBOL_ORDER_BY,
    SINGLE_SYMBOL_AGGREGATE_SCHEMA,
    SINGLE_SYMBOL_BAR_SCHEMA,
    SINGLE_SYMBOL_ORDER_BY,
    get_schema_columns,
    stringify_datetime_types,
)
from ..fintick import FinTick, FinTickDailyMixin, FinTickHourlyMixin
from ..fscache import FirestoreCache, firestore_data
from .lib import get_timestamp_from_to


class BaseAggregator(FinTick):
    def __init__(
        self,
        source_table,
        destination_table,
        symbol=None,
        period_from=None,
        period_to=None,
        has_multiple_symbols=False,
        verbose=False,
    ):
        self.source_table = source_table
        self.destination_table = destination_table
        self.has_multiple_symbols = has_multiple_symbols
        self.period_from = self.get_period_from(period_from)
        self.period_to = self.get_period_to(period_to)
        self.verbose = verbose

    def get_source(self, sep="_"):
        _, table_name = self.source_table.split(".")
        return table_name.replace("_", sep)

    def get_destination(self, sep="_"):
        _, table_name = self.destination_table.split(".")
        return table_name.replace("_", sep)

    @property
    def log_prefix(self):
        name = self.get_destination(sep=" ")
        return name[0].capitalize() + name[1:]  # Cap 1st char

    @property
    def source_schema(self):
        raise NotImplementedError

    @property
    def schema(self):
        raise NotImplementedError

    @property
    def fields(self):
        return ", ".join(
            [
                field
                for field in get_schema_columns(self.source_schema)
                if field not in ("uid", "index")
            ]
        )

    @property
    def order_by(self):
        if self.has_multiple_symbols:
            return MULTIPLE_SYMBOL_ORDER_BY
        else:
            return SINGLE_SYMBOL_ORDER_BY

    @property
    def columns(self):
        return get_schema_columns(self.schema)

    @property
    def firestore_source(self):
        collection = self.get_source(sep="-")
        return FirestoreCache(collection)

    @property
    def firestore_destination(self):
        collection = self.get_destination(sep="-")
        return FirestoreCache(collection)

    def get_period(self, timestamp):
        raise NotImplementedError

    def get_period_from(self, period_from):
        timestamp = self.get_min_timestamp()
        if timestamp:
            p = self.get_period(timestamp)
            if p > period_from:
                return p
            return period_from

    def get_min_timestamp(self):
        data = self.firestore_source.get_one(order_by="open.timestamp")
        timestamps = self.get_timestamps_from_data(data, "open")
        if len(timestamps):
            return min(timestamps)

    def get_period_to(self, period_to):
        data = self.firestore_source.get_one(
            order_by="close.timestamp", direction=firestore.Query.DESCENDING
        )
        timestamps = self.get_timestamps_from_data(data, "close")
        if len(timestamps):
            timestamp = max(timestamps)
            p = self.get_period(timestamp)
            if p < period_to:
                return p
            return period_to

    def get_timestamps_from_data(self, data, attr):
        timestamps = []
        if data:
            if self.has_multiple_symbols:
                for key, symbol in data.items():
                    if isinstance(symbol, dict):
                        timestamp = symbol[attr]["timestamp"]
                        timestamps.append(timestamp)
                        break
            else:
                timestamps.append(data[attr]["timestamp"])
        return timestamps

    def main(self):
        if self.period_from and self.period_to:
            for partition in self.iter_partition():
                self.partition_decorator = self.get_partition_decorator(partition)
                document = self.get_document_name(partition)
                if self.firestore_source.has_data(document):
                    if not self.firestore_destination.has_data(document):
                        data_frame = self.get_data_frame()
                        # Are there any trades?
                        if len(data_frame):
                            df = self.process_data_frame(data_frame)
                            self.write(df)
                        # No trades
                        else:
                            self.set_firebase(
                                {}, attr="firestore_destination", is_complete=True
                            )
                    elif self.verbose:
                        print(f"{self.log_prefix}: {document} OK")
                else:
                    print(f"{self.log_prefix}: {document} No data")
        else:
            print(f"{self.log_prefix}: No data")

    def get_data_frame(self):
        sql = f"""
            SELECT {self.fields}
            FROM {self.source_table}
            WHERE {self.where_clause}
            ORDER BY {self.order_by};
        """
        bigquery_loader = self.get_bigquery_loader(
            self.source_table, self.partition_decorator
        )
        return bigquery_loader.read_table(sql, self.job_config)

    @property
    def job_config(self):
        raise NotImplementedError

    @property
    def where_clause(self):
        raise NotImplementedError

    def aggregate(self, data_frame, cache):
        raise NotImplementedError

    def write(self, data_frame):
        # BigQuery
        bigquery_loader = self.get_bigquery_loader(
            self.destination_table, self.partition_decorator
        )
        bigquery_loader.write_table(self.schema, data_frame)
        # Firebase
        self.set_firebase(data_frame, attr="firestore_destination", is_complete=True)


class HourlyAggregatorMixin(FinTickHourlyMixin):
    def get_period(self, timestamp):
        return timestamp.replace(tzinfo=datetime.timezone.utc)

    @property
    def job_config(self):
        timestamp_from, timestamp_to = get_timestamp_from_to(self.partition)
        return bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "timestamp_from", "TIMESTAMP", timestamp_from
                ),
                bigquery.ScalarQueryParameter(
                    "timestamp_to", "TIMESTAMP", timestamp_to
                ),
            ]
        )

    @property
    def where_clause(self):
        return "timestamp >= @timestamp_from and timestamp < @timestamp_to"


class DailyAggregatorMixin(FinTickDailyMixin):
    def get_period(self, timestamp):
        return timestamp.date()

    @property
    def job_config(self):
        return bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("date", "DATE", self.partition)
            ]
        )

    @property
    def where_clause(self):
        return "date(timestamp) = @date"


class BaseCacheAggregator(BaseAggregator):
    @property
    def source_schema(self):
        if self.has_multiple_symbols:
            return MULTIPLE_SYMBOL_AGGREGATE_SCHEMA
        else:
            return SINGLE_SYMBOL_AGGREGATE_SCHEMA

    @property
    def schema(self):
        if self.has_multiple_symbols:
            return MULTIPLE_SYMBOL_BAR_SCHEMA
        else:
            return SINGLE_SYMBOL_BAR_SCHEMA

    @property
    def fields(self):
        return ", ".join(
            [
                field
                for field in get_schema_columns(self.source_schema)
                if field != "uid"
            ]
        )

    @property
    def is_cache_required(self):
        raise NotImplementedError

    def get_initial_cache(self, data_frame):
        raise NotImplementedError

    def get_cache(self, data_frame):
        document = self.get_last_document_name(self.partition)
        data = self.firestore_destination.get(document)
        # Is cache required, and no data?
        if self.is_cache_required and not data:
            doc = self.get_document_name()
            raise Exception(f"Cache does not exist, {doc}")
        if not data:
            data_frame, data = self.get_initial_cache(data_frame)
        return data_frame, data

    def get_firebase_data(self, cache):
        return

    def main(self):
        if self.period_from and self.period_to:
            for partition in self.iter_partition():
                self.partition_decorator = self.get_partition_decorator(partition)
                document = self.get_document_name(partition)
                if self.firestore_source.has_data(document):
                    if not self.firestore_destination.has_data(document):
                        data_frame = self.get_data_frame()
                        data_frame, cache = self.get_cache(data_frame)
                        # Are there any trades?
                        if len(data_frame):
                            data, cache = self.process_data_frame(data_frame, cache)
                            self.write(data, cache)
                        # No trades
                        else:
                            self.set_firebase(
                                {}, attr="firestore_destination", is_complete=True
                            )
                    elif self.verbose:
                        print(f"{self.log_prefix}: {document} OK")
                else:
                    print(f"{self.log_prefix}: {document} No data")
        else:
            print(f"{self.log_prefix}: No data")

    def write(self, data, cache):
        # JSON data
        for index, d in enumerate(data):
            data[index] = stringify_datetime_types(firestore_data(d, strip_date=False))
            data[index]["topN"] = [
                stringify_datetime_types(firestore_data(t)) for t in d["topN"]
            ]
        # BigQuery
        partition_decorator = self.get_partition_decorator(self.partition)
        bigquery_loader = self.get_bigquery_loader(
            self.destination_table, partition_decorator
        )
        bigquery_loader.write_table(self.schema, data)
        # Firebase
        cache = firestore_data(cache)
        self.set_firebase(cache, attr="firestore_destination", is_complete=True)


class HourlyCacheAggregatorMixin(HourlyAggregatorMixin):
    def get_last_partition(self, timestamp):
        timestamp -= pd.Timedelta("1h")
        return timestamp

    @property
    def partition_iterator(self):
        period = pendulum.period(self.period_from, self.period_to)  # Not reverse order
        return period.range("hours")

    @property
    def is_cache_required(self):
        return self.timestamp_from != self.get_min_timestamp()


class DailyCacheAggregatorMixin(DailyAggregatorMixin):
    def get_last_partition(self, timestamp):
        timestamp -= pd.Timedelta("1d")
        return timestamp

    @property
    def partition_iterator(self):
        period = pendulum.period(self.period_from, self.period_to)  # Not reverse order
        return period.range("days")

    @property
    def is_cache_required(self):
        return self.timestamp_from.date() != self.get_min_timestamp().date()
