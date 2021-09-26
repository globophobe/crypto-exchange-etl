import datetime

import awswrangler as wr
import pandas as pd
import pendulum
from firebase_admin import firestore
from google.cloud import bigquery

from .bqloader import (
    MULTIPLE_SYMBOL_AGGREGATE_SCHEMA,
    MULTIPLE_SYMBOL_ORDER_BY,
    MULTIPLE_SYMBOL_SCHEMA,
    SINGLE_SYMBOL_AGGREGATE_SCHEMA,
    SINGLE_SYMBOL_ORDER_BY,
    SINGLE_SYMBOL_SCHEMA,
    get_schema_columns,
    get_table_id,
)
from .controllers import FinTick, FinTickDailyMixin
from .db import MetatickTrade, get_or_create_tables
from .fscache import FirestoreCache
from .utils import get_trades_name


class Migrator(FinTickDailyMixin, FinTick):
    def __init__(
        self,
        provider=None,
        api_symbol=None,
        period_from=None,
        period_to=None,
        futures=False,
        is_aggregated=False,
        verbose=False,
    ):
        self.provider = provider
        self.api_symbol = api_symbol
        self.futures = futures
        self.is_aggregated = is_aggregated
        self.verbose = verbose
        self.period_from = self.get_period_from(period_from)
        self.period_to = self.get_period_to(period_to)
        get_or_create_tables()

    @property
    def exchange(self):
        return self.provider.lower()

    def get_suffix(self, sep="_"):
        parts = [self.symbol]
        if self.is_aggregated:
            parts.append("aggregated")
        return sep.join(parts)

    @property
    def source_table(self):
        suffix = self.get_suffix(sep="_")
        return get_table_id(self.exchange, suffix=suffix)

    def get_source(self, sep="_"):
        _, table_id = self.source_table.split(".")
        return table_id.replace("_", sep)

    def get_destination(self, sep="-"):
        return self.get_source().replace("_", sep)

    @property
    def log_prefix(self):
        name = self.get_destination(sep=" ")
        return name[0].capitalize() + name[1:]  # Cap 1st char

    @property
    def partition_iterator(self):
        period = pendulum.period(self.period_from, self.period_to)  # Not reverse order
        return period.range("days")

    @property
    def schema(self):
        if self.futures:
            if self.is_aggregated:
                return MULTIPLE_SYMBOL_AGGREGATE_SCHEMA
            else:
                return MULTIPLE_SYMBOL_SCHEMA
        else:
            if self.is_aggregated:
                return SINGLE_SYMBOL_AGGREGATE_SCHEMA
            else:
                return SINGLE_SYMBOL_SCHEMA

    @property
    def fields(self):
        return ", ".join([field for field in get_schema_columns(self.schema)])

    @property
    def order_by(self):
        if self.futures:
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
    def dynamodb_destination(self):
        collection = self.get_destination(sep="-")
        return FirestoreCache(collection)

    def get_period(self, timestamp):
        return timestamp.date()

    def get_period_from(self, period_from):
        timestamp = self.get_min_timestamp()
        if timestamp:
            p = self.get_period(timestamp)
            if period_from:
                if p > period_from:
                    return p
                else:
                    return period_from
            else:
                return p

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
            if period_to:
                if p < period_to:
                    return p
                else:
                    return period_to
            else:
                return p

    def get_timestamps_from_data(self, data, attr):
        timestamps = []
        if data:
            if self.futures:
                for key, symbol in data.items():
                    if isinstance(symbol, dict):
                        timestamp = symbol[attr]["timestamp"]
                        timestamps.append(timestamp)
                        break
            else:
                timestamps.append(data[attr]["timestamp"])
        return timestamps

    def source_has_data(self, document):
        return self.firestore_source.has_data(document)

    def main(self):
        if self.period_from and self.period_to:
            for partition in self.iter_partition():
                self.partition_decorator = self.get_partition_decorator(partition)
                document = self.get_document_name(partition)
                if self.source_has_data(document):
                    data_frame = self.get_data_frame()
                    self.process_data_frame(partition, data_frame)
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
        return bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("date", "DATE", self.partition)
            ]
        )

    @property
    def where_clause(self):
        return "date(timestamp) = @date"

    @property
    def one_minute_iterator(self):
        period = pendulum.period(
            datetime.datetime.combine(self.partition, datetime.time.min),
            datetime.datetime.combine(self.partition, datetime.time.max),
        )
        return period.range("minutes")

    def process_data_frame(self, partition, data_frame):
        for timestamp in self.one_minute_iterator:
            # Are there any trades?
            if data_frame is None:
                self.set_dynamodb(timestamp)
            else:
                if not self.is_raw_data_ok(timestamp):
                    next_timestamp = timestamp + pd.Timedelta("1m")
                    df = data_frame[
                        (data_frame.timestamp >= timestamp)
                        & (data_frame.timestamp < next_timestamp)
                    ]
                    if len(df):
                        self.write_to_s3(df, timestamp)
                    self.set_dynamodb(timestamp)

    def is_raw_data_ok(self, timestamp):
        key = self.get_destination()
        try:
            item = MetatickTrade.get(key, timestamp)
        except MetatickTrade.DoesNotExist:
            return False
        else:
            return item.ok

    def set_dynamodb(self, timestamp, data={}, ok=True):
        key = self.get_destination()
        MetatickTrade(key, timestamp, data=data, ok=ok).save()

    def get_s3_path(self, timestamp):
        bucket = get_trades_name()
        prefix = self.get_destination()
        filename = timestamp.strftime("%Y%m%dT%H%M.parquet")
        return f"s3://{bucket}/{prefix}/{filename}"

    def write_to_s3(self, data_frame, timestamp):
        path = self.get_s3_path(timestamp)
        # boto3_session = Session(
        #     aws_access_key_id=os.environ[AWS_ACCESS_KEY_ID],
        #     aws_secret_access_key=os.environ[AWS_SECRET_ACCESS_KEY],
        #     region_name=os.environ[AWS_REGION],
        # )
        wr.s3.to_parquet(
            # boto3_session=boto3_session,
            df=data_frame,
            path=path,
        )
