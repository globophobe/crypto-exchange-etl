import datetime

import pandas as pd
import pendulum
from google.cloud import bigquery

from ..bqloader import (
    SINGLE_SYMBOL_ORDER_BY,
    BigQueryDaily,
    get_schema_columns,
    get_table_id,
)
from ..fscache import FirestoreCache
from ..s3downloader import (
    HistoricalDownloader,
    calculate_notional,
    calculate_tick_rule,
    set_dtypes,
    strip_nanoseconds,
    utc_timestamp,
)
from ..utils import parse_period_from_to


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
        columns = get_schema_columns(self.schema)
        return data_frame[columns]

    def write(self, data_frame, is_complete=False):
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


class FinTickDailySequentialIntegerPaginationMixin(FinTickDailyHourlyMixin):
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


class FinTickDailySequentialIntegerMixin(FinTickDailySequentialIntegerPaginationMixin):
    """
    Binance and Coinbase REST APIs are slow to iterate. As a result, cloud functions
    may timeout iterating for the daily partition.

    However, the trade uid of both exchanges is a sequential integer id. If complete, the
    daily partition can be copied from hourly partitions.
    """

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


class FinTickDailyPartitionFromHourlyMixin(FinTickDailyMixin, FinTickDailyHourlyMixin):
    def main(self):
        for partition in self.iter_partition():
            data = self.get_document(partition)
            ok = self.is_data_OK(data)
            if not ok:
                period = pendulum.period(self.timestamp_from, self.timestamp_to)
                documents = [
                    self.get_hourly_document(timestamp)
                    for timestamp in period.range("hours")
                ]
                has_data = all([document and document["ok"] for document in documents])
                if has_data:
                    data_frame = self.load_data_frame()
                    self.assert_data_frame(data_frame)
                    self.write(data_frame)
                    self.clean_firestore()
                    return True
            return ok

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
        # TODO: Merge this method with FinTickDailyS3Mixin
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
