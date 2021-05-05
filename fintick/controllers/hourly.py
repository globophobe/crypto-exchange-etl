import datetime

import pandas as pd
import pendulum

from ..bqloader import BigQueryHourly


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
        for dt in self.partition_iterator:
            self.timestamp_from = self.partition = self.get_partition(dt)
            self.timestamp_to = (
                self.timestamp_from
                + pd.Timedelta("1 hour")
                - datetime.timedelta(microseconds=1)
            )
            yield self.partition


class FinTickMultiSymbolHourlyMixin(FinTickHourlyMixin):
    @property
    def active_symbols(self):
        return [
            s
            for s in self.symbols
            if s["expiry"] - datetime.timedelta(microseconds=1) >= self.partition
        ]
