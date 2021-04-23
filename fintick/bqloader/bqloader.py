import pandas as pd
from google.cloud import bigquery

from .base import BaseBigQueryLoader


class BigQueryTenMinutePartition(BaseBigQueryLoader):
    def set_partition(self, table):
        table.time_partitioning = bigquery.RangePartitioning(
            field="interval",
            type_=bigquery.PartitionRange(start=0, end=1440, interval=10),
            # No expiration_ms as will overwrite
        )
        return table


class BigQueryHourly(BaseBigQueryLoader):
    def set_partition(self, table):
        table.time_partitioning = bigquery.TimePartitioning(
            field="timestamp",
            type_=bigquery.TimePartitioningType.HOUR,
            expiration_ms=int(pd.Timedelta("7d").total_seconds()) * 1000,
        )
        return table


class BigQueryDaily(BaseBigQueryLoader):
    def set_partition(self, table):
        table.time_partitioning = bigquery.TimePartitioning(
            field="timestamp",
            type_=bigquery.TimePartitioningType.DAY,
        )
        return table
