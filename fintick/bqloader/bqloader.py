from google.cloud import bigquery

from ..constants import BIGQUERY_MAX_HOT
from .base import BaseBigQueryLoader


class BigQueryHourly(BaseBigQueryLoader):
    def set_partition(self, table):
        table.time_partitioning = bigquery.TimePartitioning(
            field="timestamp",
            type_=bigquery.TimePartitioningType.HOUR,
            expiration_ms=int(BIGQUERY_MAX_HOT.total_seconds()) * 1000,
        )
        return table


class BigQueryDaily(BaseBigQueryLoader):
    def set_partition(self, table):
        table.time_partitioning = bigquery.TimePartitioning(
            field="timestamp",
            type_=bigquery.TimePartitioningType.DAY,
        )
        return table
