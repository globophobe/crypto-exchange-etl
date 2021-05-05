import pendulum

from ...controllers import FinTickDailyPartitionFromHourlyMixin
from ...fscache import FirestoreCache
from ..base import DailyAggregatorMixin, HourlyAggregatorMixin
from .base import BaseTradeAggregator


class TradeAggregatorHourlyPartition(HourlyAggregatorMixin, BaseTradeAggregator):
    pass


class TradeAggregatorDailyPartitionFromHourly(
    FinTickDailyPartitionFromHourlyMixin,
    BaseTradeAggregator,
):
    def get_period_from(self, period_from):
        # Do not validate with firestore data
        return period_from

    def get_period_to(self, period_to):
        # Do not validate with firestore data
        return period_to

    def get_hourly_document(self, timestamp):
        collection = self.destination_table(sep="-")
        document_name = timestamp.strftime("%Y-%m-%dT%H")
        return FirestoreCache(collection).get(document_name)

    def main(self):
        for partition in self.iter_partition():
            document = self.get_document_name(partition)
            data = self.firestore_destination.get(document)
            if not self.is_data_OK(data):
                period = pendulum.period(
                    self.timestamp_from,
                    self.timestamp_to,
                )
                documents = [
                    self.get_hourly_document(timestamp)
                    for timestamp in period.range("hours")
                ]
                has_data = all([document and document["ok"] for document in documents])
                if has_data:
                    data_frame = self.load_data_frame()
                    # Overwrite hourly partition indices with daily index
                    data_frame["index"] = data_frame.index.values
                    self.write(data_frame)
                    self.clean_firestore()

    def load_data_frame(self):
        table_id = self.destination_table
        sql = f"""
            SELECT * FROM {table_id}
            WHERE {self.where_clause}
            ORDER BY {self.order_by}
        """
        partition_decorator = self.get_partition_decorator(self.partition)
        bigquery_loader = self.get_bigquery_loader(table_id, partition_decorator)
        return bigquery_loader.read_table(sql, self.job_config)

    def write(self, data_frame, is_complete=True):
        # BigQuery
        partition_decorator = self.get_partition_decorator(self.partition)
        bigquery_loader = self.get_bigquery_loader(
            self.destination_table, partition_decorator
        )
        bigquery_loader.write_table(self.schema, data_frame)
        # Firebase
        self.set_firebase(
            data_frame, attr="firestore_destination", is_complete=is_complete
        )


class TradeAggregatorDailyPartition(DailyAggregatorMixin, BaseTradeAggregator):
    pass
