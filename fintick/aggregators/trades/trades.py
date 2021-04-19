import pendulum

from ...fintick import FinTickDailyPartitionFromHourlyMixin
from ...fscache import FirestoreCache
from ..base import DailyAggregatorMixin, HourlyAggregatorMixin
from ..utils import assert_aggregated_table, get_firestore_collection
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
        table_name = assert_aggregated_table(self.destination_table, hot=True)
        collection = get_firestore_collection(table_name)
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
                    import pdb

                    pdb.set_trace()
                    self.write(data_frame)
                    self.clean_firestore()

    def load_data_frame(self):
        table_id = assert_aggregated_table(self.destination_table, hot=True)
        sql = f"""
            SELECT * FROM {table_id}
            WHERE {self.where_clause}
            ORDER BY {self.order_by}
        """
        partition_decorator = self.get_partition_decorator(self.partition)
        bigquery_loader = self.get_bigquery_loader(table_id, partition_decorator)
        return bigquery_loader.read_table(sql, self.job_config)


class TradeAggregatorDailyPartition(DailyAggregatorMixin, BaseTradeAggregator):
    pass
