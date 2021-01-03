import pandas as pd
from google.cloud import bigquery

from ..bqloader import (
    MULTIPLE_SYMBOL_AGGREGATE_SCHEMA,
    SINGLE_SYMBOL_AGGREGATE_SCHEMA,
    BigQueryLoader,
    get_table_id,
)
from ..utils import date_range
from .base import BaseAggregator
from .lib import aggregate_trades, calculate_exponent
from .utils import publish_post_aggregation


class TradeAggregator(BaseAggregator):
    @property
    def schema(self):
        if self.has_multiple_symbols:
            return MULTIPLE_SYMBOL_AGGREGATE_SCHEMA
        else:
            return SINGLE_SYMBOL_AGGREGATE_SCHEMA

    def main(self):
        for date in date_range(self.date_from, self.date_to, reverse=True):
            self.date = date
            document = date.isoformat()
            if self.firestore_source.has_data(document):
                if not self.firestore_destination.has_data(document):
                    data_frame = self.get_data_frame()
                    df = self.parse_dataframe(data_frame)
                    self.write(df)
                elif self.verbose:
                    print(f"{self.log_prefix}: {document} OK")

        # Intended for GCP
        if len(self.post_aggregation):
            publish_post_aggregation(
                self.table_name, self.date_from, self.post_aggregation
            )

        print(
            f"{self.log_prefix}: "
            f"{self.date_from.isoformat()} to {self.date_to.isoformat()} OK"
        )

    def get_data_frame(self):
        table_id = get_table_id(self.get_source())
        # Query by partition.
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("date", "DATE", self.date)]
        )
        if self.has_multiple_symbols:
            fields = "timestamp, nanoseconds, symbol, price, volume, notional, tickRule"
            order_by = "symbol, timestamp, nanoseconds, index"
        else:
            fields = "timestamp, nanoseconds, price, volume, notional, tickRule"
            order_by = "timestamp, nanoseconds, index"
        sql = f"""
            SELECT {fields}
            FROM {table_id}
            WHERE date = @date
            ORDER BY {order_by};
        """
        return BigQueryLoader(self.get_source(), self.date).read_table(sql, job_config)

    def parse_dataframe(self, data_frame):
        # First, aggregate.
        samples = aggregate_trades(
            data_frame, has_multiple_symbols=self.has_multiple_symbols
        )
        data_frame = pd.DataFrame(samples, columns=self.columns)
        data_frame.slippage = data_frame.slippage.round(6)
        # Next, calculate exponent.
        data_frame = calculate_exponent(data_frame)
        data_frame["index"] = data_frame.index
        return data_frame

    def write(self, data_frame):
        # BigQuery
        table_name = self.get_destination(sep="_")
        bigquery_loader = BigQueryLoader(table_name, self.date)
        bigquery_loader.write_table(self.schema, data_frame)
        # Firebase
        self.set_firebase(data_frame, cache="firestore_destination", is_complete=True)
