from google.cloud import bigquery

from ...bqloader import (
    MULTIPLE_SYMBOL_RENKO_SCHEMA,
    SINGLE_SYMBOL_RENKO_SCHEMA,
    BigQueryLoader,
    get_table_id,
    stringify_datetime_types,
)
from ...fscache import firestore_data
from ...utils import date_range
from ..base import BaseAggregator
from .lib import aggregate_renko, get_initial_cache, get_value_display


class Renko(BaseAggregator):
    def __init__(
        self,
        table_name,
        box_size,
        reversal=1,
        top_n=10,
        basename=None,
        date_from=None,
        date_to=None,
        has_multiple_symbols=False,
        verbose=False,
    ):
        super().__init__(
            table_name,
            basename=basename,
            date_from=date_from,
            date_to=date_to,
            require_cache=True,
            has_multiple_symbols=has_multiple_symbols,
            verbose=verbose,
        )
        self.box_size = float(box_size)
        self.reversal = reversal
        self.top_n = top_n

    @property
    def schema(self):
        if self.has_multiple_symbols:
            return MULTIPLE_SYMBOL_RENKO_SCHEMA
        else:
            return SINGLE_SYMBOL_RENKO_SCHEMA

    def get_destination(self, sep="_"):
        basename = self.get_basename(sep=sep)
        box_size_display = get_value_display(self.box_size)
        return f"{basename}{sep}renko{sep}{box_size_display}"

    def get_initial_cache(self, data_frame):
        return get_initial_cache(data_frame, self.box_size)

    def main(self):
        for date in date_range(self.date_from, self.date_to):
            self.date = date
            document = date.isoformat()
            if self.firestore_source.has_data(document):
                if not self.firestore_destination.has_data(document):
                    data_frame = self.get_data_frame(date)
                    data, cache = self.process_data_frame(data_frame)
                    self.write(data, cache)
                elif self.verbose:
                    print(f"{self.log_prefix}: {document} OK")
        print(
            f"{self.log_prefix}: "
            f"{self.date_from.isoformat()} to {self.date_to.isoformat()} OK"
        )

    def get_data_frame(self, date, index=None):
        table_id = get_table_id(self.table_name)
        # Query by partition.
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("date", "DATE", date),
                bigquery.ScalarQueryParameter("index", "INTEGER", index),
            ]
        )
        fields = (
            "timestamp, nanoseconds, price, slippage, "
            "volume, notional, tickRule, exponent"
        )
        clause = "AND index >= @index" if index else ""
        sql = f"""
            SELECT {fields}
            FROM {table_id}
            WHERE date = @date
            {clause}
            ORDER BY timestamp, nanoseconds, index;
        """
        return BigQueryLoader(self.table_name, self.date).read_table(sql, job_config)

    def process_data_frame(self, data_frame):
        data_frame, cache = self.get_cache(data_frame)
        data, cache = aggregate_renko(data_frame, cache, self.box_size, self.top_n)
        # Index
        for index, d in enumerate(data):
            data[index] = stringify_datetime_types(firestore_data(d, strip_date=False))
            data[index]["topN"] = [
                stringify_datetime_types(firestore_data(t)) for t in d["topN"]
            ]
            data[index]["index"] = index
        return data, cache

    def write(self, data, cache):
        # BigQuery
        table_name = self.get_destination(sep="_")
        bigquery_loader = BigQueryLoader(table_name, self.date)
        bigquery_loader.write_table(self.schema, data)
        # Firebase
        self.set_firebase(
            firestore_data(cache), attr="firestore_destination", is_complete=True
        )
