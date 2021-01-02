import os

import google.auth
import pandas as pd
from google.cloud import bigquery

from ..constants import BIGQUERY_DATASET, BIGQUERY_LOCATION, PROJECT_ID
from .lib import get_schema_columns, get_table_id


class BigQueryLoader:
    def __init__(self, table_name, date):
        credentials, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        self.bq = bigquery.Client(
            credentials=credentials,
            project=os.environ[PROJECT_ID],
            location=os.environ.get(BIGQUERY_LOCATION, None),
        )

        self.dataset = os.environ[BIGQUERY_DATASET]
        self.table_id = get_table_id(table_name)
        self.table_name = table_name
        self.date = date

    def table_exists(self):
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("table", "STRING", self.table_name)
            ]
        )
        query = f"""
            SELECT size_bytes FROM {self.dataset}.__TABLES__
            WHERE table_id = @table;
        """
        job = self.bq.query(query, job_config=job_config)
        rows = job.result()
        return len(list(rows)) > 0

    def create_table(self, schema):
        table_id = f"{self.bq.project}.{self.dataset}.{self.table_name}"
        table = bigquery.Table(table_id, schema=schema)
        # Partition on date.
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="date"
        )
        table = self.bq.create_table(table)

    def read_table(self, sql, job_config):
        query = self.bq.query(sql, job_config=job_config)
        return query.result().to_dataframe()

    def write_table(self, schema, data):
        if not self.table_exists():
            self.create_table(schema)
        # Partition by date.
        decorator = self.date.strftime("%Y%m%d")
        partition = f"{self.table_id}${decorator}"
        job_config = bigquery.LoadJobConfig(
            schema=schema, write_disposition="WRITE_TRUNCATE"
        )
        if isinstance(data, pd.DataFrame):
            # If data_frame, get columns
            columns = get_schema_columns(schema)
            data = data[columns]
            job = self.bq.load_table_from_dataframe(
                data, partition, job_config=job_config
            )
        else:
            # If dict, assume correct
            job = self.bq.load_table_from_json(data, partition, job_config=job_config)
        job.result()

    def delete_table(self):
        if self.table_exists():
            self.bq.delete_table(self.table_id)
