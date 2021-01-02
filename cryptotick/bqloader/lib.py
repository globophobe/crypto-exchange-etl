import os

from ..constants import BIGQUERY_DATASET, BIGQUERY_TABLES
from ..utils import set_env_list


def get_table_id(table_name):
    dataset = os.environ[BIGQUERY_DATASET]
    return f"{dataset}.{table_name}"


def get_table_name(exchange, suffix=""):
    if "PYTEST_CURRENT_TEST" in os.environ:
        suffix = f"{suffix}_test" if suffix else "test"
    table_name = f"{exchange}_{suffix}" if suffix else exchange
    set_env_list(BIGQUERY_TABLES, table_name)
    return table_name


def get_schema_columns(schema):
    return [field.name for field in schema]
