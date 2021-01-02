from google.cloud import bigquery

SINGLE_SYMBOL_SCHEMA = [
    bigquery.SchemaField("date", "DATE", "REQUIRED"),
    bigquery.SchemaField("timestamp", "TIMESTAMP", "REQUIRED"),
    bigquery.SchemaField("nanoseconds", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("price", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("volume", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("notional", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("tickRule", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("index", "INTEGER", "REQUIRED"),
]


SINGLE_SYMBOL_AGGREGATE_SCHEMA = [
    bigquery.SchemaField("date", "DATE", "REQUIRED"),
    bigquery.SchemaField("timestamp", "TIMESTAMP", "REQUIRED"),
    bigquery.SchemaField("nanoseconds", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("price", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("slippage", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("volume", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("notional", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("tickRule", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("exponent", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("index", "INTEGER", "REQUIRED"),
]


MULTIPLE_SYMBOL_SCHEMA = [
    bigquery.SchemaField("date", "DATE", "REQUIRED"),
    bigquery.SchemaField("symbol", "STRING", "REQUIRED"),
    bigquery.SchemaField("timestamp", "TIMESTAMP", "REQUIRED"),
    bigquery.SchemaField("nanoseconds", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("price", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("volume", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("notional", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("tickRule", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("index", "INTEGER", "REQUIRED"),
]


MULTIPLE_SYMBOL_AGGREGATE_SCHEMA = [
    bigquery.SchemaField("date", "DATE", "REQUIRED"),
    bigquery.SchemaField("timestamp", "TIMESTAMP", "REQUIRED"),
    bigquery.SchemaField("nanoseconds", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("price", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("slippage", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("volume", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("notional", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("tickRule", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("exponent", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("index", "INTEGER", "REQUIRED"),
]
