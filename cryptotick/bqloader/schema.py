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

SINGLE_SYMBOL_RENKO_SCHEMA = [
    bigquery.SchemaField("date", "DATE", "REQUIRED"),
    bigquery.SchemaField("timestamp", "TIMESTAMP", "REQUIRED"),
    bigquery.SchemaField("nanoseconds", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("price", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("level", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("change", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("slippage", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("buySlippage", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("volume", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("buyVolume", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("notional", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("buyNotional", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("ticks", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("buyTicks", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("index", "INTEGER", "REQUIRED"),
    bigquery.SchemaField(
        "topN",
        "RECORD",
        mode="REPEATED",
        fields=(
            bigquery.SchemaField("timestamp", "TIMESTAMP", "REQUIRED"),
            bigquery.SchemaField("nanoseconds", "INTEGER", "REQUIRED"),
            bigquery.SchemaField("price", "FLOAT", "REQUIRED"),
            bigquery.SchemaField("slippage", "FLOAT", "REQUIRED"),
            bigquery.SchemaField("volume", "FLOAT", "REQUIRED"),
            bigquery.SchemaField("notional", "FLOAT", "REQUIRED"),
            bigquery.SchemaField("exponent", "INTEGER", "REQUIRED"),
            bigquery.SchemaField("tickRule", "INTEGER", "REQUIRED"),
        ),
    ),
]

MULTIPLE_SYMBOL_RENKO_SCHEMA = [
    bigquery.SchemaField("date", "DATE", "REQUIRED"),
    bigquery.SchemaField("symbol", "STRING", "REQUIRED"),
] + SINGLE_SYMBOL_RENKO_SCHEMA[1:]

BAR_SCHEMA = [
    bigquery.SchemaField("date", "DATE", "REQUIRED"),
    bigquery.SchemaField("timestamp", "TIMESTAMP", "REQUIRED"),
    bigquery.SchemaField("nanoseconds", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("open", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("high", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("low", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("close", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("volume", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("buyVolume", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("notional", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("buyNotional", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("ticks", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("buyTicks", "INTEGER", "REQUIRED"),
]
