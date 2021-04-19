from google.cloud import bigquery

SINGLE_SYMBOL_ORDER_BY = "timestamp, nanoseconds, index"

MULTIPLE_SYMBOL_ORDER_BY = "symbol, timestamp, nanoseconds, index"

SINGLE_SYMBOL_SCHEMA = [
    bigquery.SchemaField("uid", "STRING", "REQUIRED"),  # For verification
    bigquery.SchemaField("timestamp", "TIMESTAMP", "REQUIRED"),
    bigquery.SchemaField("nanoseconds", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("price", "BIGNUMERIC", "REQUIRED"),
    bigquery.SchemaField("volume", "BIGNUMERIC", "REQUIRED"),
    bigquery.SchemaField("notional", "BIGNUMERIC", "REQUIRED"),
    bigquery.SchemaField("tickRule", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("index", "INTEGER", "REQUIRED"),
]

MULTIPLE_SYMBOL_SCHEMA = (
    [
        bigquery.SchemaField("uid", "STRING", "REQUIRED"),
        bigquery.SchemaField("symbol", "STRING", "REQUIRED"),
    ]
    + SINGLE_SYMBOL_SCHEMA[1:-1]
    + [
        bigquery.SchemaField("expiry", "TIMESTAMP", "REQUIRED"),
        bigquery.SchemaField("index", "INTEGER", "REQUIRED"),
    ]
)


SINGLE_SYMBOL_AGGREGATE_SCHEMA = [
    bigquery.SchemaField("timestamp", "TIMESTAMP", "REQUIRED"),
    bigquery.SchemaField("nanoseconds", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("price", "BIGNUMERIC", "REQUIRED"),
    bigquery.SchemaField("vwap", "BIGNUMERIC", "REQUIRED"),
    bigquery.SchemaField("volume", "BIGNUMERIC", "REQUIRED"),
    bigquery.SchemaField("notional", "BIGNUMERIC", "REQUIRED"),
    bigquery.SchemaField("ticks", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("tickRule", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("index", "INTEGER", "REQUIRED"),
]


MULTIPLE_SYMBOL_AGGREGATE_SCHEMA = (
    [
        bigquery.SchemaField("symbol", "STRING", "REQUIRED"),
    ]
    + SINGLE_SYMBOL_AGGREGATE_SCHEMA[:-1]
    + [
        bigquery.SchemaField("expiry", "TIMESTAMP", "REQUIRED"),
        bigquery.SchemaField("index", "INTEGER", "REQUIRED"),
    ]
)


SINGLE_SYMBOL_BAR_SCHEMA = [
    bigquery.SchemaField("timestamp", "TIMESTAMP", "REQUIRED"),
    bigquery.SchemaField("nanoseconds", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("open", "BIGNUMERIC", "REQUIRED"),
    bigquery.SchemaField("high", "BIGNUMERIC", "REQUIRED"),
    bigquery.SchemaField("low", "BIGNUMERIC", "REQUIRED"),
    bigquery.SchemaField("close", "BIGNUMERIC", "REQUIRED"),
    bigquery.SchemaField("buyVolume", "BIGNUMERIC", "REQUIRED"),
    bigquery.SchemaField("volume", "BIGNUMERIC", "REQUIRED"),
    bigquery.SchemaField("buyNotional", "BIGNUMERIC", "REQUIRED"),
    bigquery.SchemaField("notional", "BIGNUMERIC", "REQUIRED"),
    bigquery.SchemaField("buyTicks", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("ticks", "INTEGER", "REQUIRED"),
    bigquery.SchemaField(
        "topN",
        "RECORD",
        mode="REPEATED",
        fields=(
            bigquery.SchemaField("timestamp", "TIMESTAMP", "REQUIRED"),
            bigquery.SchemaField("nanoseconds", "INTEGER", "REQUIRED"),
            bigquery.SchemaField("price", "BIGNUMERIC", "REQUIRED"),
            bigquery.SchemaField("vwap", "BIGNUMERIC", "REQUIRED"),
            bigquery.SchemaField("volume", "BIGNUMERIC", "REQUIRED"),
            bigquery.SchemaField("notional", "BIGNUMERIC", "REQUIRED"),
            bigquery.SchemaField("ticks", "INTEGER", "REQUIRED"),
            bigquery.SchemaField("tickRule", "INTEGER", "REQUIRED"),
        ),
    ),
    bigquery.SchemaField(
        "footprintN",
        "RECORD",
        mode="REPEATED",
        fields=(
            bigquery.SchemaField("timestamp", "TIMESTAMP", "REQUIRED"),
            bigquery.SchemaField("nanoseconds", "INTEGER", "REQUIRED"),
            bigquery.SchemaField("price", "BIGNUMERIC", "REQUIRED"),
            bigquery.SchemaField("vwap", "BIGNUMERIC", "REQUIRED"),
            bigquery.SchemaField("volume", "BIGNUMERIC", "REQUIRED"),
            bigquery.SchemaField("notional", "BIGNUMERIC", "REQUIRED"),
            bigquery.SchemaField("ticks", "INTEGER", "REQUIRED"),
            bigquery.SchemaField("tickRule", "INTEGER", "REQUIRED"),
        ),
    ),
]


MULTIPLE_SYMBOL_BAR_SCHEMA = (
    [
        bigquery.SchemaField("symbol", "STRING", "REQUIRED"),
    ]
    + SINGLE_SYMBOL_BAR_SCHEMA
    + [
        bigquery.SchemaField("expiry", "TIMESTAMP", "REQUIRED"),
    ]
)

SINGLE_SYMBOL_THRESHOLD_SCHEMA = [
    bigquery.SchemaField("uid", "STRING", "REQUIRED")  # For join
] + SINGLE_SYMBOL_BAR_SCHEMA

MULTIPLE_SYMBOL_THRESHOLD_SCHEMA = [
    bigquery.SchemaField("uid", "STRING", "REQUIRED")  # For join
] + MULTIPLE_SYMBOL_BAR_SCHEMA

SINGLE_SYMBOL_RENKO_SCHEMA = [
    bigquery.SchemaField("timestamp", "TIMESTAMP", "REQUIRED"),
    bigquery.SchemaField("nanoseconds", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("level", "BIGNUMERIC", "REQUIRED"),
    bigquery.SchemaField("price", "BIGNUMERIC", "REQUIRED"),
] + SINGLE_SYMBOL_BAR_SCHEMA[6:]

MULTIPLE_SYMBOL_RENKO_SCHEMA = [
    bigquery.SchemaField("symbol", "STRING", "REQUIRED"),
] + SINGLE_SYMBOL_RENKO_SCHEMA
