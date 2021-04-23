from google.cloud import bigquery

SINGLE_SYMBOL_ORDER_BY = "timestamp, nanoseconds, index"

MULTIPLE_SYMBOL_ORDER_BY = "symbol, timestamp, nanoseconds, index"


def field(name, t):
    return bigquery.SchemaField(name, t, "REQUIRED")


SINGLE_SYMBOL_SCHEMA = [
    field("uid", "STRING"),  # For verification
    field("timestamp", "TIMESTAMP"),
    field("nanoseconds", "INTEGER"),
    field("price", "BIGNUMERIC"),
    field("volume", "BIGNUMERIC"),
    field("notional", "BIGNUMERIC"),
    field("tickRule", "INTEGER"),
    field("index", "INTEGER"),
]

MULTIPLE_SYMBOL_SCHEMA = (
    [field("uid", "STRING"), field("symbol", "STRING")]
    + SINGLE_SYMBOL_SCHEMA[1:-1]
    + [field("expiry", "TIMESTAMP"), field("index", "INTEGER")]
)


SINGLE_SYMBOL_AGGREGATE_SCHEMA = [
    field("timestamp", "TIMESTAMP"),
    field("nanoseconds", "INTEGER"),
    field("price", "BIGNUMERIC"),
    field("vwap", "BIGNUMERIC"),
    field("volume", "BIGNUMERIC"),
    field("notional", "BIGNUMERIC"),
    field("ticks", "INTEGER"),
    field("tickRule", "INTEGER"),
    field("index", "INTEGER"),
]


MULTIPLE_SYMBOL_AGGREGATE_SCHEMA = (
    [field("symbol", "STRING")]
    + SINGLE_SYMBOL_AGGREGATE_SCHEMA[:-1]
    + [field("expiry", "TIMESTAMP"), field("index", "INTEGER")]
)


SINGLE_SYMBOL_INTERVAL_RANGE_SCHEMA = [
    field("partition", "INTEGER")
] + SINGLE_SYMBOL_SCHEMA


MULTIPLE_SINGLE_SYMBOL_RANGE_INTERVAL_SCHEMA = [
    field("partition", "INTEGER")
] + MULTIPLE_SYMBOL_SCHEMA


SINGLE_SYMBOL_INTERVAL_RANGE_AGGREGATE_SCHEMA = [
    field("partition", "INTEGER")
] + SINGLE_SYMBOL_AGGREGATE_SCHEMA


MULTIPLE_SYMBOL_INTERVAL_RANGE_AGGREGATE_SCHEMA = [
    field("partition", "INTEGER")
] + MULTIPLE_SYMBOL_AGGREGATE_SCHEMA


SINGLE_SYMBOL_BAR_SCHEMA = [
    field("timestamp", "TIMESTAMP"),
    field("nanoseconds", "INTEGER"),
    field("open", "BIGNUMERIC"),
    field("high", "BIGNUMERIC"),
    field("low", "BIGNUMERIC"),
    field("close", "BIGNUMERIC"),
    field("buyVolume", "BIGNUMERIC"),
    field("volume", "BIGNUMERIC"),
    field("buyNotional", "BIGNUMERIC"),
    field("notional", "BIGNUMERIC"),
    field("buyTicks", "INTEGER"),
    field("ticks", "INTEGER"),
    bigquery.SchemaField(
        "topN",
        "RECORD",
        mode="REPEATED",
        fields=(
            field("timestamp", "TIMESTAMP"),
            field("nanoseconds", "INTEGER"),
            field("price", "BIGNUMERIC"),
            field("vwap", "BIGNUMERIC"),
            field("volume", "BIGNUMERIC"),
            field("notional", "BIGNUMERIC"),
            field("ticks", "INTEGER"),
            field("tickRule", "INTEGER"),
        ),
    ),
]


MULTIPLE_SYMBOL_BAR_SCHEMA = (
    [field("symbol", "STRING")]
    + SINGLE_SYMBOL_BAR_SCHEMA
    + [field("expiry", "TIMESTAMP")]
)


SINGLE_SYMBOL_RENKO_SCHEMA = [
    field("timestamp", "TIMESTAMP"),
    field("nanoseconds", "INTEGER"),
    field("level", "BIGNUMERIC"),
    field("price", "BIGNUMERIC"),
] + SINGLE_SYMBOL_BAR_SCHEMA[6:]


MULTIPLE_SYMBOL_RENKO_SCHEMA = [field("symbol", "STRING")] + SINGLE_SYMBOL_RENKO_SCHEMA
