from datetime import timezone
from decimal import Decimal


def utc_timestamp(data_frame):
    # Because pyarrow.lib.ArrowInvalid: Casting from timestamp[ns]
    # to timestamp[us, tz=UTC] would lose data.
    data_frame.timestamp = data_frame.apply(
        lambda x: x.timestamp.tz_localize(timezone.utc), axis=1
    )
    return data_frame


def strip_nanoseconds(data_frame):
    # Bitmex data is accurate to the nanosecond.
    # However, data is typically only provided to the microsecond.
    data_frame["nanoseconds"] = data_frame.apply(
        lambda x: x.timestamp.nanosecond, axis=1
    )
    data_frame.timestamp = data_frame.apply(
        lambda x: x.timestamp.replace(nanosecond=0)
        if x.nanoseconds > 0
        else x.timestamp,
        axis=1,
    )
    return data_frame


def calculate_notional(data_frame):
    data_frame["notional"] = data_frame.apply(lambda x: x.volume / x.price, axis=1)
    return data_frame


def calculate_tick_rule(data_frame):
    data_frame["tickRule"] = data_frame.apply(
        lambda x: (1 if x.tickDirection in ("PlusTick", "ZeroPlusTick") else -1),
        axis=1,
    )
    return data_frame


def set_dtypes(data_frame):
    df = data_frame.astype({"index": "int64"})
    for column in ("price", "volume"):
        df = set_type_decimal(df, column)
    return df


def set_type_decimal(data_frame, column):
    data_frame[column] = data_frame[column].apply(Decimal)
    return data_frame


def row_to_json(row):
    return {
        "timestamp": row.timestamp.to_pydatetime(),
        "nanoseconds": row.nanoseconds,
        "price": row.price,
        "volume": row.volume,
        "notional": row.notional,
        "tickRule": row.tickRule,
        "index": row["index"],  # B/C pandas index
    }
