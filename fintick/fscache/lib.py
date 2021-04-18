import datetime
import os
from copy import copy
from decimal import Decimal

from google.api_core.datetime_helpers import DatetimeWithNanoseconds

from ..constants import FIRESTORE_COLLECTIONS
from ..utils import set_env_list


def get_collection_name(exchange, suffix=""):
    if "PYTEST_CURRENT_TEST" in os.environ:
        suffix = f"{suffix}-test" if suffix else "test"
    collection = f"{exchange}-{suffix}" if suffix else exchange
    set_env_list(FIRESTORE_COLLECTIONS, collection)
    return collection


def firestore_data(data, deserialize=False):
    data = copy(data)
    # Timestamp
    for key in ("timestamp", "listing", "expiry"):
        if key in data:
            # UTC, please.
            value = data[key]
            value = value.replace(tzinfo=datetime.timezone.utc)
            # Maybe not set
            if isinstance(value, DatetimeWithNanoseconds):
                try:
                    value.nanosecond
                except AttributeError:
                    value._nanosecond = 0
            data[key] = value
    # Decimal
    for key in (
        "level",
        "change",
        "open",
        "high",
        "low",
        "close",
        "price",
        "vwap",
        "volume",
        "buyVolume",
        "notional",
        "buyNotional",
    ):
        if key in data:
            # Firestore does not support the decimal type,
            # so serialize to str
            value = data[key]
            if isinstance(value, dict):
                data[key] = firestore_data(value)
            elif deserialize:
                data[key] = Decimal(value)
            else:
                data[key] = str(value)
    # Int
    for key in (
        "nanoseconds",
        "tickRule",
        "ticks",
        "buyTicks",
        "index",
    ):
        if key in data:
            data[key] = int(data[key])
    if "nextDay" in data:
        data["nextDay"] = firestore_data(data["nextDay"], deserialize=deserialize)
    if "topN" in data:
        data["topN"] = [
            firestore_data(t, deserialize=deserialize) for t in data["topN"]
        ]
    if "candles" in data:
        data["candles"] = [
            firestore_data(c, deserialize=deserialize) for c in data["candles"]
        ]
    return data
