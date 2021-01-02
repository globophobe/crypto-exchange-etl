import datetime
import os
from copy import copy

from ..constants import FIRESTORE_COLLECTIONS
from ..utils import set_env_list


def get_collection_name(exchange, suffix=""):
    if "PYTEST_CURRENT_TEST" in os.environ:
        suffix = f"{suffix}-test" if suffix else "test"
    collection = f"{exchange}-{suffix}" if suffix else exchange
    set_env_list(FIRESTORE_COLLECTIONS, collection)
    return collection


def firestore_data(data):
    data = copy(data)
    # Firestore doesn't like datetime.date
    if "date" in data:
        del data["date"]
    # Timestamp
    for key in ("timestamp", "listing", "expiry"):
        if key in data:
            # UTC, please.
            data[key] = data[key].replace(tzinfo=datetime.timezone.utc)
    # Float
    for key in ("open", "high", "low", "close", "price", "volume", "notional"):
        if key in data:
            data[key] = float(data[key])
    # Int
    for key in ("nanoseconds", "tickRule", "ticks", "index"):
        if key in data:
            data[key] = int(data[key])
    return data
