import base64
import datetime
import json
import os
import time
from pathlib import Path

import pandas as pd
from google.cloud import pubsub_v1

from .constants import (
    BIGQUERY_HOT,
    GCP_APPLICATION_CREDENTIALS,
    PRODUCTION_ENV_VARS,
    PROJECT_ID,
)


def set_environment():
    if not os.path.exists("/.dockerenv"):
        with open("env.yaml", "r") as env:
            for line in env:
                key, value = line.split(": ")
                v = value.strip()
                if key in GCP_APPLICATION_CREDENTIALS:
                    path = Path.cwd().parents[0] / "keys" / v
                    v = str(path.resolve())
                    with open(v) as key_file:
                        data = json.loads(key_file.read())
                        os.environ[PROJECT_ID] = data["project_id"]
                os.environ[key] = v


def get_env_vars():
    env_vars = {}
    with open("env.yaml", "r") as env:
        for line in env:
            key, value = line.split(": ")
            env_vars[key] = value.strip()
    return env_vars


def get_deploy_env_vars(pre="", sep=",", keys=PRODUCTION_ENV_VARS):
    env_vars = [
        f"{pre}{key}={value}" for key, value in get_env_vars().items() if key in keys
    ]
    return f"{sep}".join(env_vars)


def get_env_list(key):
    value = os.environ.get(key, "")
    return [v for v in value.split(" ") if v]


def set_env_list(key, value):
    if "PYTEST_CURRENT_TEST" in os.environ:
        values = get_env_list(key)
        values.append(value)
        values = list(set(values))
        os.environ[key] = " ".join(values)


def json_str_or_list(string):
    if string:
        return json.loads(string)
    return []


def is_local():
    return all([os.environ.get(key, None) for key in GCP_APPLICATION_CREDENTIALS])


def parse_period_from_to(period_from=None, period_to=None, timestamp_to_max=None):
    date_from = date_to = timestamp_from = timestamp_to = None
    period_from = parse_period(period_from)
    period_to = parse_period(period_to)
    now = datetime.datetime.utcnow()
    today = now.date()
    date_to_max = today - BIGQUERY_HOT
    timestamp_from_min = datetime.datetime.combine(
        date_to_max + pd.Timedelta("1d"), datetime.datetime.min.time()
    )
    if isinstance(period_to, pd.Timedelta):
        timestamp_to = absolute_delta(now, period_to)
        date_to = date_to_max
    elif isinstance(period_to, datetime.date):
        date_to = period_to
        if date_to < date_to_max:
            timestamp_from = timestamp_to = None
    else:
        date_to = date_to_max
        timestamp_to = timestamp_to_max or now
    if isinstance(period_from, pd.Timedelta):
        timestamp_from = absolute_delta(now, period_from)
        if timestamp_from < timestamp_from_min:
            date_from = timestamp_from.date()
            date_to = date_to_max
            timestamp_from = timestamp_from_min
        else:
            date_from = date_to = None
    elif isinstance(period_from, datetime.date):
        date_from = period_from
        timestamp_from = timestamp_from_min
    else:
        date_from = datetime.date(2010, 1, 1)
        if timestamp_to and not timestamp_from:
            timestamp_from = timestamp_from_min
    # Is period_from before period_to?
    if timestamp_to and timestamp_to < timestamp_from_min:
        timestamp_from = timestamp_to = None
        date_to = timestamp_to.date()
        assert (
            timestamp_from <= timestamp_to
        ), f'"{period_from}" should be before "{period_to}"'
    # Is timestamp_to greater than timestamp_to_max?
    if (timestamp_to and timestamp_to_max) and (timestamp_to > timestamp_to_max):
        timestamp_to = timestamp_to_max
    # UTC, please
    if timestamp_from:
        timestamp_from = timestamp_from.replace(tzinfo=datetime.timezone.utc)
    if timestamp_to:
        timestamp_to = timestamp_to.replace(
            minute=0, second=0, microsecond=0, tzinfo=datetime.timezone.utc
        )
    return timestamp_from, timestamp_to, date_from, date_to


def parse_period(value):
    if value:
        delta = parse_timedelta(value)
        if not delta:
            return parse_date(value)
        return delta


def parse_timedelta(value):
    try:
        delta = pd.Timedelta(value)
    except ValueError:
        pass
    else:
        return delta


def parse_date(value):
    try:
        date = datetime.date.fromisoformat(value)
    except ValueError:
        pass
    else:
        return date


def iter_api(
    url,
    get_api_pagination_id,  # Function
    get_api_timestamp,  # Function
    get_api_response,  # Function
    max_results,
    min_elapsed_per_request,
    timestamp_from=None,
    pagination_id=None,
    log_prefix=None,
):
    results = []
    last_data = []
    stop_iteration = False
    while not stop_iteration:
        start = time.time()
        data = get_api_response(url, pagination_id=pagination_id)
        if not len(data):
            is_last_iteration = stop_iteration = True
        else:
            last_trade = data[-1]
            timestamp = get_api_timestamp(last_trade)
            # Next pagination_id
            pagination_id = get_api_pagination_id(
                timestamp, last_data=last_data, data=data
            )
            if not pagination_id:
                stop_iteration = True
            # B/C unique
            last_data = data
            # Append results
            results += data
            is_last_iteration = len(data) < max_results
            is_within_partition = timestamp_from and timestamp > timestamp_from
            # Maybe stop iteration
            if is_last_iteration or not is_within_partition:
                stop_iteration = True
            # Basic logging for stackdriver
            if log_prefix and timestamp:
                t = timestamp.replace(tzinfo=None).isoformat()
                if not timestamp.microsecond:
                    t += ".000000"
                print(f"{log_prefix}: {t}")
        # Throttle requests
        elapsed = time.time() - start
        if elapsed < min_elapsed_per_request:
            time.sleep(min_elapsed_per_request - elapsed)
    return results, is_last_iteration


def get_delta(
    date=None, microseconds=0, milliseconds=0, seconds=0, minutes=0, hours=0, days=0
):
    assert microseconds or milliseconds or seconds or minutes or days
    if isinstance(date, datetime.date):
        date = datetime.datetime.combine(date, datetime.datetime.min.time())
    else:
        date = datetime.datetime.utcnow()
    date += datetime.timedelta(
        microseconds=microseconds,
        milliseconds=milliseconds,
        seconds=seconds,
        minutes=minutes,
        hours=hours,
        days=days,
    )
    return date.date()


def absolute_delta(value, delta):
    if delta.total_seconds() < 0:
        value += delta
    else:
        value -= delta
    return value


def get_request_data(request, keys):
    data = {key: None for key in keys}
    json_data = request.get_json()
    param_data = request.args
    for key in keys:
        if key in json_data:
            data[key] = json_data[key]
        elif key in param_data:
            data[key] = param_data[key]
    return data


def publish(topic, data):
    publisher = pubsub_v1.PublisherClient()
    topic = publisher.topic_path(os.environ[PROJECT_ID], topic)
    publisher.publish(topic, json.dumps(data).encode())


def base64_decode_event(event):
    """
    Use with pub/sub functions:

    def pubsub_function(event, context):
        data = base64_decode_event(event)
    """
    if "data" in event:
        data = base64.b64decode(event["data"]).decode()
        return json.loads(data)
    else:
        return {}


def base64_encode_dict(data):
    d = json.dumps(data).encode()
    return base64.b64encode(d)
