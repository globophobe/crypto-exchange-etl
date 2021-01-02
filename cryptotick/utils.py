import base64
import datetime
import json
import os
from pathlib import Path

import pandas as pd
from google.cloud import pubsub_v1

from .constants import GCP_APPLICATION_CREDENTIALS, PRODUCTION_ENV_VARS, PROJECT_ID


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


def get_container_name(hostname="asia.gcr.io", image="crypto-exchange-etl"):
    project_id = os.environ[PROJECT_ID]
    return f"{hostname}/{project_id}/{image}"


def get_delta(
    d=None, microseconds=0, milliseconds=0, seconds=0, minutes=0, hours=0, days=0
):
    assert microseconds or milliseconds or seconds or minutes or days
    if isinstance(d, datetime.date):
        d = datetime.datetime.combine(d, datetime.datetime.min.time())
    else:
        d = datetime.datetime.utcnow()
    d += datetime.timedelta(
        microseconds=microseconds,
        milliseconds=milliseconds,
        seconds=seconds,
        minutes=minutes,
        hours=hours,
        days=days,
    )
    return d.date()


def date_range(date_from=None, date_to=None, reverse=False):
    today = datetime.datetime.utcnow().now().isoformat()
    date_from = date_from or today
    date_to = date_to or today
    assert date_from <= date_to
    timestamps = pd.date_range(start=date_from, end=date_to)
    dates = [d.date() for d in timestamps.tolist()]
    if reverse:
        dates.reverse()
    for date in dates:
        yield date


def publish(topic, data):
    publisher = pubsub_v1.PublisherClient()
    topic = publisher.topic_path(os.environ[PROJECT_ID], topic)
    publisher.publish(topic, json.dumps(data).encode())


def base64_decode_event(event):
    if "data" in event:
        data = base64.b64decode(event["data"]).decode()
        return json.loads(data)
    else:
        return {}


def base64_encode_dict(data):
    d = json.dumps(data).encode()
    return base64.b64encode(d)
