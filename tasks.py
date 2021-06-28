import datetime
import os
import re
from uuid import uuid4

from invoke import task

from fintick.constants import BIGQUERY_LOCATION, FINTICK
from fintick.fscache import FirestoreCache
from fintick.functions import (
    fintick_aggregate_bars_gcp,
    fintick_aggregate_gcp,
    fintick_api_gcp,
    fintick_scheduler_gcp,
)
from fintick.providers.utils import get_providers_from_config_yaml
from fintick.utils import get_container_name, get_deploy_env_vars, set_environment

set_environment()

NAME_REGEX = re.compile(r"^(\w+)_gcp$")

all_functions = (
    fintick_api_gcp,
    fintick_aggregate_gcp,
    fintick_aggregate_bars_gcp,
)


@task
def export_requirements(c):
    c.run("poetry export --output requirements.txt")
    c.run("poetry export --dev --output requirements-dev.txt")


@task
def upload_config(c):
    data = {
        "providers": get_providers_from_config_yaml(),
        "timestamp": datetime.datetime.utcnow(),
    }
    FirestoreCache(FINTICK).set(uuid4().hex, data)


@task
def deploy_function(c, entry_point, memory=256, is_http=True):
    name = NAME_REGEX.match(entry_point).group(1).replace("_", "-")
    region = os.environ[BIGQUERY_LOCATION]
    timeout = 540  # Max, 9 minutes
    env_vars = get_deploy_env_vars()
    cmd = f"""
        gcloud functions deploy {name}-{memory} \
            --region={region} \
            --memory={memory}MB \
            --timeout={timeout}s \
            --runtime=python38 \
            --entry-point={entry_point} \
            --set-env-vars={env_vars} \
    """
    if is_http:
        cmd += "--trigger-http"
    else:
        cmd += f"--trigger-topic={name}"
    c.run(cmd)


@task
def deploy_all_functions(c):
    for function in all_functions:
        for memory in (256, 512, 1024, 2048):
            deploy_function(c, function, memory=memory)


@task
def deploy_scheduler(
    c,
    entry_point=fintick_scheduler_gcp,
    schedule="* * * * *",  # Once a minute
    timezone="Etc/UTC",
    payload="{}",
):
    name = NAME_REGEX.match(entry_point).group(1).replace("_", "-")
    cmd = f"""
        gcloud scheduler jobs create pubsub {name} \
            --schedule='{schedule}' \
            --time-zone='{timezone}' \
            --topic={name} \
            --message-body='{payload}'
    """
    c.run(cmd)


@task
def build_container(c, hostname="asia.gcr.io", image=FINTICK):
    # Ensure requirements
    export_requirements(c)
    build_args = get_deploy_env_vars(pre="--build-arg ", sep=" ")
    name = get_container_name(hostname, image)
    cmd = f"""
        docker build \
            {build_args} \
            --file=Dockerfile \
            --tag={name} .
    """
    c.run(cmd)


@task
def push_container(c, hostname="asia.gcr.io", image=FINTICK):
    name = get_container_name(hostname, image)
    c.run(f"docker push {name}")
