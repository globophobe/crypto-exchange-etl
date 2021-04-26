import os
import re

from invoke import task

from fintick.constants import BIGQUERY_LOCATION
from fintick.utils import get_container_name, get_deploy_env_vars, set_environment
from main import fintick_aggregator_gcp, fintick_api_gcp

set_environment()

NAME_REGEX = re.compile(r"^(\w+)_gcp$")

ALL_HTTP_FUNCTIONS = [fintick_api_gcp, fintick_aggregator_gcp]


@task
def export_requirements(c):
    c.run("poetry export --output requirements.txt")
    c.run("poetry export --dev --output requirements-dev.txt")


@task
def deploy_http_function(c, entry_point, memory=256):
    name = NAME_REGEX.match(entry_point).group(1).replace("_", "-")
    region = os.environ[BIGQUERY_LOCATION]
    timeout = 540
    env_vars = get_deploy_env_vars()
    cmd = f"""
        gcloud functions deploy {name}-{memory} \
            --region={region} \
            --memory={memory}MB \
            --timeout={timeout}s \
            --runtime=python37 \
            --entry-point={entry_point} \
            --set-env-vars={env_vars} \
            --trigger-http
    """
    c.run(cmd)


@task
def deploy_all_http_functions(c):
    # Ensure requirements
    export_requirements(c)
    # Four versions of each function
    for function in ALL_HTTP_FUNCTIONS:
        deploy_http_function(c, function.__name__, memory=256)  # 256MB
    for function in ALL_HTTP_FUNCTIONS:
        deploy_http_function(c, function.__name__, memory=512)  # 512MB
    for function in ALL_HTTP_FUNCTIONS:
        deploy_http_function(c, function.__name__, memory=1024)  # 1GB
    for function in ALL_HTTP_FUNCTIONS:
        deploy_http_function(c, function.__name__, memory=2048)  # 2GB


@task
def build_container(c, hostname="asia.gcr.io", image="cryptotick"):
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
def push_container(c, hostname="asia.gcr.io", image="cryptotick"):
    name = get_container_name(hostname, image)
    c.run(f"docker push {name}")


# @task
# def deploy_scheduler(
#     c,
#     entry_point,
#     payload="{}",
#     schedule="1 * * * *",  # First minute, of every hour
#     timezone="Etc/UTC",
# ):
#     topic = TOPIC_REGEX.match(entry_point).group(1).replace("_", "-")
#     cmd = f"""
#         gcloud scheduler jobs create pubsub {topic} \
#             --schedule='{schedule}' \
#             --time-zone='{timezone}' \
#             --topic={topic} \
#             --message-body='{payload}'
#     """
#     c.run(cmd)


# @task
# def deploy_pubsub_function(c, entry_point, memory=256):
#     topic = TOPIC_REGEX.match(entry_point).group(1).replace("_", "-")
#     region = os.environ[BIGQUERY_LOCATION]
#     timeout = 540
#     env_vars = get_deploy_env_vars()
#     cmd = f"""
#         gcloud functions deploy {topic} \
#             --region={region} \
#             --memory={memory}MB \
#             --timeout={timeout}s \
#             --runtime=python37 \
#             --entry-point={entry_point} \
#             --set-env-vars={env_vars} \
#             --trigger-topic={topic}
#     """
#     c.run(cmd)
