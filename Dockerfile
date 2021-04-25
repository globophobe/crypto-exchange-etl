FROM python:3.7-slim-buster

ARG PROJECT_ID
ARG BIGQUERY_LOCATION
ARG BIGQUERY_DATASET

ENV PROJECT_ID $PROJECT_ID
ENV BIGQUERY_LOCATION $BIGQUERY_LOCATION
ENV BIGQUERY_DATASET $BIGQUERY_DATASET

ADD cryptotick /cryptotick/
ADD scripts /scripts/
ADD entrypoint.sh /

ADD requirements.txt /

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && pip install --no-cache-dir -r requirements.txt \
    && apt-get purge -y --auto-remove build-essential \
    && apt-get clean  \
    && rm -rf /var/lib/apt/lists/*

ENTRYPOINT ["/entrypoint.sh"]
