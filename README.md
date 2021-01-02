Download Bitmex historical data from S3, and load it into Google BigQuery:
--------------------------------------------------------------------------

This:

| symbol | date | timestamp | price	| volume | tickRule | index |
|--------|------|-----------|-------|--------|----------|-------|
| XBTUSD | 2016-05-13 | 03:23:24.383144 | 454.13 | 500 | -1 | 8 |
| XBTUSD | 2016-05-13 | 03:23:24.383144 | 454.13 | 3000 | -1 | 9 |
| XBTUSD | 2016-05-13 | 03:23:24.383144 | 454.14 | 1000 | 1 | 10 |
| XBTUSD | 2016-05-13 | 03:23:24.383144 | 454.16 | 2000 | 1 | 11 |
| XBTUSD | 2016-05-13 | 03:24:36.306484 | 454.18 | 2000 | 1 | 12 |

Becomes this:

| symbol | date | timestamp | price | avgPrice | volume | tickRule | exponent | notional |
|--------|------|-----------|-------|----------|--------|----------|----------|----------|
| XBTUSD | 2016-05-13 | 03:23:24.383144 | 454.13 | 454.13 | 3500 | -1 | 2 | 7.7... | 7 |
| XBTUSD | 2016-05-13 | 03:23:24.383144 | 454.16 | 454.15 | 3000 | 1 | 3 | 6.6... | 8 |
| XBTUSD | 2016-05-13 | 03:24:36.306484 | 454.18 | 454.18 | 2000 | 1 | 3 | 4.4... | 9 |

Deploy:
-------

Deploy as a Google Cloud Background Function, with a Pub/Sub trigger.

There is an `invoke` task:

```
invoke deploy
```

There is no further processing if data is not available, or aggregated data already exists.  The `invoke` tasks also require `gcloud`. Also, state is stored in Firestore cache.

Script:
-------

There is a script, so you can get initial historical data. The following will load Google BigQuery with data from 2016-05-13 until now. 

```
python script.py
```

Requirements:
-------------

First, init a python venv:

```
python -m venv .env
```

Next, install requirements:

```
pip install -r requirements.txt

```

Extra requirements are in `requirements_extra`.

Environment:
------------

Rename `env.yaml.sample` to `env.yaml`, and add the required settings.
