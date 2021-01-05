# Download cryptocurrency exchange historical tick data, and load into Google BigQuery

Historical tick data can be useful for analyzing financial markets, and for machine learning. As an example, refer to ["The Volume Clock: Insights into the High Frequency Paradigm"](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2034858).

When possible, data is downloaded from AWS S3.

Otherwise, it is downloaded with the cryptocurrency exchange REST API.

Supported cryptocurrency exchanges
----------------------------------
* BitMEX
* Bybit
* Coinbase Pro
* FTX

Why Google BigQuery?
--------------------
Google BigQuery is cost performant. For about $2 a month, the price of a coffee, about 100GB, which is years of historical tick data, can be stored in BigQuery. Cloud Functions can fetch new data every day. BigQuery is extremely fast, and easy to query, with standard SQL.

Historical tick data
--------------------
Example BitMEX XBTUSD data

|  date (1)  |    timestamp    | nanoseconds (2) | price  | volume | notional | tickRule (3) | index (4) |
|------------|-----------------|-----------------|--------|--------|----------|--------------|-----------|
| 2016-05-13 | 03:23:24.383144 | 0               | 454.13 | 500    |          | -1           | 8         |
| 2016-05-13 | 03:23:24.383144 | 0               | 454.13 | 3000   |          | -1           | 9         |
| 2016-05-13 | 03:23:24.383144 | 0               | 454.14 | 1000   |          | 1            | 10        |
| 2016-05-13 | 03:23:24.383144 | 0               | 454.16 | 2000   |          | 1            | 11        |
| 2016-05-13 | 03:24:36.306484 | 0               | 454.18 | 2000   |          | 1            | 12        |

Note:

1. Date
* [Partitioning data](https://cloud.google.com/bigquery/docs/partitioned-tables) is important when using BigQuery. Date is a required column, as `cryptotick` partitions data by date.

2. Nanoseconds
* BigQuery doesn't support nanoseconds. Then again, most cryptocurrency exchanges don't either. Nanoseconds will be saved to a new column by cryptotick.

3. Tick rule
* Plus ticks and zero plus ticks have tick rule 1, minus ticks and zero minus ticks have tick rule -1.

4. Index
* Trades may not be unique by timestamp and nanoseconds. Therefore, order by timestamp, nanoseconds, index is recommended.


Aggregated historical tick data
-------------------------------
Example BitMEX XBTUSD aggregated data

Trades are aggregated by equal timestamp, nanoseconds, and tick rule. Referring to the previous table, trades 8 and 9 are aggregated into a single trade. Those trades have equal timestamps and, are both market sells. Trades 10 and 11 are also aggregated. They have equal timestamps, and are both market buys.

|    date    |    timestamp    | price  | slippage (1) | volume | tickRule | exponent (2) |  notional   |
|------------|-----------------|--------|--------------|--------|----------|--------------|-------------|
| 2016-05-13 | 03:23:24.383144 | 454.13 | 0            | 3500   | -1       | 2            | 7.7...      |
| 2016-05-13 | 03:23:24.383144 | 454.16 |              | 3000   | 1        | 3            | 6.6...      |
| 2016-05-13 | 03:24:36.306484 | 454.18 | 0            | 2000   | 1        | 3            | 4.4...      |

Note:

1. Slippage
* The difference between the ideal execution at the first price and the weighted average price by notional, of the fully executed trade. For example, a trader market buys 20 notional at $1. However, only 10 notional is offered at $1, the next offer for an additional 10 notional is at $2. In this case, ideal execution is $1 * 20 = $20. Actual execution is ($1 * 10) + ($2 * 10) = $30. Slippage would be $30 - 20 = $10.

2. Exponent
* Trades by GUI traders, i.e. traders in front of a screen, often execute trades with size in multiples of 10. Bots tend to execute trades with random size. In the example data, the first trade is a multiple of 100, so has exponent 2. The other trades are multiples of 1000, so have exponent 3. _Note: Not all cryptocurrency exchanges have high frequency of trades with sizes in multiples of 10._

Limit the number of partitions scanned
--------------------------------------
As part of the [free tier](https://cloud.google.com/free), a terabyte of data can be queried for free with Google BigQuery. However, it is important to query by partition. `cryptotick` partitions data by date. 

Suppose cryptotick.bitmex_XBTUSD has 10GB of data, and `@date` has 10MB of data.

:money_with_wings: bad, scans all partitions. Query will process 10GB
```
SELECT * from cryptotick.bitmex_XBTUSD order by timestamp, nanoseconds, index;
```

:100: good, scans a single partitions. Query will process 10MB
```
SELECT * from cryptotick.bitmex_XBTUSD where date = @date order by timestamp, nanoseconds, index;
```

Installation
------------

For convenience, `cryptotick` can be installed from PyPI:

```
pip install cryptotick
```

However, for deployment to Google Cloud Platform, it is recommended to fork or download this repository.

Deploy
------

Deploy as a Google Cloud Background Function, with a Pub/Sub trigger.

There are `invoke` tasks. For example, Bitmex XBTUSD:

```
invoke deploy-function bitmex_perpetual
invoke deploy-function trade_aggregator
invoke deploy-scheduler bitmex_perpetual_trigger --payload {"symbols": "XBTUSD", "aggregate": true}
```

There is no further processing if data is not available, or aggregated data already exists.  The `invoke` tasks also require `gcloud`. Also, state is stored in Firestore cache.

Script
------

There are scripts, so you can get initial historical data. The following will load Google BigQuery with data from 2016-05-13 until now.

```
python scripts/bitmex_perpetual.py --symbols XBTUSD
python scripts/trade_aggregator.py --table_name bitmex_XBTUSD
```

Requirements
------------

First, init a python venv:

```
python -m venv .env
```

Next, install requirements:

```
pip install -r requirements.txt

```

Extra requirements are in `requirements_extra`.

Environment
-----------

Rename `env.yaml.sample` to `env.yaml`, and add the required settings.
