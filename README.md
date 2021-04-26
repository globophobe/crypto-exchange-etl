# What?

This is the basis of a pipeline for historical tick data from financial exchanges. At the moment, only cryptocurrency exchanges are supported.


# Why?

Historical tick data can be useful for analyzing financial markets, and for machine learning. As an example, refer to ["The Volume Clock: Insights into the High Frequency Paradigm"](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2034858).

It is said that 80% of the effort in ML is getting and cleaning your data. Maybe this can help.


# How?

To get started, run the scripts locally. Deploy the Google Cloud Functions to get new data every hour. When possible, data is downloaded from AWS S3 repositories. Otherwise, it is downloaded from the exchange's REST API.

Data is stored in Google BigQuery. It is cost performant. For the price of a fancy cup of coffee each month, years of historical data can be stored. As part of the [free tier](https://cloud.google.com/free), a terabyte of queries is free each month.

Some state, such as pagination keys, is stored in Firestore.


Example tick data
-----------------

| uid | timestamp (1)      | nanoseconds (2) | price (3) | volume (3) | notional (3) | tickRule (4) | index (5) |
|-----|--------------------|-----------------|-----------|------------|--------------|--------------|-----------|
| ... | ...00:27:17.367156 | 0               | 456.98    | 2000       | 4.3765591... | -1           | 9         |
| ... | ...00:44:59.302471 | 0               | 457.1     | 1000       | 2.1877050... | 1            | 10        |
| ... | ...00:44:59.302471 | 0               | 457.11    | 2000       | 4.3753144... | 1            | 11        |

Note:

1. Timestamp
* [Partitioning data](https://cloud.google.com/bigquery/docs/partitioned-tables) is important when using BigQuery. `fintick` partitions data into both daily and hourly partitions.

2. Nanoseconds
* BigQuery doesn't support nanoseconds. Then again, many exchanges don't either. Ttimestamps are parsed with `pd.to_datetime(value)`, as `datetime.datetime` doesn't support nanoseconds. If nanoseconds exist, they are saved to the nanoseconds column.

3. Price, volume, and notional
* BigQuery supports NUMERIC/DECIMAL types. `fintick` float data is stored as BIGNUMERIC. Some exchange REST APIs return floats rather than strings, which can be an issue when parsing to `Decimal`. Such exchanges include Bitfinex, BitMEX, Bybit, and FTX. For those exchanges, response is loaded with `json.loads(response.read(), parse_float=Decimal))`.

4. Tick rule
* Plus ticks and zero plus ticks have tick rule 1, minus ticks and zero minus ticks have tick rule -1.

5. Index
* Trades may not be unique by timestamp and nanoseconds. In SQL, it is recommended to order by timestamp, nanoseconds, and index.


Example aggregated data
-----------------------

Trades are aggregated in a separate table. To be aggregated, a sequence of trades must have equal symbol, timestamp, nanoseconds, and tick rule. 

Aggregating trades in this way, can increase information, and reduce number of rows by 30-50%

Referring to the previous table, trades 10 and 11 are aggregated into a single trade. Those trades have equal timestamps and are both market buys.

| uid |    timestamp       | ... | price  | vwap (1)      | volume |   notional   | ticks | tickRule |
|-----|--------------------|-----|--------|---------------|--------|--------------|-------|----------|
| ... | ...00:27:17.367156 | ... | 456.98 | 456.98        | 2000   | 4.3765591... | 1     | -1       |
| ... | ...00:44:59.302471 | ... | 457.11 | 457.106678... | 3000   | 6.5630195... | 2     | 1        |

Note:

1. VWAP
* The volume weighted average price paid for the aggregated trade. Suppose, a trader wants to buy 3 notional, and market buys. The trader buys 1 notional at $1. However, only 10 notional is offered at $1. The next offer is 1 notional at $2, and the next 1 notional at $3. In this case, vwap is the sum of volume, 1 + 2 + 3 = 6, divided by the sum of notional 1 + 1 + 1 = 3. Average price paid is 6 / 3 = $2.


Supported exchanges
-------------------

* Binance REST API
* Bitfinex REST API
* Bitflyer REST API
* BitMEX REST API, and [S3](https://public.bitmex.com/) repository
* Bybit REST API, and [S3](https://public.bybit.com/) repository
* Coinbase Pro REST API
* Deribit REST API
* FTX REST API
* Upbit REST API

Note: Exchanges without paginated REST APIs are not supported.


Installation
------------

For convenience, `fintick` can be installed from PyPI:

```
pip install fintick
```

Environment
-----------

To use the scripts or deploy to GCP, rename `env.yaml.sample` to `env.yaml`, and add the required settings.
