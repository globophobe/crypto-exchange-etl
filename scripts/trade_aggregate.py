#!/usr/bin/env python

# isort:skip_file
import typer

import pathfix  # noqa: F401
from cryptotick.aggregators import TradeAggregator
from cryptotick.utils import set_environment, json_str_or_list


def trade_aggregator(
    table_name: str = None,
    date_from: str = None,
    date_to: str = None,
    has_multiple_symbols: bool = False,
    post_aggregation: str = None,
    verbose: bool = False,
):
    set_environment()
    TradeAggregator(
        table_name,
        date_from=date_from,
        date_to=date_to,
        has_multiple_symbols=has_multiple_symbols,
        post_aggregation=json_str_or_list(post_aggregation),
        verbose=verbose,
    ).main()


if __name__ == "__main__":
    typer.run(trade_aggregator)
