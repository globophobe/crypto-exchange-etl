#!/usr/bin/env python

# isort:skip_file
import typer

import pathfix  # noqa: F401
from cryptotick.aggregators import ThresholdJoinAggregator
from cryptotick.utils import set_environment


def threshold_join_aggregator(
    source_table: str = None,
    destination_table: str = None,
    timestamp_cache: str = None,
    top_n: int = 10,
    symbol: str = None,
    date_from: str = None,
    date_to: str = None,
    has_multiple_symbols: bool = False,
    verbose: bool = False,
):
    set_environment()
    ThresholdJoinAggregator(
        source_table,
        destination_table,
        timestamp_cache,
        top_n=top_n,
        symbol=symbol,
        date_from=date_from,
        date_to=date_to,
        has_multiple_symbols=has_multiple_symbols,
        verbose=verbose,
    ).main()


if __name__ == "__main__":
    typer.run(threshold_join_aggregator)
