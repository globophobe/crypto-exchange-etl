#!/usr/bin/env python

# isort:skip_file
import typer

import pathfix  # noqa: F401
from cryptotick.providers.coinbase import BTCUSD, CoinbaseSpotETL
from cryptotick.utils import set_environment


def coinbase_spot(
    api_symbol: str = BTCUSD,
    date_from: str = None,
    date_to: str = None,
    aggregate: bool = False,
    verbose: bool = False,
):
    set_environment()
    CoinbaseSpotETL(
        api_symbol=api_symbol,
        date_from=date_from,
        date_to=date_to,
        aggregate=False,
        verbose=verbose
    ).main()


if __name__ == "__main__":
    typer.run(coinbase_spot)
