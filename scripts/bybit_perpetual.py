#!/usr/bin/env python

# isort:skip_file
import typer

import pathfix  # noqa: F401
from cryptotick.providers.bybit import BTCUSD, BybitPerpetualETL
from cryptotick.utils import set_environment


def bybit_perpetual(
    symbol: str = BTCUSD,
    date_from: str = None,
    date_to: str = None,
    aggregate: bool = False,
    verbose: bool = False,
):
    set_environment()
    BybitPerpetualETL(
        symbol,
        date_from=date_from,
        date_to=date_to,
        aggregate=aggregate,
        verbose=verbose
    ).main()


if __name__ == "__main__":
    typer.run(bybit_perpetual)
