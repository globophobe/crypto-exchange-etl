#!/usr/bin/env python

# isort:skip_file
import typer

import pathfix  # noqa: F401
from cryptotickdata.providers.binance import binance_perpetual
from cryptotickdata.utils import set_environment


if __name__ == "__main__":
    set_environment()
    typer.run(binance_perpetual)
