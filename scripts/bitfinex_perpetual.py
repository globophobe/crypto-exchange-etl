#!/usr/bin/env python

# isort:skip_file
import typer

import pathfix  # noqa: F401
from cryptotickdata.providers.bitfinex import bitfinex_perpetual
from cryptotickdata.utils import set_environment


if __name__ == "__main__":
    set_environment()
    typer.run(bitfinex_perpetual)
