#!/usr/bin/env python

# isort:skip_file
import typer

import pathfix  # noqa: F401
from fintick.providers.bitfinex import bitfinex_perpetual
from fintick.utils import set_environment


if __name__ == "__main__":
    set_environment()
    typer.run(bitfinex_perpetual)
