#!/usr/bin/env python

# isort:skip_file
import typer

import pathfix  # noqa: F401
from fintick.exchanges.coinbase import coinbase_spot
from fintick.utils import set_environment


if __name__ == "__main__":
    set_environment()
    typer.run(coinbase_spot)
