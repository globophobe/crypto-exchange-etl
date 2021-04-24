#!/usr/bin/env python

# isort:skip_file
import typer

import pathfix  # noqa: F401
from fintick.exchanges.bybit import bybit_perpetual
from fintick.utils import set_environment


if __name__ == "__main__":
    set_environment()
    typer.run(bybit_perpetual)
