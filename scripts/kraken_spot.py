#!/usr/bin/env python

# isort:skip_file
import typer

import pathfix  # noqa: F401
from cryptotickdata.providers.kraken import kraken_spot
from cryptotickdata.utils import set_environment


if __name__ == "__main__":
    set_environment()
    typer.run(kraken_spot)