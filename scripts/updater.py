#!/usr/bin/env python

# isort:skip_file
import typer

import pathfix  # noqa: F401
from cryptotickdata.aggregators.updater import updater
from cryptotickdata.utils import set_environment


if __name__ == "__main__":
    set_environment()
    typer.run(updater)
