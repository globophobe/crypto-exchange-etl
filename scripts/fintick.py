#!/usr/bin/env python

# isort:skip_file
import typer

import pathfix  # noqa: F401
from fintick import fintick_api
from fintick.utils import set_environment


if __name__ == "__main__":
    set_environment()
    typer.run(fintick_api)
