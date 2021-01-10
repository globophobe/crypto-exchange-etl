#!/usr/bin/env python

# isort:skip_file
import typer

import pathfix  # noqa: F401
from cryptotick.providers.ftx import FTXMOVEETL
from cryptotick.utils import set_environment


def ftx_move(
    date_from: str = None,
    date_to: str = None,
    aggregate: bool = False,
    verbose: bool = False,
):
    set_environment()
    FTXMOVEETL(
        date_from=date_from,
        date_to=date_to,
        aggregate=aggregate,
        verbose=verbose
    ).main()


if __name__ == "__main__":
    typer.run(ftx_move)
