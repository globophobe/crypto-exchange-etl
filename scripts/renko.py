#!/usr/bin/env python

# isort:skip_file
import typer

import pathfix  # noqa: F401
from cryptotick.aggregators import Renko
from cryptotick.utils import set_environment


def renko(
    table_name: str = None,
    basename: str = None,
    box_size: float = None,
    reversal: int = 1,
    top_n: int = 10,
    date_from: str = None,
    date_to: str = None,
    has_multiple_symbols: bool = False,
    verbose: bool = False,
):
    set_environment()
    Renko(
        table_name,
        box_size,
        reversal=reversal,
        top_n=top_n,
        basename=basename,
        date_from=date_from,
        date_to=date_to,
        has_multiple_symbols=has_multiple_symbols,
        verbose=verbose,
    ).main()


if __name__ == "__main__":
    typer.run(renko)
