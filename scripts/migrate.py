#!/usr/bin/env python

# isort:skip_file
import typer
from fintick.utils import set_environment

set_environment()

import pathfix  # noqa: F401, E402
from fintick.migrate import Migrator   # noqa: E402


def main(
    provider: str = None,
    api_symbol: str = None,
    period_from: str = None,
    period_to: str = None,
    futures: bool = False,
    is_aggregated: bool = False,
    verbose: bool = True
):
    Migrator(
        provider=provider,
        api_symbol=api_symbol,
        period_from=period_from,
        period_to=period_to,
        futures=futures,
        is_aggregated=is_aggregated,
        verbose=verbose
    ).main()


if __name__ == "__main__":
    typer.run(main)
