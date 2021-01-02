 #!/usr/bin/env python

# isort:skip_file
import typer

import pathfix  # noqa: F401
from cryptotick.providers.coinbase import BTCUSD, CoinbaseSpotETL
from cryptotick.utils import set_environment, json_str_or_list


def coinbase_spot(
    api_symbol: str = BTCUSD,
    date_from: str = None,
    date_to: str = None,
    aggregate: bool = False,
    post_aggregation: str = None,
    verbose: bool = False,
):
    set_environment()
    CoinbaseSpotETL(
        api_symbol=api_symbol,
        date_from=date_from,
        date_to=date_to,
        aggregate=False,
        post_aggregation=json_str_or_list(post_aggregation),
        verbose=verbose
    ).main()


if __name__ == "__main__":
    typer.run(coinbase_spot)
