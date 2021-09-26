#!/usr/bin/env python

# isort:skip_file
import typer

import pathfix  # noqa: F401
from fintick.functions import fintick_api
from fintick.aggregators import (
    renko_aggregator, thresh_aggregator, trade_aggregator, candle_aggregator
)
from fintick.utils import set_environment

set_environment()

app = typer.Typer()

api_app = typer.Typer()
aggregate_app = typer.Typer()
bars_app = typer.Typer()

app.add_typer(api_app, name="api")
app.add_typer(aggregate_app, name="aggregate")
app.add_typer(bars_app, name="bars")

@api_app.callback(invoke_without_command=True)
def api(
        provider: str = None, 
        symbol: str = None, 
        period_from: str = None, 
        period_to: str = None,
        futures: bool = False,
        verbose: bool = True
     ):
        fintick_api(
            provider, 
            symbol, 
            period_from=period_from, 
            period_to=period_to, 
            futures=futures, 
            verbose=verbose
        )

@aggregate_app.callback(invoke_without_command=True)
def aggregate(
        provider: str = None, 
        symbol: str = None, 
        period_from: str = None, 
        period_to: str = None,
        futures: bool = False,
        verbose: bool = True
     ):
        trade_aggregator(
            provider, 
            symbol, 
            period_from=period_from,
            period_to=period_to,
            futures=futures,
            verbose=verbose,
        )

@bars_app.command()
def candles(
        provider: str = None, 
        symbol: str = None, 
        period_from: str = None, 
        period_to: str = None,
        timeframe: str = None,
        top_n: int = 0,
        futures: bool = False,
        verbose: bool = True
     ):
        candle_aggregator(
            provider, 
            symbol, 
            period_from=period_from,
            period_to=period_to,
            timeframe=timeframe,
            top_n=top_n,
            futures=futures,
            verbose=verbose,
        )

@bars_app.command()
def thresh(
        provider: str = None, 
        symbol: str = None, 
        period_from: str = None, 
        period_to: str = None,
        thresh_attr: str = None,
        thresh_value: float = None,
        era_length: str = "W",
        top_n: int = 0,
        futures: bool = False,
        verbose: bool = True
     ):
        thresh_aggregator(
            provider, 
            symbol, 
            period_from=period_from,
            period_to=period_to,
            thresh_attr=thresh_attr,
            thresh_value=thresh_value,
            era_length=era_length,
            top_n=top_n,
            futures=futures,
            verbose=verbose,
        )

@bars_app.command()
def renko(
        provider: str = None, 
        symbol: str = None, 
        period_from: str = None, 
        period_to: str = None,
        box_size: str = None,
        top_n: int = 0,
        futures: bool = False,
        verbose: bool = True
     ):
        renko_aggregator(
            provider, 
            symbol, 
            period_from=period_from,
            period_to=period_to,
            box_size=box_size,
            top_n=top_n,
            futures=futures,
            verbose=verbose,
        )

if __name__ == "__main__":
    app()
