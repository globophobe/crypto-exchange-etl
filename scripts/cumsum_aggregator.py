#!/usr/bin/env python

# isort:skip_file
import typer
                                                                                               
import pathfix  # noqa: F401                                                                                                
from fintick.aggregators import cumsum_aggregator
from fintick.utils import set_environment

            
if __name__ == "__main__":
    set_environment()
    typer.run(cumsum_aggregator)~
