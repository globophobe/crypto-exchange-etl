import re

from ...fintick import (
    FinTickMultiSymbolDailyMixin,
    FinTickMultiSymbolHourlyMixin,
    FinTickMultiSymbolREST,
)
from .api import get_active_futures, get_expired_futures
from .base import FTXMixin
from .constants import BTCMOVE


class BaseFTXMOVE(FTXMixin, FinTickMultiSymbolREST):
    @property
    def log_prefix(self):
        symbol = BTCMOVE.replace("-", "")
        return f"{self.exchange_display} {symbol}"

    def get_symbols(self):
        active_futures = get_active_futures(self.symbol, verbose=False)
        expired_futures = get_expired_futures(self.symbol, verbose=False)
        futures = active_futures + expired_futures
        regex = re.compile(r"^BTC-MOVE-(WK)?-?(\d{4})?(\d{4})?(Q\d)?$")
        move_futures = []
        for future in futures:
            api_symbol = future["api_symbol"]
            match = regex.match(api_symbol)
            if match:
                week = match.group(1)
                period = match.group(3) or match.group(2)
                quarter = match.group(4)
                if week:
                    future["symbol"] = f"{week}{period}"
                if quarter:
                    future["symbol"] = f"{period}{quarter}"
                if not week and not quarter:
                    future["symbol"] = f"D{period}"
                move_futures.append(future)
            elif api_symbol == "BTC-MOVE-20202020Q1":
                future["symbol"] = "2020Q1"
                move_futures.append(future)
        return move_futures


class FTXMOVEHourlyPartition(FinTickMultiSymbolHourlyMixin, BaseFTXMOVE):
    def get_suffix(self, sep="-"):
        symbol = BTCMOVE.replace("-", "")
        return f"{symbol}{sep}hot"


class FTXMOVEDailyPartition(FinTickMultiSymbolDailyMixin, BaseFTXMOVE):
    def get_suffix(self, sep="-"):
        return BTCMOVE.replace("-", "")
