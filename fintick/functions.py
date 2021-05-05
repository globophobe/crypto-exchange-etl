# from fintick.providers.upbit import UPBIT, upbit_perpetual
from fintick.providers.binance import BINANCE, binance_perpetual

# from fintick.providers.deribit import DERIBIT, deribit_perpetual
from fintick.providers.bitfinex import BITFINEX, bitfinex_perpetual
from fintick.providers.bitflyer import BITFLYER, bitflyer_perpetual
from fintick.providers.bitmex import BITMEX, bitmex_futures, bitmex_perpetual
from fintick.providers.bybit import BYBIT, bybit_perpetual
from fintick.providers.coinbase import COINBASE, coinbase_spot
from fintick.providers.ftx import BTCMOVE, FTX, ftx_move, ftx_perpetual  # ftx_futures
from fintick.providers.utils import assert_provider


def fintick_api(
    provider: str,
    symbol: str,
    period_from: str = None,
    period_to: str = None,
    futures: bool = False,
    verbose: bool = False,
):
    assert_provider(provider)
    assert symbol, 'Required param "symbol" not provided'
    kwargs = {"period_from": period_from, "period_to": period_to, "verbose": verbose}
    if provider == BINANCE:
        if futures:
            raise NotImplementedError
        else:
            binance_perpetual(symbol, **kwargs)
    elif provider == BITFINEX:
        if futures:
            raise NotImplementedError
        else:
            bitfinex_perpetual(symbol, **kwargs)
    elif provider == BITFLYER:
        bitflyer_perpetual(symbol, **kwargs)
    elif provider == BITMEX:
        if futures:
            bitmex_futures(symbol, **kwargs)
        else:
            bitmex_perpetual(symbol, **kwargs)
    elif provider == BYBIT:
        if futures:
            raise NotImplementedError
        else:
            bybit_perpetual(symbol, **kwargs)
    elif provider == COINBASE:
        if futures:
            raise NotImplementedError
        else:
            coinbase_spot(symbol, **kwargs)
    # elif provider == DERIBIT:
    #     deribit_perpetual(**kwargs)
    elif provider == FTX:
        if symbol == BTCMOVE:
            ftx_move(**kwargs)
        elif futures:
            raise NotImplementedError
        else:
            ftx_perpetual(symbol, **kwargs)
    # elif provider == UPBIT:
    #     upbit_perpetual(**kwargs)
