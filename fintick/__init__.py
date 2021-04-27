from fintick.providers.binance import BINANCE, binance_perpetual
from fintick.providers.bitfinex import BITFINEX, bitfinex_perpetual
from fintick.providers.bitflyer import BITFLYER, bitflyer_perpetual
from fintick.providers.bitmex import BITMEX, bitmex_futures, bitmex_perpetual
from fintick.providers.bybit import BYBIT, bybit_perpetual
from fintick.providers.coinbase import COINBASE, coinbase_spot

# from fintick.providers.deribit import DERIBIT, deribit_perpetual
from fintick.providers.ftx import FTX, ftx_move, ftx_perpetual  # ftx_futures

# from fintick.providers.upbit import UPBIT, upbit_perpetual


def fintick_api(
    provider: str = None,
    symbol: str = None,
    period_from: str = None,
    period_to: str = None,
    futures: bool = False,
    verbose: bool = False,
):
    assert symbol, 'Required param "symbol" not provided'
    kwargs = {
        "symbol": symbol,
        "period_from": period_from,
        "period_to": period_to,
        "verbose": verbose,
    }
    if provider == BINANCE:
        if futures:
            raise NotImplementedError
        else:
            binance_perpetual(**kwargs)
    elif provider == BITFINEX:
        if futures:
            raise NotImplementedError
        else:
            bitfinex_perpetual(**kwargs)
    elif provider == BITFLYER:
        bitflyer_perpetual(**kwargs)
    elif provider == BITMEX:
        if futures:
            bitmex_futures(**kwargs)
        else:
            bitmex_perpetual(**kwargs)
    elif provider == BYBIT:
        if futures:
            raise NotImplementedError
        else:
            bybit_perpetual(**kwargs)
    elif provider == COINBASE:
        if futures:
            raise NotImplementedError
        else:
            coinbase_spot(**kwargs)
    # elif provider == DERIBIT:
    #     deribit_perpetual(**kwargs)
    elif provider == FTX:
        if symbol == "BTCMOVE":
            kwargs.pop("symbol")
            ftx_move(**kwargs)
        elif futures:
            raise NotImplementedError
        else:
            ftx_perpetual(**kwargs)
    # elif provider == UPBIT:
    #     upbit_perpetual(**kwargs)
    else:
        raise ValueError(f'Unknown provider "{provider}"')
