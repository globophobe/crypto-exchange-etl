from copy import deepcopy

from fintick.aggregators.cumsum.constants import NOTIONAL
from fintick.aggregators.cumsum.lib import aggregate_cumsum, get_initial_cumsum_cache

from .utils import get_data_frame


def get_samples(trades, cache=None, cumsum_attr=NOTIONAL, cumsum_value=2):
    data_frame, _ = get_data_frame(trades)
    cache = cache or get_initial_cumsum_cache(cumsum_attr)
    return aggregate_cumsum(data_frame, cache, cumsum_attr, cumsum_value)


def test_cumsum():
    trades = [{"notional": 1, "ticks": [1, 1]}]
    samples, cache = get_samples(trades)
    assert len(samples) == 1


def test_cumsum_with_cache():
    trades_1 = [{"notional": 1, "ticks": [1, 1, 1]}]
    trades_2 = [{"notional": 1, "ticks": [1]}]
    samples_1, c = get_samples(trades_1)
    cache_1 = deepcopy(c)
    assert "nextDay" in c
    samples_2, cache_2 = get_samples(trades_2, cache=c)
    assert samples_2[0]["open"] == cache_1["nextDay"]["open"]
    assert "nextDay" not in cache_2
