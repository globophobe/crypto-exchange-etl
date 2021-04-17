from ...utils import parse_period_from_to
from .renko import RenkoAggregatorDailyPartition, RenkoAggregatorHourlyPartition


def renko_aggregator(
    source_table: str = None,
    box_size: float = 1.0,
    top_n: int = 0,
    period_from: str = None,
    period_to: str = None,
    has_multiple_symbols: bool = False,
    verbose: bool = False,
):
    assert source_table, 'Required param "source_table" not provided'
    assert box_size, 'Required param "box_size" not provided'
    timestamp_from, timestamp_to, date_from, date_to = parse_period_from_to(
        period_from=period_from, period_to=period_to
    )
    # Daily partitions, then hourly partitions
    if date_from and date_to:
        RenkoAggregatorDailyPartition(
            source_table,
            box_size=box_size,
            top_n=top_n,
            period_from=date_from,
            period_to=date_to,
            has_multiple_symbols=has_multiple_symbols,
            verbose=verbose,
        ).main()
    if timestamp_from and timestamp_to:
        RenkoAggregatorHourlyPartition(
            f"{source_table}_hot",
            box_size=box_size,
            top_n=top_n,
            period_from=timestamp_from,
            period_to=timestamp_to,
            has_multiple_symbols=has_multiple_symbols,
            verbose=verbose,
        ).main()
