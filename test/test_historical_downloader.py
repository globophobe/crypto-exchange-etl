import datetime

from fintick.providers.bitmex.perpetual import (
    BITMEX,
    XBTUSD,
    BitmexPerpetualDailyPartition,
)
from fintick.s3downloader import HistoricalDownloader


def assert_200(exchange):
    date = datetime.date(2016, 5, 14)
    controller = BitmexPerpetualDailyPartition(XBTUSD, period_from=date, period_to=date)
    url = controller.get_url(date)
    data_frame = HistoricalDownloader(url).main()
    assert len(data_frame) > 0


def assert_404(exchange):
    now = datetime.datetime.utcnow()
    delta = now + datetime.timedelta(days=1)
    tomorrow = delta.date()
    controller = BitmexPerpetualDailyPartition(
        XBTUSD, period_from=tomorrow, period_to=tomorrow
    )
    url = controller.get_url(tomorrow)
    data_frame = HistoricalDownloader(url).main()
    assert data_frame is None


def test_bitmex_200():
    assert_200(BITMEX)


def test_bitmex_404():
    assert_404(BITMEX)
