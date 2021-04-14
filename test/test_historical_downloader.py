import datetime

from fintick.providers.bitmex import BITMEX, XBTUSD, BitmexPerpetualETL
from fintick.providers.bitmex.constants import MIN_DATE
from fintick.s3downloader import HistoricalDownloader

SYMBOLS = [XBTUSD]


def assert_200(exchange):
    url = BitmexPerpetualETL(SYMBOLS).get_url(MIN_DATE)
    data_frame = HistoricalDownloader(url).main()
    assert len(data_frame) > 0


def assert_404(exchange):
    now = datetime.datetime.utcnow()
    delta = now + datetime.timedelta(days=1)
    tomorrow = delta.date()
    url = BitmexPerpetualETL(SYMBOLS).get_url(tomorrow)
    data_frame = HistoricalDownloader(url).main()
    assert data_frame is None


def test_bitmex_200():
    assert_200(BITMEX)


def test_bitmex_404():
    assert_404(BITMEX)
