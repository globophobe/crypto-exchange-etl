import datetime

from cryptotick.providers.bitmex import BITMEX, XBTUSD, BitmexPerpetualETL
from cryptotick.s3downloader import HistoricalDownloader

SYMBOLS = [XBTUSD]


def assert_200(exchange):
    now = datetime.datetime.utcnow()
    delta = now - datetime.timedelta(days=2)
    two_days_ago = delta.date()
    url = BitmexPerpetualETL(SYMBOLS).get_url(two_days_ago)
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
