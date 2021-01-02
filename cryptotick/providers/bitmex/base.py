import pandas as pd

from ...cryptotick import S3CryptoExchangeETL
from ...s3downloader import calculate_index, calculate_notional
from .constants import URL
from .lib import calc_notional


class BaseBitmexETL(S3CryptoExchangeETL):
    def get_url(self, date):
        date_string = date.strftime("%Y%m%d")
        return f"{URL}{date_string}.csv.gz"

    def parse_dataframe(self, data_frame):
        # No false positives.
        # Source: https://pandas.pydata.org/pandas-docs/stable/user_guide/
        # indexing.html#returning-a-view-versus-a-copy
        pd.options.mode.chained_assignment = None
        # Reset index.
        data_frame = calculate_index(data_frame)
        # Timestamp
        data_frame["timestamp"] = pd.to_datetime(
            data_frame["timestamp"], format="%Y-%m-%dD%H:%M:%S.%f"
        )
        data_frame = super().parse_dataframe(data_frame)
        # Notional after other transforms.
        data_frame = calculate_notional(data_frame, calc_notional)
        return data_frame
