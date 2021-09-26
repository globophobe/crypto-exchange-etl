import os

from pynamodb.attributes import (
    BooleanAttribute,
    JSONAttribute,
    UnicodeAttribute,
    UTCDateTimeAttribute,
)
from pynamodb.models import Model

from ..constants import AWS_REGION
from ..utils import get_trades_name


class MetatickTrade(Model):
    key = UnicodeAttribute(hash_key=True)
    timestamp = UTCDateTimeAttribute(range_key=True)
    data = JSONAttribute()
    ok = BooleanAttribute()

    class Meta:
        table_name = get_trades_name()
        region = os.environ[AWS_REGION]
        billing_mode = "PAY_PER_REQUEST"
