from ..utils import get_trades_name
from .buckets import (
    create_bucket,
    put_bucket_lifecycle_configuration,
    put_public_access_block,
)


def get_or_create_buckets():
    bucket = get_trades_name()
    create_bucket(bucket)
    put_bucket_lifecycle_configuration(bucket)
    put_public_access_block(bucket)
