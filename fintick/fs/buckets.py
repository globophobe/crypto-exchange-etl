import os

import boto3

from ..constants import AWS_REGION

REGION = os.environ[AWS_REGION]


def create_bucket(name, region=REGION):
    location = {"LocationConstraint": region}
    s3_client = boto3.client("s3", region_name=region)
    s3_client.create_bucket(Bucket=name, CreateBucketConfiguration=location)


def put_bucket_lifecycle_configuration(
    name, storage_class="STANDARD_IA", days=1, region=REGION
):
    s3_client = boto3.client("s3", region_name=region)
    s3_client.put_bucket_lifecycle_configuration(
        Bucket=name,
        LifecycleConfiguration={
            "Rules": [
                {
                    "ID": f"{storage_class}_{days}_DAY",
                    "Status": "Enabled",
                    "Transitions": [
                        {
                            "Days": days,
                            "StorageClass": storage_class,
                        },
                    ],
                },
            ]
        },
    )


def put_public_access_block(name):
    s3_client = boto3.client("s3", region_name=REGION)
    s3_client.put_public_access_block(
        Bucket=name,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        },
    )
