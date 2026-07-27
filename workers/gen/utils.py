import logging
import os

import boto3
from botocore.client import Config

logger = logging.getLogger(__name__)


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT_URL"),
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name=os.environ.get("S3_REGION", "us-east-1"),
        config=Config(signature_version="s3v4"),
    )


def file_exists_in_s3(s3_key: str, bucket: str | None = None) -> bool:
    bucket = bucket or os.environ["S3_BUCKET"]
    client = get_s3_client()
    try:
        client.head_object(Bucket=bucket, Key=s3_key)
        return True
    except client.exceptions.ClientError:
        return False


def download_file_from_s3(s3_key: str, bucket: str | None = None) -> bytes:
    bucket = bucket or os.environ["S3_BUCKET"]
    client = get_s3_client()
    logger.info(f"Downloading s3://{bucket}/{s3_key}")
    return client.get_object(Bucket=bucket, Key=s3_key)["Body"].read()


def upload_file_to_s3(
    data: bytes,
    s3_key: str,
    content_type: str = "application/octet-stream",
    bucket: str | None = None,
) -> str:
    bucket = bucket or os.environ["S3_BUCKET"]
    client = get_s3_client()
    logger.info(f"Uploading {len(data)} bytes -> s3://{bucket}/{s3_key}")
    client.put_object(Bucket=bucket, Key=s3_key, Body=data, ContentType=content_type)
    return s3_key
