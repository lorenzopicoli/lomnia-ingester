from pathlib import Path

import boto3
from mypy_boto3_s3 import S3Client


class S3Storage:
    def __init__(
        self,
        bucket: str,
        region_name: str,
        endpoint_url: str,
        access_key_id: str,
        secret_access_key: str,
    ):
        self.bucket = bucket
        self.client: S3Client = boto3.client(
            "s3",
            region_name=region_name,
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
        )

    def upload_file(self, file_path: Path, key: str) -> str:
        self.client.upload_file(str(file_path), self.bucket, key)
        return key
