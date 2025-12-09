import boto3
from dotenv import load_dotenv
from mypy_boto3_s3 import S3Client

load_dotenv()


def foo(bar: str) -> str:
    s3: S3Client = boto3.client(
        "s3",
        endpoint_url="http://localhost:3900",
    )
    print(s3.list_objects(Bucket="lomnia-raw"))
    return bar


if __name__ == "__main__":
    foo("Test")
    pass
