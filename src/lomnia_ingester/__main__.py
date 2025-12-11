import json
from pathlib import Path

import boto3
import pika
import yaml
from dotenv import load_dotenv
from mypy_boto3_s3 import S3Client

from lomnia_ingester.models import FailedToRunPlugin, PluginsConfig
from lomnia_ingester.plugin_runner import run_plugin

load_dotenv()

BUCKET_NAME = "lomnia"

s3: S3Client = boto3.client(
    "s3",
    endpoint_url="http://localhost:3900",
)


def send_message(message: str, queue_name: str = "test_queue"):
    conn = pika.BlockingConnection(
        pika.ConnectionParameters(
            host="localhost",
            port=5672,
            credentials=pika.PlainCredentials("guest", "guest"),
        )
    )
    channel = conn.channel()
    channel.queue_declare(queue=queue_name, durable=False)

    channel.basic_publish(
        exchange="",
        routing_key=queue_name,
        body=message.encode(),
    )

    print(f"[sender] Sent: {message}")
    conn.close()


def upload_file_and_notify(file_path: Path):
    key = f"plugins/{file_path.name}"

    s3.upload_file(str(file_path), BUCKET_NAME, key)

    signed_url = s3.generate_presigned_url(
        ClientMethod="get_object",
        Params={
            "Bucket": BUCKET_NAME,
            "Key": key,
        },
        ExpiresIn=3600,  # seconds
    )

    message = json.dumps({
        "file": signed_url,
        "bucket": BUCKET_NAME,
        "key": key,
    })
    message = json.dumps(message)
    send_message(message)

    print(f"Uploaded {file_path} → {signed_url}")
    return signed_url


def process_plugin_outputs(canonical_dir: Path):
    if not canonical_dir.exists():
        raise FailedToRunPlugin("CANONICAL_FOLDER_NOT_FOUND")

    for file in canonical_dir.iterdir():
        if file.is_file():
            upload_file_and_notify(file)


def load_config():
    with open("plugins.yaml") as stream:
        try:
            config = yaml.safe_load(stream)
            return PluginsConfig(**config)
        except yaml.YAMLError as exc:
            print(exc)


if __name__ == "__main__":
    config = load_config()
    if config is None:
        raise FailedToRunPlugin("MISSING_PLUGINS")

    print("Running plugins")

    for plugin in config.plugins:
        with run_plugin(plugin) as plugin_output:
            print("Plugin run complete:", plugin_output)
            process_plugin_outputs(plugin_output.canonical)
